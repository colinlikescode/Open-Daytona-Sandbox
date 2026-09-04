"""In-memory fake runtime for tests and cloud-free development.

It simulates enough of a Linux sandbox for the control plane, scheduler, API
and SDK test suites: an in-memory filesystem per sandbox, a tiny shell that
understands a handful of commands (``echo``, ``cat``, ``sleep``, ``exit``, ...),
capacity exhaustion, startup failures, crashes and disappearance.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import posixpath
import shlex
import tarfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field

from sandboxpilot.errors import (
    CommandError,
    FileTransferError,
    NetworkProxyError,
    RuntimeUnavailableError,
    SandboxNotFoundError,
    SandboxRuntimeError,
)
from sandboxpilot.schemas.sandbox import (
    RuntimeSandbox,
    RuntimeSandboxSpec,
    RuntimeSandboxState,
    SandboxResources,
)
from sandboxpilot.utils.clock import Clock, SystemClock
from sandboxpilot.utils.ids import new_id
from sandboxpilot.utils.sizes import cpus_to_millis
from sandboxpilot.version import __version__
from sandboxpilot.worker.runtime.base import (
    LABEL_CPU_MILLIS,
    LABEL_EXPIRES_AT,
    LABEL_MEMORY_BYTES,
    LABEL_PIDS_LIMIT,
    LABEL_POOL_ID,
    LABEL_SANDBOX_ID,
    LABEL_VERSION,
    LABEL_WORKER_ID,
    MANAGED_LABEL,
    ExecHandle,
    HostResources,
    ImageInfo,
    RuntimeDoctorResult,
    SandboxRuntime,
    StreamName,
)

CommandHandler = Callable[["FakeExec", list[str]], Awaitable[int]]


@dataclass
class FakeFS:
    """Minimal in-memory filesystem: files are bytes, directories are a set."""

    files: dict[str, bytes] = field(default_factory=dict)
    modes: dict[str, int] = field(default_factory=dict)
    dirs: set[str] = field(default_factory=lambda: {"/", "/tmp", "/workspace"})

    def norm(self, path: str, cwd: str = "/") -> str:
        if not path.startswith("/"):
            path = posixpath.join(cwd, path)
        return posixpath.normpath(path)

    def mkdirs(self, path: str) -> None:
        path = self.norm(path)
        while path not in self.dirs:
            self.dirs.add(path)
            if path == "/":
                break
            path = posixpath.dirname(path)

    def write(self, path: str, data: bytes, mode: int = 0o644) -> None:
        path = self.norm(path)
        self.mkdirs(posixpath.dirname(path))
        self.files[path] = data
        self.modes[path] = mode

    def read(self, path: str) -> bytes:
        path = self.norm(path)
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def exists(self, path: str) -> bool:
        path = self.norm(path)
        return path in self.files or path in self.dirs

    def is_dir(self, path: str) -> bool:
        return self.norm(path) in self.dirs

    def listdir(self, path: str) -> list[str]:
        path = self.norm(path)
        out: set[str] = set()
        prefix = path.rstrip("/") + "/"
        for p in list(self.files) + list(self.dirs):
            if p.startswith(prefix) and p != path:
                out.add(p[len(prefix) :].split("/", 1)[0])
        return sorted(out)

    def to_tar(self, path: str) -> bytes:
        path = self.norm(path)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            if path in self.files:
                self._add_file(tar, path, posixpath.basename(path))
            elif path in self.dirs:
                base = posixpath.basename(path.rstrip("/")) or "root"
                info = tarfile.TarInfo(base)
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                tar.addfile(info)
                prefix = path.rstrip("/") + "/"
                for d in sorted(self.dirs):
                    if d.startswith(prefix):
                        dinfo = tarfile.TarInfo(posixpath.join(base, d[len(prefix) :]))
                        dinfo.type = tarfile.DIRTYPE
                        dinfo.mode = 0o755
                        tar.addfile(dinfo)
                for f in sorted(self.files):
                    if f.startswith(prefix):
                        self._add_file(tar, f, posixpath.join(base, f[len(prefix) :]))
            else:
                raise FileNotFoundError(path)
        return buf.getvalue()

    def _add_file(self, tar: tarfile.TarFile, path: str, arcname: str) -> None:
        data = self.files[path]
        info = tarfile.TarInfo(arcname)
        info.size = len(data)
        info.mode = self.modes.get(path, 0o644)
        info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(data))

    def extract_tar(self, path: str, data: bytes) -> None:
        path = self.norm(path)
        self.mkdirs(path)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tar:
            for member in tar.getmembers():
                name = member.name.lstrip("./")
                if not name or ".." in name.split("/"):
                    continue
                target = posixpath.join(path, name)
                if member.isdir():
                    self.mkdirs(target)
                elif member.isfile():
                    fh = tar.extractfile(member)
                    self.write(target, fh.read() if fh else b"", member.mode)


@dataclass
class FakeSandbox:
    spec: RuntimeSandboxSpec
    runtime_id: str
    created: bool = True
    running: bool = False
    exit_code: int | None = None
    oom_killed: bool = False
    fs: FakeFS = field(default_factory=FakeFS)
    labels: dict[str, str] = field(default_factory=dict)
    procs: set[FakeExec] = field(default_factory=set)
    image_digest: str | None = None


class FakeExec(ExecHandle):
    """A simulated process."""

    def __init__(
        self,
        runtime: FakeSandboxRuntime,
        sandbox: FakeSandbox,
        argv: list[str],
        env: dict[str, str],
        cwd: str,
    ) -> None:
        self.exec_id = new_id("exec")
        self.runtime = runtime
        self.sandbox = sandbox
        self.argv = argv
        self.env = env
        self.cwd = cwd
        self._queue: asyncio.Queue[tuple[StreamName, bytes] | None] = asyncio.Queue()
        self._exit: asyncio.Future[int] = asyncio.get_event_loop().create_future()
        self._task: asyncio.Task[None] | None = None
        self._killed = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())
        self.sandbox.procs.add(self)

    async def _run(self) -> None:
        code = 0
        try:
            code = await self.runtime.shell.execute(self, self.argv)
        except asyncio.CancelledError:
            code = 137
        except Exception as exc:  # pragma: no cover - defensive
            self.emit("stderr", f"{exc}\n".encode())
            code = 1
        finally:
            self.sandbox.procs.discard(self)
            await self._queue.put(None)
            if not self._exit.done():
                self._exit.set_result(137 if self._killed else code)

    def emit(self, stream: StreamName, data: bytes) -> None:
        self._queue.put_nowait((stream, data))

    async def stream(self) -> AsyncIterator[tuple[StreamName, bytes]]:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def wait(self) -> int:
        return await self._exit

    async def kill(self) -> None:
        self._killed = True
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task


class FakeShell:
    """A tiny POSIX-shell lookalike: ``;``/``&&`` sequencing, ``>`` and ``>>`` redirects."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.handlers: dict[str, CommandHandler] = {}
        self.sleep_scale = 1.0

    async def execute(self, proc: FakeExec, argv: list[str]) -> int:
        if (
            len(argv) >= 3
            and argv[0] in {"/bin/sh", "sh", "/bin/bash", "bash"}
            and argv[1] in {"-c", "-lc", "-ec"}
        ):
            return await self._run_script(proc, argv[2])
        return await self._run_simple(proc, argv)

    async def _run_script(self, proc: FakeExec, script: str) -> int:
        code = 0
        for statement in _split_statements(script):
            parts = statement.strip()
            if not parts:
                continue
            if parts.startswith("&&"):
                if code != 0:
                    continue
                parts = parts[2:].strip()
            elif parts.startswith("||"):
                if code == 0:
                    continue
                parts = parts[2:].strip()
            try:
                tokens = shlex.split(parts)
            except ValueError as exc:
                proc.emit("stderr", f"sh: syntax error: {exc}\n".encode())
                return 2
            if not tokens:
                continue
            code = await self._run_tokens(proc, tokens)
            if tokens[0] == "exit":
                return code
        return code

    async def _run_tokens(self, proc: FakeExec, tokens: list[str]) -> int:
        redirect: tuple[str, bool] | None = None
        if ">>" in tokens:
            i = tokens.index(">>")
            redirect = (tokens[i + 1], True)
            tokens = tokens[:i]
        elif ">" in tokens:
            i = tokens.index(">")
            redirect = (tokens[i + 1], False)
            tokens = tokens[:i]
        if redirect:
            captured: list[bytes] = []
            original_emit = proc.emit

            def capture(stream: StreamName, data: bytes) -> None:
                if stream == "stdout":
                    captured.append(data)
                else:
                    original_emit(stream, data)

            proc.emit = capture  # type: ignore[method-assign]
            try:
                code = await self._run_simple(proc, tokens)
            finally:
                proc.emit = original_emit  # type: ignore[method-assign]
            target, append = redirect
            data = b"".join(captured)
            fs = proc.sandbox.fs
            path = fs.norm(target, proc.cwd)
            if append and path in fs.files:
                data = fs.files[path] + data
            fs.write(path, data)
            return code
        return await self._run_simple(proc, tokens)

    async def _run_simple(self, proc: FakeExec, argv: list[str]) -> int:
        name = argv[0]
        args = argv[1:]
        if name in self.handlers:
            return await self.handlers[name](proc, args)
        fs = proc.sandbox.fs
        if name in {"true", ":"}:
            return 0
        if name == "false":
            return 1
        if name == "exit":
            return int(args[0]) if args else 0
        if name == "echo":
            text = " ".join(_expand(a, proc.env) for a in args)
            proc.emit("stdout", (text + "\n").encode())
            return 0
        if name == "printf":
            proc.emit("stdout", (" ".join(args)).encode().decode("unicode_escape").encode())
            return 0
        if name == "sleep":
            await self.clock.sleep(float(args[0]) * self.sleep_scale if args else 0)
            return 0
        if name == "pwd":
            proc.emit("stdout", f"{proc.cwd}\n".encode())
            return 0
        if name == "env" or name == "printenv":
            if args:
                val = proc.env.get(args[0])
                if val is None:
                    return 1
                proc.emit("stdout", f"{val}\n".encode())
                return 0
            for k, v in sorted(proc.env.items()):
                proc.emit("stdout", f"{k}={v}\n".encode())
            return 0
        if name == "cat":
            code = 0
            for a in args:
                try:
                    proc.emit("stdout", fs.read(fs.norm(a, proc.cwd)))
                except FileNotFoundError:
                    proc.emit("stderr", f"cat: {a}: No such file or directory\n".encode())
                    code = 1
            return code
        if name == "ls":
            target = args[-1] if args and not args[-1].startswith("-") else proc.cwd
            path = fs.norm(target, proc.cwd)
            if not fs.exists(path):
                proc.emit("stderr", f"ls: {target}: No such file or directory\n".encode())
                return 2
            entries = fs.listdir(path) if fs.is_dir(path) else [posixpath.basename(path)]
            proc.emit("stdout", ("\n".join(entries) + "\n").encode() if entries else b"")
            return 0
        if name == "mkdir":
            for a in args:
                if not a.startswith("-"):
                    fs.mkdirs(fs.norm(a, proc.cwd))
            return 0
        if name == "touch":
            for a in args:
                path = fs.norm(a, proc.cwd)
                if path not in fs.files:
                    fs.write(path, b"")
            return 0
        if name == "rm":
            for a in args:
                if a.startswith("-"):
                    continue
                path = fs.norm(a, proc.cwd)
                fs.files.pop(path, None)
                fs.dirs.discard(path)
            return 0
        if name == "cp" and len(args) == 2:
            try:
                fs.write(fs.norm(args[1], proc.cwd), fs.read(fs.norm(args[0], proc.cwd)))
                return 0
            except FileNotFoundError:
                proc.emit("stderr", f"cp: {args[0]}: No such file or directory\n".encode())
                return 1
        if name == "test" or name == "[":
            flag, target = [*args, "", ""][:2]
            path = fs.norm(target, proc.cwd)
            if flag == "-f":
                return 0 if path in fs.files else 1
            if flag == "-d":
                return 0 if fs.is_dir(path) else 1
            if flag == "-e":
                return 0 if fs.exists(path) else 1
            return 1
        if name == "wc" and args and args[0] == "-c":
            try:
                proc.emit("stdout", f"{len(fs.read(fs.norm(args[1], proc.cwd)))}\n".encode())
                return 0
            except FileNotFoundError:
                return 1
        if name in {"python", "python3"} and len(args) >= 2 and args[0] == "-c":
            return self._fake_python(proc, args[1])
        if name == "id":
            proc.emit("stdout", b"uid=0(root) gid=0(root) groups=0(root)\n")
            return 0
        if name == "hostname":
            proc.emit("stdout", f"{proc.sandbox.spec.sandbox_id[:12]}\n".encode())
            return 0
        if name in {"curl", "wget"}:
            # Networking is simulated: only the policy matters for tests.
            url = next((a for a in args if a.startswith("http")), "")
            policy = proc.sandbox.spec.spec.network.value
            blocked = any(h in url for h in ("169.254.", "10.", "172.16.", "192.168."))
            if policy == "none" or blocked:
                proc.emit(
                    "stderr",
                    f"{name}: (7) Failed to connect to host: Network unreachable\n".encode(),
                )
                return 7
            proc.emit("stdout", b"OK\n")
            return 0
        proc.emit("stderr", f"sh: {name}: not found\n".encode())
        return 127

    def _fake_python(self, proc: FakeExec, code: str) -> int:
        """Support ``print(<expr>)`` for simple arithmetic and string literals only."""
        stripped = code.strip()
        if stripped.startswith("print(") and stripped.endswith(")"):
            expr = stripped[6:-1]
            try:
                value = eval(expr, {"__builtins__": {}}, {})  # noqa: S307 - test-only fake, no builtins
            except Exception:
                proc.emit("stderr", b"Traceback (most recent call last):\nNameError\n")
                return 1
            proc.emit("stdout", f"{value}\n".encode())
            return 0
        if stripped.startswith("import sys; sys.exit(") or stripped.startswith("raise SystemExit("):
            digits = "".join(ch for ch in stripped if ch.isdigit())
            return int(digits or "1")
        proc.emit("stderr", b"fake python: unsupported program\n")
        return 1


def _split_statements(script: str) -> list[str]:
    """Split on ``;`` and newlines while keeping ``&&``/``||`` markers attached."""
    out: list[str] = []
    buf = ""
    quote: str | None = None
    i = 0
    while i < len(script):
        ch = script[i]
        if quote:
            buf += ch
            if ch == quote:
                quote = None
        elif ch in {"'", '"'}:
            quote = ch
            buf += ch
        elif ch in {";", "\n"}:
            out.append(buf)
            buf = ""
        elif script.startswith("&&", i) or script.startswith("||", i):
            out.append(buf)
            buf = script[i : i + 2]
            i += 1
        else:
            buf += ch
        i += 1
    out.append(buf)
    return out


def _expand(token: str, env: dict[str, str]) -> str:
    if token.startswith("$") and len(token) > 1:
        name = token[1:].strip("{}")
        return env.get(name, "")
    return token


class FakeSandboxRuntime(SandboxRuntime):
    name = "fake"

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        cpu_millis: int = 16000,
        memory_bytes: int = 64 * 1024**3,
        disk_total_bytes: int = 200 * 1024**3,
        disk_free_bytes: int = 150 * 1024**3,
        preloaded_images: set[str] | None = None,
        image_pull_seconds: float = 0.0,
    ) -> None:
        self.clock = clock or SystemClock()
        self.shell = FakeShell(self.clock)
        self.sandboxes: dict[str, FakeSandbox] = {}
        self.images: set[str] = set(preloaded_images or {"python:3.12-slim", "node:22-slim"})
        self.image_pull_seconds = image_pull_seconds
        self.host = HostResources(
            cpu_millis=cpu_millis,
            memory_bytes=memory_bytes,
            disk_total_bytes=disk_total_bytes,
            disk_free_bytes=disk_free_bytes,
        )
        self.port_targets: dict[tuple[str, int], tuple[str, int]] = {}
        # failure injection
        self.fail_create: Exception | None = None
        self.fail_start: Exception | None = None
        self.fail_images: set[str] = set()
        self.unavailable = False
        self.create_delay = 0.0
        self.create_count = 0
        self.adopt_count = 0
        self.removed: list[str] = []

    # -- doctor / host -----------------------------------------------------

    async def doctor(self) -> RuntimeDoctorResult:
        return RuntimeDoctorResult(
            ok=not self.unavailable,
            runtime=self.name,
            docker_version="fake",
            runsc_version="fake",
            checks={"runtime": not self.unavailable},
            errors=[] if not self.unavailable else ["fake runtime marked unavailable"],
        )

    async def host_resources(self) -> HostResources:
        return self.host

    # -- images ------------------------------------------------------------

    async def ensure_image(self, reference: str, policy: str) -> ImageInfo:
        if reference in self.fail_images:
            raise SandboxRuntimeError(
                f"Image pull failed for {reference}: manifest unknown",
                hint="Check the image reference and registry credentials on the worker.",
            )
        present = reference in self.images
        if present and policy != "always":
            return ImageInfo(
                reference=reference, digest=f"sha256:{abs(hash(reference)):x}", pulled=False
            )
        if policy == "never":
            raise SandboxRuntimeError(
                f"Image {reference} is not present and pull policy is 'never'"
            )
        start = self.clock.monotonic()
        if self.image_pull_seconds:
            await self.clock.sleep(self.image_pull_seconds)
        self.images.add(reference)
        return ImageInfo(
            reference=reference,
            digest=f"sha256:{abs(hash(reference)):x}",
            pulled=True,
            pull_seconds=self.clock.monotonic() - start,
        )

    async def list_images(self) -> list[ImageInfo]:
        return [
            ImageInfo(reference=r, digest=f"sha256:{abs(hash(r)):x}") for r in sorted(self.images)
        ]

    # -- lifecycle ---------------------------------------------------------

    def _get(self, sandbox_id: str) -> FakeSandbox:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            raise SandboxNotFoundError(f"Sandbox {sandbox_id} not found in runtime")
        return sb

    async def create(self, spec: RuntimeSandboxSpec) -> RuntimeSandbox:
        if self.unavailable:
            raise RuntimeUnavailableError("fake runtime unavailable")
        if self.fail_create:
            raise self.fail_create
        if spec.spec.image not in self.images:
            raise SandboxRuntimeError(
                f"image {spec.spec.image} not present; ensure_image() must run first"
            )
        self.create_count += 1
        if self.create_delay:
            await self.clock.sleep(self.create_delay)
        runtime_id = "fake-" + new_id().replace("-", "")[:24]
        res = spec.spec.resources
        labels = {
            MANAGED_LABEL: "true",
            LABEL_SANDBOX_ID: spec.sandbox_id,
            LABEL_POOL_ID: spec.pool_id,
            LABEL_WORKER_ID: spec.worker_id,
            LABEL_EXPIRES_AT: spec.expires_at.isoformat(),
            LABEL_VERSION: __version__,
            LABEL_CPU_MILLIS: str(res.cpu_millis),
            LABEL_MEMORY_BYTES: str(res.memory_bytes),
            LABEL_PIDS_LIMIT: str(res.pids_limit),
            **spec.spec.labels,
        }
        sb = FakeSandbox(spec=spec, runtime_id=runtime_id, labels=labels)
        sb.fs.mkdirs(spec.spec.workdir)
        sb.image_digest = f"sha256:{abs(hash(spec.spec.image)):x}"
        self.sandboxes[spec.sandbox_id] = sb
        return RuntimeSandbox(
            sandbox_id=spec.sandbox_id, runtime_id=runtime_id, image_digest=sb.image_digest
        )

    def remember(self, sandbox_id: str, runtime_id: str) -> None:
        for sid, sb in list(self.sandboxes.items()):
            if sb.runtime_id == runtime_id and sid != sandbox_id:
                self.sandboxes[sandbox_id] = self.sandboxes.pop(sid)

    async def adopt(self, old_id: str, spec: RuntimeSandboxSpec) -> RuntimeSandbox | None:
        sb = self.sandboxes.pop(old_id, None)
        if sb is None:
            raise SandboxNotFoundError(f"Sandbox {old_id} not found in runtime")
        self.adopt_count += 1
        sb.spec = spec
        sb.labels[LABEL_CPU_MILLIS] = str(spec.spec.resources.cpu_millis)
        sb.labels[LABEL_MEMORY_BYTES] = str(spec.spec.resources.memory_bytes)
        sb.labels[LABEL_PIDS_LIMIT] = str(spec.spec.resources.pids_limit)
        self.sandboxes[spec.sandbox_id] = sb
        return RuntimeSandbox(
            sandbox_id=spec.sandbox_id, runtime_id=sb.runtime_id, image_digest=sb.image_digest
        )

    async def start(self, sandbox_id: str) -> None:
        sb = self._get(sandbox_id)
        if self.fail_start:
            raise self.fail_start
        sb.running = True

    async def stop(self, sandbox_id: str, timeout: float = 5.0) -> None:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            return
        for proc in list(sb.procs):
            await proc.kill()
        sb.running = False
        if sb.exit_code is None:
            sb.exit_code = 137

    async def remove(self, sandbox_id: str) -> None:
        sb = self.sandboxes.pop(sandbox_id, None)
        if sb:
            for proc in list(sb.procs):
                await proc.kill()
            self.removed.append(sandbox_id)

    async def inspect(self, sandbox_id: str) -> RuntimeSandboxState:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            return RuntimeSandboxState(
                sandbox_id=sandbox_id, runtime_id=None, exists=False, running=False
            )
        return self._state(sb)

    def _state(self, sb: FakeSandbox) -> RuntimeSandboxState:
        return RuntimeSandboxState(
            sandbox_id=sb.spec.sandbox_id,
            runtime_id=sb.runtime_id,
            exists=True,
            running=sb.running,
            exit_code=sb.exit_code,
            oom_killed=sb.oom_killed,
            expires_at=sb.spec.expires_at,
            resources=SandboxResources(
                cpu_millis=cpus_to_millis(sb.spec.spec.cpus),
                memory_bytes=sb.spec.spec.memory_bytes,
                pids_limit=sb.spec.spec.pids_limit,
            ),
            runtime_name=self.name,
            labels=dict(sb.labels),
        )

    async def list_managed(self) -> list[RuntimeSandboxState]:
        return [self._state(sb) for sb in self.sandboxes.values()]

    # -- exec / files / network ---------------------------------------------

    async def exec(
        self,
        sandbox_id: str,
        argv: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        user: str | None = None,
        stdin: bytes | None = None,
    ) -> ExecHandle:
        sb = self._get(sandbox_id)
        if not sb.running:
            raise CommandError(f"Sandbox {sandbox_id} is not running")
        full_env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": "/root",
            **sb.spec.spec.env,
            **(env or {}),
        }
        proc = FakeExec(self, sb, argv, full_env, cwd or sb.spec.spec.workdir)
        proc.start()
        return proc

    async def upload(self, sandbox_id: str, path: str, archive: AsyncIterator[bytes]) -> None:
        sb = self._get(sandbox_id)
        chunks = [chunk async for chunk in archive]
        try:
            sb.fs.extract_tar(path, b"".join(chunks))
        except tarfile.TarError as exc:
            raise FileTransferError(f"Invalid archive: {exc}") from exc

    async def download(self, sandbox_id: str, path: str) -> AsyncIterator[bytes]:
        sb = self._get(sandbox_id)
        try:
            data = sb.fs.to_tar(path)
        except FileNotFoundError:
            raise FileTransferError(
                f"{path}: No such file or directory", details={"path": path}
            ) from None
        view = memoryview(data)
        for i in range(0, len(view), 64 * 1024):
            yield bytes(view[i : i + 64 * 1024])

    async def endpoint(self, sandbox_id: str, port: int) -> tuple[str, int]:
        self._get(sandbox_id)
        target = self.port_targets.get((sandbox_id, port))
        if target is None:
            raise NetworkProxyError(
                f"Nothing is listening on port {port} in sandbox {sandbox_id}",
                details={"port": port},
            )
        return target

    # -- failure injection helpers ---------------------------------------------

    def crash(self, sandbox_id: str, *, oom: bool = False) -> None:
        sb = self._get(sandbox_id)
        sb.running = False
        sb.exit_code = 137
        sb.oom_killed = oom

    def disappear(self, sandbox_id: str) -> None:
        self.sandboxes.pop(sandbox_id, None)
