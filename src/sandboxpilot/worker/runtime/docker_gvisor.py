"""Production runtime: Docker Engine API with the gVisor ``runsc`` runtime.

All Docker calls go through the Python SDK over the Unix socket (never ``docker``
subprocesses for request handling). Each blocking SDK call runs in a worker
thread so the asyncio event loop stays responsive.

Security defaults (see docs/security.md):

* ``Runtime=runsc`` requested explicitly and verified after creation; no fallback.
* ``privileged=False``, no host network/PID/IPC namespaces, no host mounts,
  no device passthrough, no Docker socket.
* Capabilities reduced to a small subset needed by package managers;
  ``no-new-privileges`` enabled.
* CPU quota, memory limit (swap disabled), PID limit enforced via cgroups.
"""

from __future__ import annotations

import asyncio
import contextlib
import posixpath
import shutil
import socket
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import psutil

from sandboxpilot.errors import (
    FileTransferError,
    NetworkProxyError,
    RuntimeUnavailableError,
    SandboxNotFoundError,
    SandboxRuntimeError,
)
from sandboxpilot.schemas.common import NetworkPolicy
from sandboxpilot.schemas.sandbox import (
    RuntimeSandbox,
    RuntimeSandboxSpec,
    RuntimeSandboxState,
    SandboxResources,
)
from sandboxpilot.utils.ids import new_id, short_id
from sandboxpilot.utils.logging import get_logger
from sandboxpilot.version import __version__
from sandboxpilot.worker.runtime.base import (
    LABEL_CPU_MILLIS,
    LABEL_CREATED_AT,
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

log = get_logger("worker.runtime.docker")

T = TypeVar("T")

RUNSC = "runsc"
CPU_PERIOD = 100_000
_UPLOAD_BATCH_BYTES = 4 * 1024 * 1024

# Docker's embedded DNS (127.0.0.11 on user-defined networks) lives on the host
# kernel's loopback inside the container netns; gVisor's netstack owns the sandbox's
# loopback, so it never reaches it. Sandboxes therefore get their own resolv.conf
# pointing at public resolvers (reachable through the egress firewall).
DEFAULT_SANDBOX_DNS: tuple[str, ...] = ("8.8.8.8", "1.1.1.1")


def container_name(sandbox_id: str) -> str:
    """Docker container name for a sandbox: the full UUID body, so ids minted in the
    same millisecond (which differ only in their last bits) never collide."""
    return f"sp-sbx-{sandbox_id.split('_', 1)[-1].replace('-', '')}"


def render_resolv_conf(servers: Sequence[str]) -> str:
    lines = [f"nameserver {s}" for s in servers]
    lines.append("options timeout:2 attempts:2")
    return "\n".join(lines) + "\n"


# Docker's default set minus AUDIT_WRITE, MKNOD, NET_RAW, SETFCAP, SETPCAP, SYS_CHROOT.
# These are what package managers and build tools need as root inside the sandbox;
# gVisor remains the primary isolation boundary.
ALLOWED_CAPABILITIES = [
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "SETGID",
    "SETUID",
    "KILL",
    "NET_BIND_SERVICE",
]

PID_DIR = "/tmp/.sandboxpilot"

# Wrapper that records the command's PID so it (and its descendants) can be killed
# later. ``exec "$@"`` keeps the PID stable.
_EXEC_WRAPPER = 'mkdir -p "$(dirname "$1")" 2>/dev/null; echo $$ > "$1"; shift; exec "$@"'

# Recursively kill a process tree rooted at the PID in the given pid file.
_KILL_SCRIPT = r"""
pidfile="$1"
[ -r "$pidfile" ] || exit 0
root=$(cat "$pidfile")
kill_tree() {
  for d in /proc/[0-9]*; do
    c=${d#/proc/}
    ppid=""
    while read -r k v; do
      if [ "$k" = "PPid:" ]; then ppid=$v; break; fi
    done < "$d/status" 2>/dev/null
    [ "$ppid" = "$1" ] && kill_tree "$c"
  done
  kill -9 "$1" 2>/dev/null
}
kill_tree "$root"
rm -f "$pidfile"
"""


class DockerExecHandle(ExecHandle):
    def __init__(
        self, runtime: GVisorDockerRuntime, container_id: str, exec_id: str, pidfile: str
    ) -> None:
        self.runtime = runtime
        self.container_id = container_id
        self.exec_id = exec_id
        self.pidfile = pidfile
        self._queue: asyncio.Queue[tuple[StreamName, bytes] | BaseException | None] = (
            asyncio.Queue()
        )
        self._exit_code: int | None = None
        self._started = False

    def _start(self) -> None:
        if self._started:
            return
        self._started = True
        loop = asyncio.get_running_loop()
        api = self.runtime.api

        def pump() -> None:
            try:
                stream = api.exec_start(self.exec_id, stream=True, demux=True)
                for out, err in stream:
                    if out:
                        loop.call_soon_threadsafe(self._queue.put_nowait, ("stdout", bytes(out)))
                    if err:
                        loop.call_soon_threadsafe(self._queue.put_nowait, ("stderr", bytes(err)))
                loop.call_soon_threadsafe(self._queue.put_nowait, None)
            except BaseException as exc:
                loop.call_soon_threadsafe(self._queue.put_nowait, exc)

        self.runtime._spawn_thread(pump)

    async def stream(self) -> AsyncIterator[tuple[StreamName, bytes]]:
        self._start()
        while True:
            item = await self._queue.get()
            if item is None:
                return
            if isinstance(item, BaseException):
                raise SandboxRuntimeError(f"exec stream failed: {item}") from item
            yield item

    async def wait(self) -> int:
        self._start()
        if self._exit_code is not None:
            return self._exit_code
        delay = 0.01
        while True:
            info = await self.runtime._call(self.runtime.api.exec_inspect, self.exec_id)
            if not info.get("Running", False):
                code = info.get("ExitCode")
                self._exit_code = int(code) if code is not None else -1
                return self._exit_code
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 0.5)

    async def kill(self) -> None:
        with contextlib.suppress(Exception):
            info = await self.runtime._call(self.runtime.api.exec_inspect, self.exec_id)
            if not info.get("Running", False):
                return
        killer = await self.runtime._call(
            self.runtime.api.exec_create,
            self.container_id,
            ["/bin/sh", "-c", _KILL_SCRIPT, "sp-kill", self.pidfile],
            stdout=True,
            stderr=True,
        )
        await self.runtime._call(self.runtime.api.exec_start, killer["Id"])


class GVisorDockerRuntime(SandboxRuntime):
    def __init__(
        self,
        *,
        network_name: str = "sandboxpilot",
        network_subnet: str | None = "10.211.0.0/16",
        docker_host: str | None = None,
        unsafe_runc: bool = False,
        sandbox_dns: Sequence[str] = DEFAULT_SANDBOX_DNS,
        state_dir: Path | str = "/var/lib/sandboxpilot",
    ) -> None:
        self.network_name = network_name
        self.network_subnet = network_subnet
        self.docker_host = docker_host
        self.unsafe_runc = unsafe_runc
        self.sandbox_dns = [s for s in sandbox_dns if s]
        self.state_dir = Path(state_dir)
        self.name = "runc" if unsafe_runc else RUNSC
        self.runtime_name = None if unsafe_runc else RUNSC
        self._client: Any = None
        self._containers: dict[str, str] = {}
        self._threads: list[Any] = []
        self._resolv_conf: Path | None = None
        self._has_tar: dict[str, bool] = {}  # container id -> image ships `tar`

    # -- plumbing ----------------------------------------------------------------------------

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                import docker
            except ImportError as exc:  # pragma: no cover
                raise RuntimeUnavailableError(
                    "The docker Python package is required on workers (pip install 'sandboxpilot[worker]')"
                ) from exc
            try:
                self._client = (
                    docker.DockerClient(base_url=self.docker_host)
                    if self.docker_host
                    else docker.from_env()
                )
            except Exception as exc:
                raise RuntimeUnavailableError(f"Cannot connect to Docker: {exc}") from exc
        return self._client

    @property
    def api(self) -> Any:
        return self.client.api

    async def _call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _spawn_thread(self, target: Callable[[], None]) -> None:
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        self._threads.append(thread)
        self._threads = [t for t in self._threads if t.is_alive()]

    async def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._call(self._client.close)
            self._client = None

    # -- doctor / host -----------------------------------------------------------------------

    async def doctor(self) -> RuntimeDoctorResult:
        checks: dict[str, bool] = {}
        errors: list[str] = []
        warnings: list[str] = []
        docker_version: str | None = None
        runsc_version: str | None = None
        try:
            version = await self._call(self.client.version)
            docker_version = version.get("Version")
            checks["docker"] = True
        except Exception as exc:
            checks["docker"] = False
            errors.append(f"Docker unavailable: {exc}")
            return RuntimeDoctorResult(ok=False, runtime=self.name, checks=checks, errors=errors)
        info = await self._call(self.client.info)
        runtimes = info.get("Runtimes", {}) or {}
        if self.unsafe_runc:
            warnings.append(
                "UNSAFE: running with the default Docker runtime (runc); no gVisor isolation."
            )
            checks["runsc"] = True
        else:
            checks["runsc_configured"] = RUNSC in runtimes
            if RUNSC not in runtimes:
                errors.append(
                    "Docker has no 'runsc' runtime configured. Install gVisor and add it to /etc/docker/daemon.json."
                )
            runsc_bin = await asyncio.to_thread(shutil.which, RUNSC) or (
                runtimes.get(RUNSC, {}) or {}
            ).get("path")
            if runsc_bin:
                try:
                    proc = await asyncio.create_subprocess_exec(
                        runsc_bin,
                        "--version",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                    )
                    out, _ = await asyncio.wait_for(proc.communicate(), 10)
                    runsc_version = (
                        out.decode(errors="replace").strip().splitlines()[0] if out else None
                    )
                    checks["runsc_binary"] = proc.returncode == 0
                except Exception as exc:
                    checks["runsc_binary"] = False
                    errors.append(f"runsc --version failed: {exc}")
            else:
                checks["runsc_binary"] = False
                errors.append("runsc binary not found on PATH")
        try:
            await self._ensure_network()
            checks["network"] = True
        except Exception as exc:
            checks["network"] = False
            errors.append(f"sandbox network unavailable: {exc}")
        external = await self._count_external_containers()
        if external:
            warnings.append(f"{external} non-SandboxPilot container(s) running on this host")
        if not errors and not self.unsafe_runc:
            try:
                await self._smoke_test()
                checks["runsc_smoke_test"] = True
            except Exception as exc:
                checks["runsc_smoke_test"] = False
                errors.append(f"gVisor smoke test failed: {exc}")
        return RuntimeDoctorResult(
            ok=not errors,
            runtime=self.name,
            docker_version=docker_version,
            runsc_version=runsc_version,
            checks=checks,
            errors=errors,
            warnings=warnings,
        )

    async def _smoke_test(self) -> None:
        """Run ``true`` in a tiny gVisor container to prove the runtime works end to end."""
        image = "busybox:latest"
        await self.ensure_image(image, "if-not-present")
        container = await self._call(
            self.api.create_container,
            image,
            command=["/bin/sh", "-c", "true"],
            name=f"sp-selftest-{new_id().replace('-', '')[:12]}",
            labels={MANAGED_LABEL: "selftest"},
            host_config=self.api.create_host_config(
                runtime=RUNSC, network_mode="none", auto_remove=False
            ),
        )
        cid = container["Id"]
        try:
            await self._call(self.api.start, cid)
            result = await self._call(self.api.wait, cid, timeout=60)
            code = result.get("StatusCode", 1)
            if code != 0:
                raise SandboxRuntimeError(f"self-test container exited with {code}")
            inspect = await self._call(self.api.inspect_container, cid)
            if inspect.get("HostConfig", {}).get("Runtime") != RUNSC:
                raise SandboxRuntimeError("self-test container did not run under runsc")
        finally:
            with contextlib.suppress(Exception):
                await self._call(self.api.remove_container, cid, force=True)

    async def _count_external_containers(self) -> int:
        try:
            containers = await self._call(self.api.containers, quiet=False, all=False)
        except Exception:
            return 0
        return sum(1 for c in containers if (c.get("Labels") or {}).get(MANAGED_LABEL) != "true")

    async def host_resources(self) -> HostResources:
        try:
            info = await self._call(self.client.info)
            root = info.get("DockerRootDir") or "/var/lib/docker"
        except Exception:
            root = "/var/lib/docker"

        def probe() -> tuple[int, int, int | None, int | None]:
            cpu = psutil.cpu_count(logical=True) or 1
            mem = psutil.virtual_memory().total
            for candidate in (root, "/"):
                try:
                    usage = shutil.disk_usage(candidate)
                    return cpu, mem, usage.total, usage.free
                except OSError:
                    continue
            return cpu, mem, None, None

        cpu, mem, disk_total, disk_free = await self._call(probe)
        return HostResources(
            cpu_millis=cpu * 1000,
            memory_bytes=mem,
            disk_total_bytes=disk_total,
            disk_free_bytes=disk_free,
            external_containers=await self._count_external_containers(),
        )

    # -- network ----------------------------------------------------------------------------------

    async def _ensure_network(self) -> dict[str, Any]:
        networks = await self._call(self.api.networks, names=[self.network_name])
        for net in networks:
            if net.get("Name") == self.network_name:
                return net
        return await self._call(self._create_network)

    def _create_network(self) -> dict[str, Any]:
        import docker.types

        ipam = None
        if self.network_subnet:
            ipam = docker.types.IPAMConfig(
                pool_configs=[docker.types.IPAMPool(subnet=self.network_subnet)]
            )
        created = self.api.create_network(
            self.network_name,
            driver="bridge",
            ipam=ipam,
            # IPv4 only: the egress firewall is iptables (v4) and metadata endpoints are v4.
            enable_ipv6=False,
            labels={MANAGED_LABEL: "true"},
            options={"com.docker.network.bridge.enable_icc": "false"},
        )
        return dict(self.api.inspect_network(created["Id"]))

    async def network_id(self) -> str:
        net = await self._ensure_network()
        return str(net["Id"])

    def _ensure_resolv_conf(self) -> Path:
        """Write (once) the resolv.conf that is bind-mounted read-only into sandboxes."""
        if self._resolv_conf is None:
            path = self.state_dir / "resolv.conf"
            content = render_resolv_conf(self.sandbox_dns)
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists() or path.read_text() != content:
                path.write_text(content)
                path.chmod(0o644)  # must be world-readable inside the sandbox
            self._resolv_conf = path
        return self._resolv_conf

    # -- images --------------------------------------------------------------------------------------

    async def ensure_image(self, reference: str, policy: str) -> ImageInfo:
        present = None
        with contextlib.suppress(Exception):
            present = await self._call(self.api.inspect_image, reference)
        if present is not None and policy != "always":
            return ImageInfo(reference=reference, digest=_digest(present), pulled=False)
        if policy == "never":
            raise SandboxRuntimeError(
                f"Image {reference} is not present on the worker and pull policy is 'never'",
                hint=f"Preload it with: sandboxpilot image preload {reference}",
            )
        repo, tag = _split_reference(reference)
        start = time.monotonic()
        try:
            await self._call(self.client.images.pull, repo, tag=tag)
        except Exception as exc:
            message = str(exc)
            hint = "Check the image reference. For private registries, configure Docker credentials on the worker."
            if "unauthorized" in message.lower() or "denied" in message.lower():
                hint = "Registry authentication failed. Configure Docker registry credentials on the worker."
            raise SandboxRuntimeError(
                f"Image pull failed for {reference}: {message}", hint=hint
            ) from exc
        pulled = await self._call(self.api.inspect_image, reference)
        return ImageInfo(
            reference=reference,
            digest=_digest(pulled),
            pulled=True,
            pull_seconds=time.monotonic() - start,
        )

    async def list_images(self) -> list[ImageInfo]:
        images = await self._call(self.api.images)
        out: list[ImageInfo] = []
        for img in images:
            for tag in img.get("RepoTags") or []:
                if tag != "<none>:<none>":
                    out.append(ImageInfo(reference=tag, digest=_digest(img)))
        return sorted(out, key=lambda i: i.reference)

    # -- containers ----------------------------------------------------------------------------------

    async def _container_id(self, sandbox_id: str) -> str:
        cid = self._containers.get(sandbox_id)
        if cid:
            return cid
        found = await self._call(
            self.api.containers,
            all=True,
            filters={"label": [f"{MANAGED_LABEL}=true", f"{LABEL_SANDBOX_ID}={sandbox_id}"]},
        )
        if not found:
            raise SandboxNotFoundError(f"Sandbox {sandbox_id} has no container on this worker")
        self._containers[sandbox_id] = found[0]["Id"]
        return str(found[0]["Id"])

    async def create(self, spec: RuntimeSandboxSpec) -> RuntimeSandbox:
        s = spec.spec
        res = s.resources
        labels = {
            MANAGED_LABEL: "true",
            LABEL_SANDBOX_ID: spec.sandbox_id,
            LABEL_POOL_ID: spec.pool_id,
            LABEL_WORKER_ID: spec.worker_id,
            LABEL_CREATED_AT: spec.created_at.isoformat(),
            LABEL_EXPIRES_AT: spec.expires_at.isoformat(),
            LABEL_VERSION: __version__,
            LABEL_CPU_MILLIS: str(res.cpu_millis),
            LABEL_MEMORY_BYTES: str(res.memory_bytes),
            LABEL_PIDS_LIMIT: str(res.pids_limit),
        }
        for key, value in s.labels.items():
            if not key.startswith("sandboxpilot."):
                labels[f"user.{key}"] = value
        tmpfs: dict[str, str] = {}
        if s.tmpfs_size_bytes:
            tmpfs["/tmp"] = f"size={s.tmpfs_size_bytes},mode=1777"
        if s.read_only_root:
            tmpfs.setdefault("/tmp", "size=268435456,mode=1777")
            tmpfs[s.workdir] = "size=1073741824,mode=0777"
        network_mode = "none" if s.network == NetworkPolicy.NONE else self.network_name
        binds: list[str] = []
        if s.network != NetworkPolicy.NONE:
            await self._ensure_network()
            if self.sandbox_dns:
                resolv = await self._call(self._ensure_resolv_conf)
                binds.append(f"{resolv}:/etc/resolv.conf:ro")
        host_config = self.api.create_host_config(
            runtime=self.runtime_name,
            privileged=False,
            cpu_period=CPU_PERIOD,
            cpu_quota=int(res.cpu_millis * CPU_PERIOD / 1000),
            mem_limit=res.memory_bytes,
            memswap_limit=res.memory_bytes,
            pids_limit=res.pids_limit,
            cap_drop=["ALL"],
            cap_add=ALLOWED_CAPABILITIES,
            security_opt=["no-new-privileges"],
            network_mode=network_mode,
            read_only=s.read_only_root,
            tmpfs=tmpfs or None,
            binds=binds or None,
            ipc_mode="private",
            init=False,
        )
        name = container_name(spec.sandbox_id)
        try:
            created = await self._call(
                self.api.create_container,
                s.image,
                command=s.keepalive_command,
                name=name,
                labels=labels,
                environment=s.env or None,
                working_dir=s.workdir,
                user=s.user,
                host_config=host_config,
                hostname=f"sandbox-{short_id(spec.sandbox_id, 12)}",
                stdin_open=False,
                tty=False,
                detach=True,
            )
        except Exception as exc:
            raise SandboxRuntimeError(f"Docker container creation failed: {exc}") from exc
        cid = str(created["Id"])
        self._containers[spec.sandbox_id] = cid
        inspect = await self._call(self.api.inspect_container, cid)
        actual_runtime = inspect.get("HostConfig", {}).get("Runtime")
        if not self.unsafe_runc and actual_runtime != RUNSC:
            with contextlib.suppress(Exception):
                await self._call(self.api.remove_container, cid, force=True)
            self._containers.pop(spec.sandbox_id, None)
            raise SandboxRuntimeError(
                f"Docker did not honour runtime=runsc (got {actual_runtime!r}); refusing to run the sandbox",
                hint="Verify gVisor is configured in /etc/docker/daemon.json and Docker was restarted.",
            )
        hc = inspect.get("HostConfig", {})
        if hc.get("Privileged") or hc.get("NetworkMode") == "host" or hc.get("PidMode") == "host":
            with contextlib.suppress(Exception):
                await self._call(self.api.remove_container, cid, force=True)
            raise SandboxRuntimeError("container security configuration was not applied")
        digest = None
        with contextlib.suppress(Exception):
            digest = _digest(await self._call(self.api.inspect_image, s.image))
        return RuntimeSandbox(sandbox_id=spec.sandbox_id, runtime_id=cid, image_digest=digest)

    async def start(self, sandbox_id: str) -> None:
        cid = await self._container_id(sandbox_id)
        try:
            await self._call(self.api.start, cid)
        except Exception as exc:
            message = str(exc)
            if "runsc" in message or "OCI runtime" in message:
                raise SandboxRuntimeError(
                    f"gVisor failed to start the sandbox: {message}",
                    hint="Run `runsc --version` and check `journalctl -u docker` on the worker.",
                ) from exc
            raise SandboxRuntimeError(f"Container start failed: {message}") from exc

    async def stop(self, sandbox_id: str, timeout: float = 5.0) -> None:
        try:
            cid = await self._container_id(sandbox_id)
        except SandboxNotFoundError:
            return
        with contextlib.suppress(Exception):
            await self._call(self.api.stop, cid, timeout=int(timeout))

    def remember(self, sandbox_id: str, runtime_id: str) -> None:
        self._containers[sandbox_id] = runtime_id

    async def adopt(self, old_id: str, spec: RuntimeSandboxSpec) -> RuntimeSandbox | None:
        cid = await self._container_id(old_id)
        res = spec.spec.resources
        try:
            await self._call(self.api.rename, cid, container_name(spec.sandbox_id))
            # Live cgroup update: same call `docker update` makes. Works under runsc
            # because limits live in the host cgroup that wraps the sandbox.
            await self._call(
                self.api._post_json,
                self.api._url("/containers/{0}/update", cid),
                data={
                    "CpuPeriod": CPU_PERIOD,
                    "CpuQuota": int(res.cpu_millis * CPU_PERIOD / 1000),
                    "Memory": res.memory_bytes,
                    "MemorySwap": res.memory_bytes,
                    "PidsLimit": res.pids_limit,
                },
            )
        except Exception as exc:
            raise SandboxRuntimeError(f"could not adopt warm sandbox: {exc}") from exc
        self._containers.pop(old_id, None)
        self._containers[spec.sandbox_id] = cid
        info = await self._call(self.api.inspect_container, cid)
        digest = None
        with contextlib.suppress(Exception):
            image = await self._call(self.api.inspect_image, info["Image"])
            digests = image.get("RepoDigests") or []
            digest = digests[0].split("@", 1)[-1] if digests else image.get("Id")
        return RuntimeSandbox(sandbox_id=spec.sandbox_id, runtime_id=cid, image_digest=digest)

    async def remove(self, sandbox_id: str) -> None:
        try:
            cid = await self._container_id(sandbox_id)
        except SandboxNotFoundError:
            return
        try:
            await self._call(self.api.remove_container, cid, force=True, v=True)
        except Exception as exc:
            if "No such container" not in str(exc):
                raise SandboxRuntimeError(f"Container removal failed: {exc}") from exc
        finally:
            self._containers.pop(sandbox_id, None)

    async def inspect(self, sandbox_id: str) -> RuntimeSandboxState:
        try:
            cid = await self._container_id(sandbox_id)
            info = await self._call(self.api.inspect_container, cid)
        except Exception:
            self._containers.pop(sandbox_id, None)
            return RuntimeSandboxState(
                sandbox_id=sandbox_id, runtime_id=None, exists=False, running=False
            )
        return self._state_from_inspect(info)

    def _state_from_inspect(self, info: dict[str, Any]) -> RuntimeSandboxState:
        state = info.get("State", {})
        labels = info.get("Config", {}).get("Labels", {}) or {}
        hc = info.get("HostConfig", {})
        resources = None
        if hc.get("CpuQuota") and hc.get("Memory"):
            resources = SandboxResources(
                cpu_millis=int(hc["CpuQuota"] * 1000 / (hc.get("CpuPeriod") or CPU_PERIOD)),
                memory_bytes=int(hc["Memory"]),
                pids_limit=int(hc.get("PidsLimit") or labels.get(LABEL_PIDS_LIMIT, 1024)),
            )
        expires = None
        if labels.get(LABEL_EXPIRES_AT):
            with contextlib.suppress(ValueError):
                expires = datetime.fromisoformat(labels[LABEL_EXPIRES_AT])
        cid = str(info.get("Id") or "")
        # The label is immutable, so an adopted warm slot still carries its old id
        # there. Our own mapping (seeded from persisted worker state) wins.
        known = next((sid for sid, c in self._containers.items() if c == cid), None)
        return RuntimeSandboxState(
            sandbox_id=known or labels.get(LABEL_SANDBOX_ID, ""),
            runtime_id=info.get("Id"),
            exists=True,
            running=bool(state.get("Running")),
            exit_code=state.get("ExitCode"),
            oom_killed=bool(state.get("OOMKilled")),
            expires_at=expires,
            resources=resources,
            runtime_name=hc.get("Runtime"),
            labels=labels,
        )

    async def list_managed(self) -> list[RuntimeSandboxState]:
        containers = await self._call(
            self.api.containers, all=True, filters={"label": [f"{MANAGED_LABEL}=true"]}
        )
        out: list[RuntimeSandboxState] = []
        for c in containers:
            with contextlib.suppress(Exception):
                info = await self._call(self.api.inspect_container, c["Id"])
                state = self._state_from_inspect(info)
                if state.sandbox_id:
                    self._containers[state.sandbox_id] = str(c["Id"])
                    out.append(state)
        return out

    # -- exec ----------------------------------------------------------------------------------------

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
        cid = await self._container_id(sandbox_id)
        pidfile = f"{PID_DIR}/{new_id().replace('-', '')}.pid"
        wrapped = ["/bin/sh", "-c", _EXEC_WRAPPER, "sp-exec", pidfile, *argv]
        try:
            created = await self._call(
                self.api.exec_create,
                cid,
                wrapped,
                stdout=True,
                stderr=True,
                stdin=False,
                tty=False,
                environment=[f"{k}={v}" for k, v in (env or {}).items()] or None,
                workdir=cwd,
                user=user or "",
            )
        except Exception as exc:
            raise SandboxRuntimeError(f"exec_create failed: {exc}") from exc
        return DockerExecHandle(self, cid, created["Id"], pidfile)

    # -- files -----------------------------------------------------------------------------------------

    # File transfer goes *through the sandbox* (``tar`` run via exec) rather than
    # ``docker cp``. gVisor caches the rootfs and keeps the sandbox's writes in its
    # own overlay, so host-side ``docker cp`` neither shows new files to a running
    # sandbox nor sees files the sandbox created. Images without ``tar`` fall back
    # to ``docker cp`` (correct only for sandboxes that never touched the path).

    async def _tar_available(self, cid: str) -> bool:
        cached = self._has_tar.get(cid)
        if cached is not None:
            return cached

        def probe() -> bool:
            exec_id = self.api.exec_create(cid, ["/bin/sh", "-c", "command -v tar >/dev/null"])[
                "Id"
            ]
            self.api.exec_start(exec_id)  # blocks until the probe exits
            return self._wait_exec_sync(exec_id, timeout=10) == 0

        try:
            has_tar = await self._call(probe)
        except Exception:
            has_tar = False
        self._has_tar[cid] = has_tar
        if not has_tar:
            log.warning("image has no `tar`; falling back to docker cp for file transfer")
        return has_tar

    async def upload(self, sandbox_id: str, path: str, archive: AsyncIterator[bytes]) -> None:
        cid = await self._container_id(sandbox_id)
        with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024) as spool:
            # Batch chunks so disk writes (once the spool rolls over) happen in a
            # thread a few times per upload rather than once per 64 KiB.
            batch: list[bytes] = []
            size = 0
            async for chunk in archive:
                batch.append(chunk)
                size += len(chunk)
                if size >= _UPLOAD_BATCH_BYTES:
                    await self._call(spool.write, b"".join(batch))
                    batch, size = [], 0
            if batch:
                await self._call(spool.write, b"".join(batch))
            spool.seek(0)
            if await self._tar_available(cid):
                code, output = await self._call(
                    self._exec_with_stdin, cid, ["tar", "-xf", "-", "-C", path], spool
                )
                if code != 0:
                    raise SandboxRuntimeError(
                        f"upload to {path} failed inside the sandbox: {output.strip() or f'tar exit {code}'}"
                    )
                return
            try:
                ok = await self._call(self.api.put_archive, cid, path, spool)
            except Exception as exc:
                raise SandboxRuntimeError(f"upload to {path} failed: {exc}") from exc
        if not ok:
            raise SandboxRuntimeError(f"upload to {path} was rejected by Docker")

    def _exec_with_stdin(self, cid: str, argv: list[str], data: Any) -> tuple[int, str]:
        """Run ``argv`` in the container feeding ``data`` (file-like) on stdin (blocking)."""
        from docker.utils.socket import frames_iter

        exec_id = self.api.exec_create(cid, argv, stdin=True, stdout=True, stderr=True)["Id"]
        sock = self.api.exec_start(exec_id, socket=True)
        raw = getattr(sock, "_sock", sock)
        output: list[bytes] = []

        def drain() -> None:  # read stdout/stderr while we write, so neither side blocks
            with contextlib.suppress(Exception):
                for _stream, chunk in frames_iter(sock, False):
                    output.append(bytes(chunk))

        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        try:
            while True:
                chunk = data.read(_UPLOAD_BATCH_BYTES)
                if not chunk:
                    break
                raw.sendall(chunk)
            with contextlib.suppress(OSError):
                raw.shutdown(socket.SHUT_WR)  # EOF for tar
            reader.join(timeout=600)
        finally:
            with contextlib.suppress(Exception):
                sock.close()
        code = self._wait_exec_sync(exec_id)
        return code, b"".join(output).decode(errors="replace")

    def _wait_exec_sync(self, exec_id: str, timeout: float = 30.0) -> int:
        deadline = time.monotonic() + timeout
        delay = 0.01
        while True:
            info = self.api.exec_inspect(exec_id)
            if not info.get("Running", False):
                code = info.get("ExitCode")
                return int(code) if code is not None else -1
            if time.monotonic() > deadline:
                return -1
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)

    async def download(self, sandbox_id: str, path: str) -> AsyncIterator[bytes]:
        cid = await self._container_id(sandbox_id)
        if await self._tar_available(cid):
            async for chunk in self._download_via_exec(sandbox_id, path):
                yield chunk
            return
        async for chunk in self._download_via_docker_cp(cid, path):
            yield chunk

    async def _download_via_exec(self, sandbox_id: str, path: str) -> AsyncIterator[bytes]:
        norm = posixpath.normpath(path)
        parent, name = posixpath.dirname(norm) or "/", posixpath.basename(norm)
        if not name:  # "/" itself
            parent, name = "/", "."
        handle = await self.exec(sandbox_id, ["tar", "-cf", "-", "-C", parent, name], cwd="/")
        errors: list[bytes] = []
        produced = False
        async for stream, chunk in handle.stream():
            if stream == "stdout":
                produced = True
                yield chunk
            else:
                errors.append(chunk)
        code = await handle.wait()
        if code != 0 and not produced:
            message = b"".join(errors).decode(errors="replace").strip()
            if "Cannot stat" in message or "No such file" in message or "not found" in message:
                raise FileTransferError(
                    f"{path}: No such file or directory", details={"path": path}
                )
            raise FileTransferError(
                f"{path}: {message or f'tar exit {code}'}", details={"path": path}
            )

    async def _download_via_docker_cp(self, cid: str, path: str) -> AsyncIterator[bytes]:
        try:
            stream, _stat = await self._call(self.api.get_archive, cid, path, chunk_size=64 * 1024)
        except Exception as exc:
            raise FileTransferError(f"{path}: {exc}", details={"path": path}) from exc
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[bytes | BaseException | None] = asyncio.Queue(maxsize=32)
        abandoned = threading.Event()  # set when the consumer stops iterating early

        def offer(item: bytes | BaseException | None) -> bool:
            """Hand ``item`` to the loop; give up if the consumer has gone away."""
            while not abandoned.is_set():
                fut = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
                try:
                    fut.result(timeout=0.5)
                    return True
                except TimeoutError:
                    fut.cancel()
                except Exception:
                    return False
            return False

        def pump() -> None:
            try:
                for chunk in stream:
                    if not offer(chunk):
                        return
                offer(None)
            except BaseException as exc:
                offer(exc)

        self._spawn_thread(pump)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                if isinstance(item, BaseException):
                    raise SandboxRuntimeError(f"download failed: {item}") from item
                yield item
        finally:
            abandoned.set()

    # -- network -------------------------------------------------------------------------------------

    async def endpoint(self, sandbox_id: str, port: int) -> tuple[str, int]:
        cid = await self._container_id(sandbox_id)
        info = await self._call(self.api.inspect_container, cid)
        networks = info.get("NetworkSettings", {}).get("Networks", {}) or {}
        net = networks.get(self.network_name)
        if not net or not net.get("IPAddress"):
            raise NetworkProxyError(
                "Sandbox has no network address (network policy may be 'none')",
                details={"sandbox_id": sandbox_id, "port": port},
            )
        return str(net["IPAddress"]), port


def _digest(image_info: dict[str, Any]) -> str | None:
    digests = image_info.get("RepoDigests") or []
    if digests:
        return str(digests[0]).split("@", 1)[-1]
    image_id = image_info.get("Id")
    return str(image_id) if image_id else None


def _split_reference(reference: str) -> tuple[str, str | None]:
    if "@" in reference:
        return reference, None
    last_slash = reference.rfind("/")
    colon = reference.rfind(":")
    if colon > last_slash:
        return reference[:colon], reference[colon + 1 :]
    return reference, "latest"
