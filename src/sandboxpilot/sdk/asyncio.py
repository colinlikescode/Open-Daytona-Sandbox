"""Async SDK: :class:`AsyncSandboxPilot` (client) and :class:`AsyncSandbox`.

    async with AsyncSandbox.create(image="python:3.12-slim") as sb:
        result = await sb.run("python3 -c 'print(1)'")

Every method maps to one control plane API call. Nothing here talks to
workers directly.
"""

from __future__ import annotations

import asyncio
import io
import os
import tarfile
from collections.abc import AsyncIterator
from pathlib import Path
from types import TracebackType
from typing import Any

import httpx
from pydantic import ValidationError as PydanticValidationError

from sandboxpilot.errors import (
    CommandError,
    CommandTimeoutError,
    FileTransferError,
    SandboxPilotError,
    ValidationError,
)
from sandboxpilot.schemas.commands import CommandEvent, CommandInfo, CommandLogs, CommandResult
from sandboxpilot.schemas.common import NetworkPolicy
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.schemas.pool import PoolCreateRequest, PoolUpdateRequest, WorkerPool
from sandboxpilot.schemas.sandbox import SandboxCreateRequest, SandboxInfo
from sandboxpilot.schemas.template import Template
from sandboxpilot.schemas.worker import WorkerView
from sandboxpilot.sdk._http import (
    DEFAULT_TIMEOUT,
    aparse_sse_lines,
    auth_headers,
    raise_for_status,
    resolve_connection,
    resolve_connection_async,
    wrap_transport_error,
)


class AsyncSandboxPilot:
    """Async client for the control plane API.

    Constructing the client does no I/O. The control plane is located (and, on
    loopback, started) on the first request, or explicitly via :meth:`connect`.
    """

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        *,
        autostart: bool = True,
        timeout: httpx.Timeout | float | None = None,
    ) -> None:
        self._url_arg = url
        self._token_arg = token
        self._autostart = autostart
        self._timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
        self._resolved: tuple[str, str | None] | None = None
        self._http: httpx.AsyncClient | None = None
        self._connect_lock = asyncio.Lock()

    @property
    def url(self) -> str:
        """Control plane base URL (resolved from config if it has not been yet)."""
        if self._resolved is None:
            self._resolved = resolve_connection(self._url_arg, self._token_arg, autostart=False)
        return self._resolved[0]

    @property
    def token(self) -> str | None:
        if self._resolved is None:
            self._resolved = resolve_connection(self._url_arg, self._token_arg, autostart=False)
        return self._resolved[1]

    async def connect(self) -> httpx.AsyncClient:
        """Locate (and if allowed, start) the control plane; idempotent and non-blocking."""
        if self._http is not None:
            return self._http
        async with self._connect_lock:
            if self._http is None:
                url, token = await resolve_connection_async(
                    self._url_arg, self._token_arg, autostart=self._autostart
                )
                self._resolved = (url, token)
                self._http = httpx.AsyncClient(
                    base_url=f"{url}/v1", headers=auth_headers(token), timeout=self._timeout
                )
        return self._http

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> AsyncSandboxPilot:
        await self.connect()
        return self

    async def __aexit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.aclose()

    # -- raw ------------------------------------------------------------------------------

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        http = await self.connect()
        try:
            resp = await http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, self.url) from exc
        raise_for_status(resp)
        return resp

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self.request(method, path, **kwargs)
        return resp.json() if resp.content else None

    # -- system ---------------------------------------------------------------------------

    async def health(self) -> dict[str, Any]:
        return dict(await self._json("GET", "/health"))

    async def status(self) -> dict[str, Any]:
        return dict(await self._json("GET", "/status"))

    async def doctor(self) -> dict[str, Any]:
        return dict(await self._json("GET", "/doctor"))

    async def cleanup(self, *, terminate_workers: bool = False) -> dict[str, Any]:
        return dict(
            await self._json(
                "POST", "/cleanup", params={"terminate_workers": str(terminate_workers).lower()}
            )
        )

    async def metrics(self) -> str:
        return (await self.request("GET", "/metrics")).text

    # -- pools ----------------------------------------------------------------------------

    async def list_pools(self) -> list[WorkerPool]:
        return [WorkerPool.model_validate(p) for p in await self._json("GET", "/pools")]

    async def get_pool(self, ref: str) -> WorkerPool:
        return WorkerPool.model_validate(await self._json("GET", f"/pools/{ref}"))

    async def create_pool(self, request: PoolCreateRequest) -> WorkerPool:
        return WorkerPool.model_validate(
            await self._json(
                "POST", "/pools", json=request.model_dump(mode="json", exclude_none=True)
            )
        )

    async def update_pool(self, ref: str, request: PoolUpdateRequest) -> WorkerPool:
        return WorkerPool.model_validate(
            await self._json(
                "PATCH", f"/pools/{ref}", json=request.model_dump(mode="json", exclude_none=True)
            )
        )

    async def delete_pool(self, ref: str, *, force: bool = False) -> None:
        await self.request("DELETE", f"/pools/{ref}", params={"force": str(force).lower()})

    async def pool_up(self, ref: str, *, workers: int | None = None) -> list[Operation]:
        body = {"workers": workers} if workers is not None else {}
        return [
            Operation.model_validate(o)
            for o in await self._json("POST", f"/pools/{ref}/up", json=body)
        ]

    async def pool_down(self, ref: str, *, force: bool = False) -> dict[str, Any]:
        return dict(
            await self._json("POST", f"/pools/{ref}/down", params={"force": str(force).lower()})
        )

    # -- workers --------------------------------------------------------------------------

    async def list_workers(self, pool: str | None = None) -> list[WorkerView]:
        params = {"pool": pool} if pool else {}
        return [
            WorkerView.model_validate(w) for w in await self._json("GET", "/workers", params=params)
        ]

    async def get_worker(self, ref: str) -> WorkerView:
        return WorkerView.model_validate(await self._json("GET", f"/workers/{ref}"))

    async def drain_worker(self, ref: str, *, terminate_when_empty: bool = True) -> WorkerView:
        return WorkerView.model_validate(
            await self._json(
                "POST",
                f"/workers/{ref}/drain",
                params={"terminate_when_empty": str(terminate_when_empty).lower()},
            )
        )

    async def remove_worker(self, ref: str, *, force: bool = False) -> WorkerView:
        return WorkerView.model_validate(
            await self._json("DELETE", f"/workers/{ref}", params={"force": str(force).lower()})
        )

    # -- operations / images / templates -----------------------------------------------

    async def get_operation(self, op_id: str, *, wait: float = 0) -> Operation:
        return Operation.model_validate(
            await self._json("GET", f"/operations/{op_id}", params={"wait": wait}, timeout=None)
        )

    async def wait_operation(self, op_id: str, *, timeout: float = 900) -> Operation:
        op = await self.get_operation(op_id, wait=timeout)
        if not op.status.is_terminal:
            raise SandboxPilotError(f"operation {op_id} did not finish within {timeout:.0f}s")
        if op.error:
            raise SandboxPilotError(op.error)
        return op

    async def preload_image(self, reference: str, *, pool: str | None = None) -> Operation:
        return Operation.model_validate(
            await self._json(
                "POST", "/images/preload", json={"reference": reference, "pool": pool}, timeout=None
            )
        )

    async def list_images(self, *, worker: str | None = None) -> list[dict[str, Any]]:
        params = {"worker": worker} if worker else {}
        return list(await self._json("GET", "/images", params=params))

    async def list_templates(self) -> list[Template]:
        return [Template.model_validate(t) for t in await self._json("GET", "/templates")]

    async def add_template(self, template: Template) -> Template:
        return Template.model_validate(
            await self._json(
                "POST", "/templates", json=template.model_dump(mode="json", exclude_none=True)
            )
        )

    async def remove_template(self, name: str) -> None:
        await self.request("DELETE", f"/templates/{name}")

    # -- sandboxes ------------------------------------------------------------------------

    async def list_sandboxes(
        self, *, pool: str | None = None, all: bool = False
    ) -> list[SandboxInfo]:
        params: dict[str, Any] = {"all": str(all).lower()}
        if pool:
            params["pool"] = pool
        return [
            SandboxInfo.model_validate(s)
            for s in await self._json("GET", "/sandboxes", params=params)
        ]

    async def get_sandbox_info(self, ref: str) -> SandboxInfo:
        return SandboxInfo.model_validate(await self._json("GET", f"/sandboxes/{ref}"))

    async def create_sandbox(
        self, request: SandboxCreateRequest, *, idempotency_key: str | None = None
    ) -> SandboxInfo:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
        return SandboxInfo.model_validate(
            await self._json(
                "POST",
                "/sandboxes",
                json=request.model_dump(mode="json", exclude_none=True),
                headers=headers,
                timeout=None,
            )
        )

    async def kill_sandbox(self, ref: str) -> SandboxInfo:
        return SandboxInfo.model_validate(await self._json("DELETE", f"/sandboxes/{ref}"))


class AsyncSandbox:
    """A handle to one sandbox. Create with :meth:`create` or :meth:`connect`."""

    def __init__(
        self, client: AsyncSandboxPilot, info: SandboxInfo, *, owns_client: bool = False
    ) -> None:
        self._client = client
        self.info = info
        self._owns_client = owns_client

    # -- construction -----------------------------------------------------------------

    @classmethod
    async def create(
        cls,
        image: str | None = None,
        *,
        pool: str | None = None,
        template: str | None = None,
        cpus: float | None = None,
        memory: str | int | None = None,
        timeout: str | int | None = None,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        network: NetworkPolicy | str | None = None,
        labels: dict[str, str] | None = None,
        metadata: dict[str, str] | None = None,
        create_timeout: float | None = None,
        idempotency_key: str | None = None,
        client: AsyncSandboxPilot | None = None,
        **extra: Any,
    ) -> AsyncSandbox:
        owns = client is None
        client = client or AsyncSandboxPilot()
        try:
            request = SandboxCreateRequest(
                image=image,
                pool=pool,
                template=template,
                cpus=cpus,
                memory=memory,
                timeout=timeout,
                env=env or {},
                workdir=workdir,
                network=NetworkPolicy(network) if isinstance(network, str) else network,
                labels=labels or {},
                metadata=metadata or {},
                create_timeout=create_timeout,
                **extra,
            )
        except PydanticValidationError as exc:
            if owns:
                await client.aclose()
            raise ValidationError(_describe_validation_error(exc)) from exc
        try:
            info = await client.create_sandbox(request, idempotency_key=idempotency_key)
        except BaseException:
            if owns:
                await client.aclose()
            raise
        return cls(client, info, owns_client=owns)

    @classmethod
    async def connect(
        cls, sandbox_id: str, *, client: AsyncSandboxPilot | None = None
    ) -> AsyncSandbox:
        owns = client is None
        client = client or AsyncSandboxPilot()
        return cls(client, await client.get_sandbox_info(sandbox_id), owns_client=owns)

    async def __aenter__(self) -> AsyncSandbox:
        return self

    async def __aexit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        try:
            await self.kill()
        finally:
            if self._owns_client:
                await self._client.aclose()

    # -- identity -------------------------------------------------------------------------

    @property
    def id(self) -> str:
        return self.info.id

    @property
    def short_id(self) -> str:
        return self.info.short_id

    async def refresh(self) -> SandboxInfo:
        self.info = await self._client.get_sandbox_info(self.id)
        return self.info

    # -- lifecycle ------------------------------------------------------------------------

    async def kill(self) -> None:
        self.info = await self._client.kill_sandbox(self.id)

    async def set_timeout(self, timeout: str | int) -> SandboxInfo:
        self.info = SandboxInfo.model_validate(
            await self._client._json(
                "POST", f"/sandboxes/{self.id}/timeout", json={"timeout": timeout}
            )
        )
        return self.info

    # -- commands -------------------------------------------------------------------------

    def _cmd_body(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None,
        cwd: str | None,
        timeout: float | None,
        user: str | None,
        background: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"env": env or {}, "background": background}
        if isinstance(command, str):
            body["command"] = command
        else:
            body["args"] = list(command)
        if cwd:
            body["cwd"] = cwd
        if timeout:
            body["timeout"] = timeout
        if user:
            body["user"] = user
        return body

    async def run(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
        check: bool = False,
    ) -> CommandResult:
        """Run to completion. ``check=True`` raises on non-zero exit."""
        data = await self._client._json(
            "POST",
            f"/sandboxes/{self.id}/exec",
            json=self._cmd_body(command, env=env, cwd=cwd, timeout=timeout, user=user),
            timeout=None,
        )
        result = CommandResult.model_validate(data)
        if result.status.value == "TIMED_OUT":
            raise CommandTimeoutError(
                f"command timed out after {timeout}s", details={"result": data}
            )
        if check and result.exit_code != 0:
            raise CommandError(
                f"command exited with {result.exit_code}: {result.stderr.strip()[:500]}",
                details={"exit_code": result.exit_code},
            )
        return result

    async def run_background(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
    ) -> AsyncCommand:
        data = await self._client._json(
            "POST",
            f"/sandboxes/{self.id}/exec/start",
            json=self._cmd_body(
                command, env=env, cwd=cwd, timeout=timeout, user=user, background=True
            ),
        )
        return AsyncCommand(self, CommandInfo.model_validate(data))

    async def stream(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
    ) -> AsyncIterator[CommandEvent]:
        """Start a command and yield stdout/stderr/exit events as they happen."""
        cmd = await self.run_background(command, env=env, cwd=cwd, timeout=timeout, user=user)
        async for event in cmd.events():
            yield event

    # -- files ------------------------------------------------------------------------------

    async def write(self, path: str, content: str | bytes, *, mode: int | None = None) -> None:
        data = content.encode() if isinstance(content, str) else content
        params: dict[str, Any] = {"path": path}
        if mode is not None:
            params["mode"] = mode
        await self._client.request(
            "PUT",
            f"/sandboxes/{self.id}/files",
            params=params,
            content=data,
            headers={"Content-Length": str(len(data))},
            timeout=None,
        )

    async def read(self, path: str) -> bytes:
        resp = await self._client.request(
            "GET", f"/sandboxes/{self.id}/files", params={"path": path}, timeout=None
        )
        return resp.content

    async def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return (await self.read(path)).decode(encoding)

    async def upload(
        self, local: str | os.PathLike[str], remote: str, *, mode: int | None = None
    ) -> None:
        """Upload a local file (or a directory, sent as a tar) into the sandbox."""
        src = Path(local)
        if await asyncio.to_thread(src.is_dir):
            data = await asyncio.to_thread(_tar_directory, src)
            await self._client.request(
                "PUT",
                f"/sandboxes/{self.id}/files",
                params={"path": remote, "archive": "true"},
                content=data,
                headers={"Content-Length": str(len(data))},
                timeout=None,
            )
            return
        await self.write(remote, await asyncio.to_thread(src.read_bytes), mode=mode)

    async def download(self, remote: str, local: str | os.PathLike[str]) -> Path:
        """Download a file, or a whole directory (extracted under ``local``), from the sandbox."""
        dest = Path(local)
        try:
            content = await self.read(remote)
        except FileTransferError as exc:
            if exc.details.get("reason") != "is_directory":
                raise
            resp = await self._client.request(
                "GET",
                f"/sandboxes/{self.id}/files",
                params={"path": remote, "archive": "true"},
                timeout=None,
            )
            await asyncio.to_thread(_extract_directory, resp.content, dest)
            return dest
        if await asyncio.to_thread(dest.is_dir):
            dest = dest / Path(remote).name
        await asyncio.to_thread(dest.write_bytes, content)
        return dest

    # -- network ----------------------------------------------------------------------------

    async def get_url(self, port: int, *, expires_in: int | None = None) -> str:
        data = await self._client._json(
            "POST", f"/sandboxes/{self.id}/url", json={"port": port, "expires_in": expires_in}
        )
        return str(data["url"])


def _tar_directory(src: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(src, arcname=".")
    return buf.getvalue()


def _extract_directory(archive: bytes, dest: Path) -> None:
    """Extract a directory archive from the sandbox into ``dest`` (the archive's top-level
    directory entry is stripped so ``download("/work", "./out")`` fills ``./out``)."""
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:*") as tar:
        members = tar.getmembers()
        root = members[0].name.split("/", 1)[0] if members and members[0].isdir() else None
        for member in members:
            if root is not None:
                if member.name == root:
                    continue
                if member.name.startswith(root + "/"):
                    member.name = member.name[len(root) + 1 :]
        # "data" filter refuses absolute paths, parent traversal and special files.
        tar.extractall(dest, members=[m for m in members if m.name], filter="data")


def _describe_validation_error(exc: PydanticValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ())) or "request"
        parts.append(f"{loc}: {err.get('msg', 'invalid')}")
    return "invalid sandbox request: " + "; ".join(parts)


class AsyncCommand:
    """A background command. Poll :meth:`info`, :meth:`wait`, or iterate :meth:`events`."""

    def __init__(self, sandbox: AsyncSandbox, info: CommandInfo) -> None:
        self.sandbox = sandbox
        self.info = info

    @property
    def id(self) -> str:
        return self.info.command_id

    async def refresh(self) -> CommandInfo:
        self.info = CommandInfo.model_validate(
            await self.sandbox._client._json("GET", f"/sandboxes/{self.sandbox.id}/exec/{self.id}")
        )
        return self.info

    async def wait(self) -> CommandResult:
        data = await self.sandbox._client._json(
            "GET",
            f"/sandboxes/{self.sandbox.id}/exec/{self.id}/result",
            params={"wait": "true"},
            timeout=None,
        )
        return CommandResult.model_validate(data)

    async def logs(self) -> CommandLogs:
        return CommandLogs.model_validate(
            await self.sandbox._client._json(
                "GET", f"/sandboxes/{self.sandbox.id}/exec/{self.id}/logs"
            )
        )

    async def kill(self) -> CommandInfo:
        self.info = CommandInfo.model_validate(
            await self.sandbox._client._json(
                "DELETE", f"/sandboxes/{self.sandbox.id}/exec/{self.id}"
            )
        )
        return self.info

    async def events(self, from_seq: int = 0) -> AsyncIterator[CommandEvent]:
        client = self.sandbox._client
        http = await client.connect()
        try:
            async with http.stream(
                "GET",
                f"/sandboxes/{self.sandbox.id}/exec/{self.id}/stream",
                params={"from_seq": from_seq},
                timeout=None,
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    raise_for_status(resp)
                async for event in aparse_sse_lines(resp.aiter_lines()):
                    yield event
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, client.url) from exc
