"""Worker private HTTP API (loopback only, bearer-authenticated)."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from collections.abc import AsyncIterator
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, Query, Request, WebSocket
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

from sandboxpilot.errors import (
    FileTransferError,
    SandboxPilotError,
    ValidationError,
    WorkerAuthenticationError,
)
from sandboxpilot.schemas.commands import CommandInfo, CommandLogs, CommandRequest, CommandResult
from sandboxpilot.schemas.files import MAX_FILE_MODE, validate_sandbox_path
from sandboxpilot.schemas.worker import WorkerCapacity, WorkerHealth
from sandboxpilot.utils.clock import Clock
from sandboxpilot.utils.logging import configure_logging, get_logger
from sandboxpilot.utils.proxy import build_target, proxy_http, proxy_websocket
from sandboxpilot.utils.tarstream import (
    basename_and_parent,
    first_regular_file,
    single_file_tar,
    single_file_tar_bytes,
)
from sandboxpilot.version import __version__
from sandboxpilot.worker.auth import check_bearer
from sandboxpilot.worker.config import WorkerConfig
from sandboxpilot.worker.runtime.base import ImageInfo, SandboxRuntime
from sandboxpilot.worker.service import (
    ExpirationUpdate,
    WorkerSandboxCreate,
    WorkerSandboxView,
    WorkerService,
)

log = get_logger("worker.app")


class DrainRequest(BaseModel):
    draining: bool = True


class PullRequest(BaseModel):
    reference: str
    policy: str = "if-not-present"


def build_runtime(config: WorkerConfig, env: dict[str, str] | None = None) -> SandboxRuntime:
    env = env if env is not None else dict(os.environ)
    if config.runtime == "fake":
        from sandboxpilot.worker.runtime.fake import FakeSandboxRuntime

        return FakeSandboxRuntime()
    from sandboxpilot.worker.runtime.docker_gvisor import GVisorDockerRuntime

    return GVisorDockerRuntime(
        network_name=config.network_name,
        network_subnet=config.network_subnet,
        docker_host=config.docker_host,
        unsafe_runc=config.runtime == "docker-unsafe",
        sandbox_dns=config.sandbox_dns_list,
        state_dir=config.state_dir,
    )


def create_worker_app(
    config: WorkerConfig,
    runtime: SandboxRuntime | None = None,
    *,
    clock: Clock | None = None,
    service: WorkerService | None = None,
) -> FastAPI:
    svc = service or WorkerService(config, runtime or build_runtime(config), clock=clock)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=None))
        await svc.start()
        try:
            yield
        finally:
            await svc.shutdown()
            await app.state.http.aclose()

    app = FastAPI(
        title="SandboxPilot Worker",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )
    app.state.service = svc
    app.state.config = config

    async def auth(authorization: str | None = Header(default=None)) -> None:
        check_bearer(authorization, config.token)

    def ws_auth(ws: WebSocket) -> None:
        check_bearer(ws.headers.get("authorization"), config.token)

    @app.exception_handler(SandboxPilotError)
    async def _handle(_: Request, exc: SandboxPilotError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content={"error": exc.to_dict()})

    prefix = "/v1"

    @app.get(f"{prefix}/health", response_model=WorkerHealth, dependencies=[Depends(auth)])
    async def health() -> WorkerHealth:
        return await svc.health()

    @app.get(f"{prefix}/capacity", response_model=WorkerCapacity, dependencies=[Depends(auth)])
    async def capacity() -> WorkerCapacity:
        return svc.capacity_snapshot()

    @app.post(f"{prefix}/drain", dependencies=[Depends(auth)])
    async def drain(body: DrainRequest) -> dict[str, bool]:
        svc.set_draining(body.draining)
        return {"draining": body.draining}

    @app.get(f"{prefix}/images", response_model=list[ImageInfo], dependencies=[Depends(auth)])
    async def images() -> list[ImageInfo]:
        return await svc.runtime.list_images()

    @app.post(f"{prefix}/images/pull", response_model=ImageInfo, dependencies=[Depends(auth)])
    async def pull(body: PullRequest) -> ImageInfo:
        return await svc.runtime.ensure_image(body.reference, body.policy)

    @app.get(
        f"{prefix}/sandboxes", response_model=list[WorkerSandboxView], dependencies=[Depends(auth)]
    )
    async def list_sandboxes() -> list[WorkerSandboxView]:
        return await svc.list_sandboxes()

    @app.post(
        f"{prefix}/sandboxes",
        response_model=WorkerSandboxView,
        status_code=201,
        dependencies=[Depends(auth)],
    )
    async def create_sandbox(body: WorkerSandboxCreate) -> WorkerSandboxView:
        return await svc.create_sandbox(body)

    @app.get(
        f"{prefix}/sandboxes/{{sandbox_id}}",
        response_model=WorkerSandboxView,
        dependencies=[Depends(auth)],
    )
    async def get_sandbox(sandbox_id: str) -> WorkerSandboxView:
        return await svc.get_sandbox(sandbox_id)

    @app.delete(f"{prefix}/sandboxes/{{sandbox_id}}", dependencies=[Depends(auth)])
    async def delete_sandbox(sandbox_id: str) -> dict[str, Any]:
        view = await svc.delete_sandbox(sandbox_id)
        return {"deleted": True, "sandbox": view.model_dump(mode="json") if view else None}

    @app.patch(
        f"{prefix}/sandboxes/{{sandbox_id}}/expiration",
        response_model=WorkerSandboxView,
        dependencies=[Depends(auth)],
    )
    async def set_expiration(sandbox_id: str, body: ExpirationUpdate) -> WorkerSandboxView:
        return await svc.set_expiration(sandbox_id, body.expires_at)

    # -- commands ------------------------------------------------------------------------------

    @app.post(f"{prefix}/sandboxes/{{sandbox_id}}/exec", dependencies=[Depends(auth)])
    async def exec_command(sandbox_id: str, body: CommandRequest) -> Response:
        if body.background:
            run = await svc.start_command(sandbox_id, body)
            return JSONResponse(status_code=202, content=run.info().model_dump(mode="json"))
        result = await svc.run_command(sandbox_id, body)
        return JSONResponse(content=result.model_dump(mode="json"))

    @app.post(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/start",
        response_model=CommandInfo,
        status_code=202,
        dependencies=[Depends(auth)],
    )
    async def start_command(sandbox_id: str, body: CommandRequest) -> CommandInfo:
        run = await svc.start_command(sandbox_id, body)
        return run.info()

    @app.get(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/{{command_id}}",
        response_model=CommandInfo,
        dependencies=[Depends(auth)],
    )
    async def get_command(sandbox_id: str, command_id: str) -> CommandInfo:
        return svc.get_command(command_id)

    @app.get(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/{{command_id}}/result",
        response_model=CommandResult,
        dependencies=[Depends(auth)],
    )
    async def command_result(
        sandbox_id: str, command_id: str, wait: bool = Query(default=True)
    ) -> CommandResult:
        run = svc.commands.get(command_id)
        if wait:
            await run.wait()
        return run.result()

    @app.get(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/{{command_id}}/logs",
        response_model=CommandLogs,
        dependencies=[Depends(auth)],
    )
    async def command_logs(sandbox_id: str, command_id: str) -> CommandLogs:
        return svc.command_logs(command_id)

    @app.delete(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/{{command_id}}",
        response_model=CommandInfo,
        dependencies=[Depends(auth)],
    )
    async def kill_command(sandbox_id: str, command_id: str) -> CommandInfo:
        return await svc.kill_command(command_id)

    @app.get(
        f"{prefix}/sandboxes/{{sandbox_id}}/exec/{{command_id}}/stream",
        dependencies=[Depends(auth)],
    )
    async def stream_command(
        sandbox_id: str, command_id: str, from_seq: int = Query(default=0)
    ) -> StreamingResponse:
        run = svc.commands.get(command_id)

        async def events() -> AsyncIterator[bytes]:
            async for event in run.subscribe(from_seq):
                yield event.to_sse().encode()

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # -- files ----------------------------------------------------------------------------------

    @app.put(f"{prefix}/sandboxes/{{sandbox_id}}/files", dependencies=[Depends(auth)])
    async def upload(
        sandbox_id: str,
        request: Request,
        path: str = Query(...),
        archive: bool = Query(default=False),
        mode: int | None = Query(default=None, ge=0, le=MAX_FILE_MODE),
    ) -> dict[str, Any]:
        try:
            validate_sandbox_path(path)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        length_header = request.headers.get("content-length")
        limit = config.max_upload_size
        if length_header and int(length_header) > limit:
            raise FileTransferError(
                f"upload exceeds max_upload_size ({limit} bytes)", details={"limit": limit}
            )

        counted = 0

        async def body() -> AsyncIterator[bytes]:
            nonlocal counted
            async for chunk in request.stream():
                counted += len(chunk)
                if counted > limit:
                    raise FileTransferError(
                        f"upload exceeds max_upload_size ({limit} bytes)", details={"limit": limit}
                    )
                if chunk:
                    yield chunk

        if archive:
            await svc.upload(sandbox_id, path, body())
        else:
            name, parent = basename_and_parent(path)
            if not name:
                raise ValidationError("path must name a file")
            if length_header is not None:
                stream = single_file_tar(name, body(), int(length_header), mode or 0o644)
            else:
                data = b"".join([c async for c in body()])

                async def one() -> AsyncIterator[bytes]:
                    yield single_file_tar_bytes(name, data, mode or 0o644)

                stream = one()
            await svc.upload(sandbox_id, parent, stream)
        return {"path": path, "bytes": counted}

    @app.get(f"{prefix}/sandboxes/{{sandbox_id}}/files", dependencies=[Depends(auth)])
    async def download(
        sandbox_id: str, path: str = Query(...), archive: bool = Query(default=False)
    ) -> StreamingResponse:
        try:
            validate_sandbox_path(path)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        limit = config.max_download_size
        source = svc.download(sandbox_id, path)

        async def limited(inner: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
            total = 0
            async for chunk in inner:
                total += len(chunk)
                if total > limit:
                    raise FileTransferError(
                        f"download exceeds max_download_size ({limit} bytes)",
                        details={"limit": limit},
                    )
                yield chunk

        if archive:
            return StreamingResponse(limited(source), media_type="application/x-tar")
        content = first_regular_file(limited(source))
        # Pull the first chunk eagerly so path errors surface as HTTP errors, not mid-stream failures.
        first: bytes | None
        try:
            first = await content.__anext__()
        except StopAsyncIteration:
            first = None

        async def rest() -> AsyncIterator[bytes]:
            if first is not None:
                yield first
                async for chunk in content:
                    yield chunk

        return StreamingResponse(rest(), media_type="application/octet-stream")

    # -- proxy ------------------------------------------------------------------------------------

    @app.api_route(
        f"{prefix}/sandboxes/{{sandbox_id}}/proxy/{{port}}/{{path:path}}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        dependencies=[Depends(auth)],
    )
    async def http_proxy(sandbox_id: str, port: int, path: str, request: Request) -> Response:
        host, target_port = await svc.endpoint(sandbox_id, port)
        target = build_target(f"http://{host}:{target_port}", path, request.url.query)
        return await proxy_http(request, target, request.app.state.http)

    @app.websocket(f"{prefix}/sandboxes/{{sandbox_id}}/proxy/{{port}}/{{path:path}}")
    async def ws_proxy(ws: WebSocket, sandbox_id: str, port: int, path: str) -> None:
        try:
            ws_auth(ws)
        except WorkerAuthenticationError:
            await ws.close(code=1008)
            return
        try:
            host, target_port = await svc.endpoint(sandbox_id, port)
        except SandboxPilotError:
            await ws.close(code=1011)
            return
        target = build_target(f"ws://{host}:{target_port}", path, ws.url.query)
        with contextlib.suppress(SandboxPilotError):
            await proxy_websocket(ws, target)

    return app


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``sandboxpilot-worker``."""
    import uvicorn

    config = WorkerConfig()
    configure_logging(config.log_level, json_format=config.log_format == "json")
    try:
        config.validate_for_serving(dict(os.environ))
    except SandboxPilotError as exc:
        print(f"sandboxpilot-worker: {exc}", file=sys.stderr)
        return 2
    app = create_worker_app(config)
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
        access_log=False,
    )
    return 0


async def serve_in_process(app: FastAPI, host: str = "127.0.0.1", port: int = 0) -> tuple[Any, int]:
    """Start a uvicorn server in the current event loop; returns (server, bound_port)."""
    import socket

    import uvicorn

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)
    bound_port = sock.getsockname()[1]
    server_config = uvicorn.Config(app, log_level="warning", access_log=False, lifespan="on")
    server = uvicorn.Server(server_config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    server._sp_task = task  # type: ignore[attr-defined]
    for _ in range(200):
        if server.started:
            break
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    return server, bound_port


async def stop_in_process(server: Any) -> None:
    server.should_exit = True
    task = getattr(server, "_sp_task", None)
    if task is not None:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, 10)


__all__ = ["create_worker_app", "main", "serve_in_process", "stop_in_process"]
