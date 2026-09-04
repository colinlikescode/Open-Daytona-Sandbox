"""FastAPI application for the control plane.

``create_app(control_plane)`` is the only thing tests and the daemon need.
Everything under ``/v1`` except ``/v1/health`` and the signed proxy requires
the API token when one is configured.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from sandboxpilot.api.routes import build_router
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.errors import SandboxPilotError, ValidationError
from sandboxpilot.utils.logging import get_logger
from sandboxpilot.version import __version__

log = get_logger("api")


def create_app(control_plane: ControlPlane, *, manage_lifecycle: bool = True) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=None))
        if manage_lifecycle:
            await control_plane.start()
        try:
            yield
        finally:
            if manage_lifecycle:
                await control_plane.stop()
            await app.state.http.aclose()

    app = FastAPI(
        title="SandboxPilot",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )
    app.state.control = control_plane
    app.state.api_token = control_plane.config.api.token

    @app.exception_handler(SandboxPilotError)
    async def _handle_ours(_: Request, exc: SandboxPilotError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content={"error": exc.to_dict()})

    @app.exception_handler(RequestValidationError)
    async def _handle_validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        err = ValidationError("invalid request", details={"errors": exc.errors()})
        return JSONResponse(status_code=err.http_status, content={"error": err.to_dict()})

    app.include_router(build_router(), prefix="/v1")
    return app
