"""FastAPI dependencies shared by all route modules."""

from __future__ import annotations

from fastapi import Header, Request, WebSocket

from sandboxpilot.api.auth import check_api_token
from sandboxpilot.control.service import ControlPlane


def control(request: Request) -> ControlPlane:
    cp: ControlPlane = request.app.state.control
    return cp


def ws_control(ws: WebSocket) -> ControlPlane:
    cp: ControlPlane = ws.app.state.control
    return cp


async def require_auth(request: Request, authorization: str | None = Header(default=None)) -> None:
    check_api_token(authorization, request.app.state.api_token)


def ws_require_auth(ws: WebSocket) -> None:
    check_api_token(ws.headers.get("authorization"), ws.app.state.api_token)
