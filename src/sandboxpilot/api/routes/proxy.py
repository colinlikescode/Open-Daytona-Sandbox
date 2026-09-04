"""Signed-URL proxy to a port inside a sandbox (HTTP + WebSocket).

URL shape: ``/v1/proxy/{sandbox_id}/{port}/{token}/{path}``. The token is an
HMAC over (sandbox, port, expiry) so a URL can be handed to a browser without
leaking the API token. Requests are relayed control plane -> tunnel -> worker.
"""

from __future__ import annotations

import contextlib

from fastapi import APIRouter, Depends, Request, WebSocket
from fastapi.responses import Response

from sandboxpilot.api.deps import control, ws_control
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.errors import SandboxPilotError
from sandboxpilot.utils.proxy import build_target, proxy_http, proxy_websocket, websocket_url

router = APIRouter(prefix="/proxy", tags=["proxy"])

METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@router.api_route("/{sandbox_id}/{port}/{token}", methods=METHODS)
@router.api_route("/{sandbox_id}/{port}/{token}/{path:path}", methods=METHODS)
async def http_proxy(
    sandbox_id: str,
    port: int,
    token: str,
    request: Request,
    path: str = "",
    cp: ControlPlane = Depends(control),
) -> Response:
    cp.verify_proxy_token(token, sandbox_id, port)
    client, base = await cp.proxy_target(sandbox_id, port)
    target = build_target(base, path, request.url.query)
    return await proxy_http(request, target, request.app.state.http, extra_headers=client.headers())


@router.websocket("/{sandbox_id}/{port}/{token}")
@router.websocket("/{sandbox_id}/{port}/{token}/{path:path}")
async def ws_proxy(
    ws: WebSocket,
    sandbox_id: str,
    port: int,
    token: str,
    path: str = "",
    cp: ControlPlane = Depends(ws_control),
) -> None:
    try:
        cp.verify_proxy_token(token, sandbox_id, port)
    except SandboxPilotError:
        await ws.close(code=1008)
        return
    try:
        client, base = await cp.proxy_target(sandbox_id, port)
    except SandboxPilotError:
        await ws.close(code=1011)
        return
    target = build_target(websocket_url(base), path, ws.url.query)
    with contextlib.suppress(SandboxPilotError):
        await proxy_websocket(ws, target, extra_headers=client.headers())
