"""Sandbox lifecycle: create, list, inspect, timeout, kill, port URLs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.common import SandboxState
from sandboxpilot.schemas.sandbox import (
    SandboxCreateRequest,
    SandboxInfo,
    SandboxRecord,
    SandboxTimeoutRequest,
)

router = APIRouter(prefix="/sandboxes", tags=["sandboxes"], dependencies=[Depends(require_auth)])


class UrlRequest(BaseModel):
    port: int
    expires_in: int | None = None


async def _info(cp: ControlPlane, sb: SandboxRecord) -> SandboxInfo:
    worker_state = None
    if sb.worker_id:
        worker = await cp.db.workers.get(sb.worker_id)
        worker_state = worker.state.value if worker else None
    return sb.to_info(worker_state=worker_state)


@router.get("", response_model=list[SandboxInfo])
async def list_sandboxes(
    pool: str | None = Query(default=None),
    all: bool = Query(default=False, description="Include stopped/failed sandboxes"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    cp: ControlPlane = Depends(control),
) -> list[SandboxInfo]:
    records = await cp.list_sandboxes(pool=pool, active_only=not all, limit=limit)
    return [await _info(cp, sb) for sb in records]


@router.post("", response_model=SandboxInfo)
async def create_sandbox(
    body: SandboxCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    cp: ControlPlane = Depends(control),
) -> JSONResponse:
    sb = await cp.create_sandbox(body, idempotency_key=idempotency_key)
    info = await _info(cp, sb)
    status = 201 if sb.state == SandboxState.RUNNING else 202
    return JSONResponse(status_code=status, content=info.model_dump(mode="json"))


@router.get("/{ref}", response_model=SandboxInfo)
async def get_sandbox(ref: str, cp: ControlPlane = Depends(control)) -> SandboxInfo:
    return await _info(cp, await cp.get_sandbox(ref))


@router.delete("/{ref}", response_model=SandboxInfo)
async def kill_sandbox(ref: str, cp: ControlPlane = Depends(control)) -> SandboxInfo:
    return await _info(cp, await cp.kill_sandbox(ref))


@router.post("/{ref}/timeout", response_model=SandboxInfo)
async def set_timeout(
    ref: str, body: SandboxTimeoutRequest, cp: ControlPlane = Depends(control)
) -> SandboxInfo:
    return await _info(cp, await cp.set_timeout(ref, body.seconds()))


@router.post("/{ref}/url")
async def get_url(
    ref: str, body: UrlRequest, cp: ControlPlane = Depends(control)
) -> dict[str, Any]:
    return await cp.get_url(ref, body.port, body.expires_in)
