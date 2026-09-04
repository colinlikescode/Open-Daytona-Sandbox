"""Worker pools: create, inspect, scale up/down, delete."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.schemas.pool import PoolCreateRequest, PoolUpdateRequest, WorkerPool

router = APIRouter(prefix="/pools", tags=["pools"], dependencies=[Depends(require_auth)])


class PoolUpRequest(BaseModel):
    workers: int | None = None


@router.get("", response_model=list[WorkerPool])
async def list_pools(cp: ControlPlane = Depends(control)) -> list[WorkerPool]:
    return await cp.list_pools()


@router.post("", response_model=WorkerPool, status_code=201)
async def create_pool(body: PoolCreateRequest, cp: ControlPlane = Depends(control)) -> WorkerPool:
    return await cp.create_pool(body)


@router.get("/{ref}", response_model=WorkerPool)
async def get_pool(ref: str, cp: ControlPlane = Depends(control)) -> WorkerPool:
    return await cp.get_pool(ref)


@router.patch("/{ref}", response_model=WorkerPool)
async def update_pool(
    ref: str, body: PoolUpdateRequest, cp: ControlPlane = Depends(control)
) -> WorkerPool:
    return await cp.update_pool(ref, body)


@router.delete("/{ref}", status_code=204)
async def delete_pool(
    ref: str, force: bool = Query(default=False), cp: ControlPlane = Depends(control)
) -> None:
    await cp.delete_pool(ref, force=force)


@router.post("/{ref}/up", response_model=list[Operation], status_code=202)
async def pool_up(
    ref: str, body: PoolUpRequest | None = None, cp: ControlPlane = Depends(control)
) -> list[Operation]:
    return await cp.pool_up(ref, workers=body.workers if body else None)


@router.post("/{ref}/down")
async def pool_down(
    ref: str, force: bool = Query(default=False), cp: ControlPlane = Depends(control)
) -> dict[str, Any]:
    return await cp.pool_down(ref, force=force)
