"""Workers: list, inspect, drain, remove. Tokens are never returned."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.worker import WorkerView

router = APIRouter(prefix="/workers", tags=["workers"], dependencies=[Depends(require_auth)])


@router.get("", response_model=list[WorkerView])
async def list_workers(
    pool: str | None = Query(default=None), cp: ControlPlane = Depends(control)
) -> list[WorkerView]:
    views = []
    for w in await cp.list_workers(pool):
        views.append(w.to_view(await cp.sandboxes_on_worker(w.id)))
    return views


@router.get("/{ref}", response_model=WorkerView)
async def get_worker(ref: str, cp: ControlPlane = Depends(control)) -> WorkerView:
    w = await cp.get_worker(ref)
    return w.to_view(await cp.sandboxes_on_worker(w.id))


@router.post("/{ref}/drain", response_model=WorkerView)
async def drain_worker(
    ref: str,
    terminate_when_empty: bool = Query(default=True),
    cp: ControlPlane = Depends(control),
) -> WorkerView:
    w = await cp.drain_worker(ref, terminate_when_empty=terminate_when_empty)
    return w.to_view(await cp.sandboxes_on_worker(w.id))


@router.delete("/{ref}", response_model=WorkerView)
async def remove_worker(
    ref: str, force: bool = Query(default=False), cp: ControlPlane = Depends(control)
) -> WorkerView:
    return (await cp.remove_worker(ref, force=force)).to_view(0)
