"""Container images: preload onto workers, list what is cached where."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.operations import Operation

router = APIRouter(prefix="/images", tags=["images"], dependencies=[Depends(require_auth)])


class PreloadRequest(BaseModel):
    reference: str
    pool: str | None = None


@router.get("")
async def list_images(
    worker: str | None = Query(default=None), cp: ControlPlane = Depends(control)
) -> list[dict[str, Any]]:
    return await cp.list_images(worker=worker)


@router.post("/preload", response_model=Operation)
async def preload(body: PreloadRequest, cp: ControlPlane = Depends(control)) -> Operation:
    return await cp.preload_image(body.reference, pool=body.pool)
