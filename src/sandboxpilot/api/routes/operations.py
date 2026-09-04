"""Long-running operations (worker provisioning, image preloads)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.operations import Operation

router = APIRouter(prefix="/operations", tags=["operations"], dependencies=[Depends(require_auth)])


@router.get("/{op_id}", response_model=Operation)
async def get_operation(
    op_id: str,
    wait: float = Query(default=0, ge=0, le=3600, description="Seconds to wait for completion"),
    cp: ControlPlane = Depends(control),
) -> Operation:
    if wait:
        return await cp.wait_operation(op_id, wait)
    return await cp.get_operation(op_id)
