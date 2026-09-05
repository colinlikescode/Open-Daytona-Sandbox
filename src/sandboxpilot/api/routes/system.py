"""Health, status, doctor, metrics, cleanup."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Response

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.version import API_VERSION, __version__

router = APIRouter(tags=["system"])


@router.get("/health")
async def health(cp: ControlPlane = Depends(control)) -> dict[str, Any]:
    # Unauthenticated on purpose: the SDK uses this to find out if a control plane is up.
    return {
        "status": "ok",
        "version": __version__,
        "api_version": API_VERSION,
        "provider": cp.provider.name,
        "started_at": cp.started_at.isoformat() if cp.started_at else None,
    }


@router.get("/status", dependencies=[Depends(require_auth)])
async def status(cp: ControlPlane = Depends(control)) -> dict[str, Any]:
    return await cp.status()


@router.get("/doctor", dependencies=[Depends(require_auth)])
async def doctor(cp: ControlPlane = Depends(control)) -> dict[str, Any]:
    return await cp.doctor()


@router.post("/cleanup", dependencies=[Depends(require_auth)])
async def cleanup(
    terminate_workers: bool = Query(default=False), cp: ControlPlane = Depends(control)
) -> dict[str, Any]:
    return await cp.cleanup(terminate_workers=terminate_workers)


@router.get("/metrics", dependencies=[Depends(require_auth)])
async def metrics(cp: ControlPlane = Depends(control)) -> Response:
    return Response(content=cp.metrics.render(), media_type="text/plain; version=0.0.4")
