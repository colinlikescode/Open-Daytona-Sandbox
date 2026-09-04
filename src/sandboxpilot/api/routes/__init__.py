"""One router per resource. ``app.py`` mounts them all under ``/v1``."""

from fastapi import APIRouter

from sandboxpilot.api.routes import (
    commands,
    files,
    images,
    operations,
    pools,
    proxy,
    sandboxes,
    system,
    templates,
    workers,
)


def build_router() -> APIRouter:
    router = APIRouter()
    for module in (
        system,
        pools,
        workers,
        sandboxes,
        commands,
        files,
        proxy,
        templates,
        images,
        operations,
    ):
        router.include_router(module.router)
    return router


__all__ = ["build_router"]
