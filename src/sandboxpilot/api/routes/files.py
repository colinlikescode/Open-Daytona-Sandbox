"""File transfer. Bodies stream through to the worker; nothing is buffered here."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.errors import FileTransferError, ValidationError
from sandboxpilot.schemas.files import MAX_FILE_MODE, validate_sandbox_path

router = APIRouter(
    prefix="/sandboxes/{ref}/files", tags=["files"], dependencies=[Depends(require_auth)]
)


def _check_path(path: str) -> None:
    try:
        validate_sandbox_path(path)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc


@router.put("")
async def upload(
    ref: str,
    request: Request,
    path: str = Query(...),
    archive: bool = Query(default=False),
    mode: int | None = Query(default=None, ge=0, le=MAX_FILE_MODE),
    cp: ControlPlane = Depends(control),
) -> dict[str, Any]:
    _check_path(path)
    limit = cp.config.limits.max_upload_size
    length = request.headers.get("content-length")
    size = int(length) if length else None
    if size is not None and size > limit:
        raise FileTransferError(
            f"upload exceeds max_upload_size ({limit} bytes)", details={"limit": limit}
        )

    counted = 0

    async def body() -> AsyncIterator[bytes]:
        nonlocal counted
        async for chunk in request.stream():
            counted += len(chunk)
            if counted > limit:
                raise FileTransferError(
                    f"upload exceeds max_upload_size ({limit} bytes)", details={"limit": limit}
                )
            if chunk:
                yield chunk

    if size is None:
        # Chunked upload: we need the size for the tar header, so collect it (bounded by limit).
        data = b"".join([c async for c in body()])
        await cp.upload(ref, path, data, archive=archive, mode=mode, size=len(data))
    else:
        await cp.upload(ref, path, body(), archive=archive, mode=mode, size=size)
    return {"path": path, "bytes": counted}


@router.get("")
async def download(
    ref: str,
    path: str = Query(...),
    archive: bool = Query(default=False),
    cp: ControlPlane = Depends(control),
) -> StreamingResponse:
    _check_path(path)
    limit = cp.config.limits.max_download_size
    source = cp.download(ref, path, archive=archive)
    # Pull the first chunk now so worker-side errors (missing file) become HTTP errors.
    try:
        first = await source.__anext__()
    except StopAsyncIteration:
        first = b""

    async def rest() -> AsyncIterator[bytes]:
        total = len(first)
        yield first
        async for chunk in source:
            total += len(chunk)
            if total > limit:
                raise FileTransferError(
                    f"download exceeds max_download_size ({limit} bytes)", details={"limit": limit}
                )
            yield chunk

    media = "application/x-tar" if archive else "application/octet-stream"
    return StreamingResponse(rest(), media_type=media)
