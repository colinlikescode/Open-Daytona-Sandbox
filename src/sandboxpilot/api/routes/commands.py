"""Commands inside a sandbox: run, background, stream (SSE), logs, kill."""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse, Response, StreamingResponse

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.commands import CommandInfo, CommandLogs, CommandRequest, CommandResult

router = APIRouter(
    prefix="/sandboxes/{ref}/exec", tags=["commands"], dependencies=[Depends(require_auth)]
)


@router.post("")
async def exec_command(
    ref: str, body: CommandRequest, cp: ControlPlane = Depends(control)
) -> Response:
    if body.background:
        info = await cp.start_command(ref, body)
        return JSONResponse(status_code=202, content=info.model_dump(mode="json"))
    result = await cp.run_command(ref, body)
    return JSONResponse(content=result.model_dump(mode="json"))


@router.post("/start", response_model=CommandInfo, status_code=202)
async def start_command(
    ref: str, body: CommandRequest, cp: ControlPlane = Depends(control)
) -> CommandInfo:
    return await cp.start_command(ref, body)


@router.get("/{command_id}", response_model=CommandInfo)
async def get_command(
    ref: str, command_id: str, cp: ControlPlane = Depends(control)
) -> CommandInfo:
    return await cp.get_command(ref, command_id)


@router.get("/{command_id}/result", response_model=CommandResult)
async def command_result(
    ref: str,
    command_id: str,
    wait: bool = Query(default=True),
    cp: ControlPlane = Depends(control),
) -> CommandResult:
    return await cp.command_result(ref, command_id, wait=wait)


@router.get("/{command_id}/logs", response_model=CommandLogs)
async def command_logs(
    ref: str, command_id: str, cp: ControlPlane = Depends(control)
) -> CommandLogs:
    return await cp.command_logs(ref, command_id)


@router.delete("/{command_id}", response_model=CommandInfo)
async def kill_command(
    ref: str, command_id: str, cp: ControlPlane = Depends(control)
) -> CommandInfo:
    return await cp.kill_command(ref, command_id)


@router.get("/{command_id}/stream")
async def stream_command(
    ref: str,
    command_id: str,
    from_seq: int = Query(default=0, ge=0),
    cp: ControlPlane = Depends(control),
) -> StreamingResponse:
    async def events() -> AsyncIterator[bytes]:
        async for event in cp.stream_command(ref, command_id, from_seq):
            yield event.to_sse().encode()

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
