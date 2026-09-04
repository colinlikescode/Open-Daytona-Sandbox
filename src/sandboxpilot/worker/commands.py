"""Command execution manager: bounded output buffers, streaming subscribers, timeouts."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import datetime

from sandboxpilot.errors import CommandNotFoundError
from sandboxpilot.schemas.commands import (
    CommandEvent,
    CommandInfo,
    CommandLogs,
    CommandRequest,
    CommandResult,
    CommandStatus,
)
from sandboxpilot.utils.clock import Clock
from sandboxpilot.utils.ids import new_id
from sandboxpilot.utils.logging import get_logger
from sandboxpilot.worker.runtime.base import ExecHandle

log = get_logger("worker.commands")


class BoundedOutput:
    """Keeps the head and tail of a stream within ``limit`` bytes."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.head_limit = limit // 2
        self.tail_limit = limit - self.head_limit
        self.head = bytearray()
        self.tail = bytearray()
        self.total = 0
        self.truncated = False

    def write(self, data: bytes) -> None:
        self.total += len(data)
        if len(self.head) < self.head_limit:
            take = min(len(data), self.head_limit - len(self.head))
            self.head.extend(data[:take])
            data = data[take:]
            if not data:
                return
        self.truncated = True
        self.tail.extend(data)
        if len(self.tail) > self.tail_limit:
            del self.tail[: len(self.tail) - self.tail_limit]

    def text(self) -> str:
        if not self.truncated:
            return self.head.decode("utf-8", errors="replace")
        dropped = self.total - len(self.head) - len(self.tail)
        marker = f"\n... [{dropped} bytes truncated] ...\n".encode()
        return (bytes(self.head) + marker + bytes(self.tail)).decode("utf-8", errors="replace")


class CommandRun:
    def __init__(
        self,
        sandbox_id: str,
        request: CommandRequest,
        clock: Clock,
        output_limit: int,
        event_history_bytes: int,
    ) -> None:
        self.id = new_id("cmd")
        self.sandbox_id = sandbox_id
        self.request = request
        self.clock = clock
        self.status = CommandStatus.STARTING
        self.exit_code: int | None = None
        self.started_at: datetime = clock.now()
        self.finished_at: datetime | None = None
        self.error: str | None = None
        self.stdout = BoundedOutput(output_limit)
        self.stderr = BoundedOutput(output_limit)
        self._events: list[CommandEvent] = []
        self._events_bytes = 0
        self._events_limit = event_history_bytes
        self._history_dropped = False
        self._seq = 0
        self._cond = asyncio.Condition()
        self.handle: ExecHandle | None = None
        self.task: asyncio.Task[None] | None = None
        self._done = asyncio.Event()

    def _push(self, event: CommandEvent) -> None:
        event.seq = self._seq
        self._seq += 1
        self._events.append(event)
        self._events_bytes += len(event.text)
        while self._events_bytes > self._events_limit and len(self._events) > 1:
            dropped = self._events.pop(0)
            self._events_bytes -= len(dropped.text)
            self._history_dropped = True

    async def emit(self, event: CommandEvent) -> None:
        async with self._cond:
            self._push(event)
            self._cond.notify_all()

    async def finish(
        self, status: CommandStatus, exit_code: int | None, error: str | None = None
    ) -> None:
        self.status = status
        self.exit_code = exit_code
        self.error = error
        self.finished_at = self.clock.now()
        kind = "exit" if status in {CommandStatus.EXITED, CommandStatus.KILLED} else "error"
        await self.emit(
            CommandEvent(type=kind, text=error or "", exit_code=exit_code, status=status)
        )
        self._done.set()

    @property
    def done(self) -> bool:
        return self._done.is_set()

    async def wait(self, timeout: float | None = None) -> None:
        await asyncio.wait_for(self._done.wait(), timeout)

    async def subscribe(self, from_seq: int = 0) -> AsyncIterator[CommandEvent]:
        """Replay buffered events then follow live output until the command ends."""
        next_seq = from_seq
        while True:
            async with self._cond:
                pending = [e for e in self._events if e.seq >= next_seq]
                if not pending and not self.done:
                    await self._cond.wait()
                    continue
            for event in pending:
                next_seq = event.seq + 1
                yield event
                if event.type in {"exit", "error"}:
                    return
            if self.done and not pending:
                return

    def info(self) -> CommandInfo:
        return CommandInfo(
            command_id=self.id,
            sandbox_id=self.sandbox_id,
            status=self.status,
            exit_code=self.exit_code,
            started_at=self.started_at,
            finished_at=self.finished_at,
            display=self.request.display()[:200],
            error=self.error,
        )

    def result(self) -> CommandResult:
        return CommandResult(
            command_id=self.id,
            exit_code=self.exit_code if self.exit_code is not None else -1,
            stdout=self.stdout.text(),
            stderr=self.stderr.text(),
            started_at=self.started_at,
            finished_at=self.finished_at or self.clock.now(),
            status=self.status,
            output_truncated=self.stdout.truncated or self.stderr.truncated,
            error=self.error,
        )

    def logs(self) -> CommandLogs:
        return CommandLogs(
            command_id=self.id,
            stdout=self.stdout.text(),
            stderr=self.stderr.text(),
            truncated=self.stdout.truncated or self.stderr.truncated,
        )


class CommandManager:
    def __init__(
        self,
        clock: Clock,
        *,
        max_output_bytes: int,
        event_history_bytes: int | None = None,
        retain_finished: int = 500,
    ) -> None:
        self.clock = clock
        self.max_output_bytes = max_output_bytes
        self.event_history_bytes = event_history_bytes or max_output_bytes
        self.retain_finished = retain_finished
        self._runs: dict[str, CommandRun] = {}

    def get(self, command_id: str) -> CommandRun:
        run = self._runs.get(command_id)
        if run is None:
            raise CommandNotFoundError(f"Command {command_id} not found")
        return run

    def for_sandbox(self, sandbox_id: str) -> list[CommandRun]:
        return [r for r in self._runs.values() if r.sandbox_id == sandbox_id]

    async def start(self, sandbox_id: str, request: CommandRequest, handle_factory) -> CommandRun:  # type: ignore[no-untyped-def]
        run = CommandRun(
            sandbox_id, request, self.clock, self.max_output_bytes, self.event_history_bytes
        )
        self._runs[run.id] = run
        run.task = asyncio.create_task(self._drive(run, handle_factory), name=f"cmd-{run.id}")
        self._trim()
        return run

    async def _drive(self, run: CommandRun, handle_factory) -> None:  # type: ignore[no-untyped-def]
        try:
            run.handle = await handle_factory()
        except Exception as exc:
            await run.finish(CommandStatus.FAILED, None, str(exc))
            return
        run.status = CommandStatus.RUNNING
        timed_out = False

        async def pump() -> None:
            assert run.handle is not None
            async for stream, chunk in run.handle.stream():
                if stream == "stdout":
                    run.stdout.write(chunk)
                else:
                    run.stderr.write(chunk)
                await run.emit(
                    CommandEvent(type=stream, text=chunk.decode("utf-8", errors="replace"))
                )

        pump_task = asyncio.create_task(pump())
        try:
            if run.request.timeout:
                try:
                    await asyncio.wait_for(asyncio.shield(pump_task), run.request.timeout)
                except TimeoutError:
                    timed_out = True
                    with contextlib.suppress(Exception):
                        await run.handle.kill()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await asyncio.wait_for(pump_task, 5)
            else:
                await pump_task
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await run.handle.kill()
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pump_task
            await run.finish(CommandStatus.KILLED, 137, "killed")
            raise
        except Exception as exc:
            log.warning("command %s stream failed: %s", run.id, exc)
            await run.finish(CommandStatus.FAILED, None, str(exc))
            return
        finally:
            if not pump_task.done():
                pump_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await pump_task
        try:
            exit_code = await asyncio.wait_for(run.handle.wait(), 10)
        except Exception:
            exit_code = 137 if timed_out or run.status == CommandStatus.KILLED else -1
        if timed_out:
            await run.finish(
                CommandStatus.TIMED_OUT,
                exit_code,
                f"Command timed out after {run.request.timeout:g}s",
            )
        elif run.status == CommandStatus.KILLED:
            await run.finish(CommandStatus.KILLED, exit_code, "killed")
        else:
            await run.finish(CommandStatus.EXITED, exit_code)

    async def kill(self, command_id: str) -> CommandInfo:
        run = self.get(command_id)
        if run.done:
            return run.info()
        run.status = CommandStatus.KILLED
        if run.handle is not None:
            with contextlib.suppress(Exception):
                await run.handle.kill()
        if run.task and not run.task.done():
            with contextlib.suppress(TimeoutError, Exception):
                await asyncio.wait_for(run.task, 10)
        if not run.done:
            await run.finish(CommandStatus.KILLED, 137, "killed")
        return run.info()

    async def kill_all(self, sandbox_id: str) -> None:
        for run in self.for_sandbox(sandbox_id):
            if not run.done:
                with contextlib.suppress(Exception):
                    await self.kill(run.id)

    def _trim(self) -> None:
        finished = [r for r in self._runs.values() if r.done]
        if len(finished) > self.retain_finished:
            finished.sort(key=lambda r: r.finished_at or r.started_at)
            for run in finished[: len(finished) - self.retain_finished]:
                self._runs.pop(run.id, None)

    async def shutdown(self) -> None:
        for run in list(self._runs.values()):
            if not run.done:
                with contextlib.suppress(Exception):
                    await self.kill(run.id)
