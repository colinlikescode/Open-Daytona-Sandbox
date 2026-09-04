"""Command execution models."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CommandStatus(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    EXITED = "EXITED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    TIMED_OUT = "TIMED_OUT"

    @property
    def is_terminal(self) -> bool:
        return self in {
            CommandStatus.EXITED,
            CommandStatus.FAILED,
            CommandStatus.KILLED,
            CommandStatus.TIMED_OUT,
        }


class CommandRequest(BaseModel):
    """Run a command in a sandbox.

    Either ``command`` (a shell string executed with ``/bin/sh -lc``) or
    ``args`` (an argument list executed without a shell) must be given.
    """

    model_config = ConfigDict(extra="forbid")

    command: str | None = None
    args: list[str] | None = None
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    timeout: float | None = Field(default=None, gt=0)
    background: bool = False
    user: str | None = None

    @model_validator(mode="after")
    def _one_of(self) -> CommandRequest:
        if bool(self.command) == bool(self.args):
            raise ValueError("exactly one of 'command' or 'args' must be provided")
        if self.args is not None and not self.args:
            raise ValueError("'args' must not be empty")
        return self

    def argv(self) -> list[str]:
        if self.args:
            return list(self.args)
        assert self.command is not None
        return ["/bin/sh", "-lc", self.command]

    def display(self) -> str:
        return self.command if self.command else " ".join(self.args or [])


class CommandResult(BaseModel):
    command_id: str
    exit_code: int
    stdout: str
    stderr: str
    started_at: datetime
    finished_at: datetime
    status: CommandStatus = CommandStatus.EXITED
    output_truncated: bool = False
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


class CommandInfo(BaseModel):
    """Status of a (possibly background) command."""

    command_id: str
    sandbox_id: str
    status: CommandStatus
    exit_code: int | None = None
    started_at: datetime
    finished_at: datetime | None = None
    display: str = ""
    error: str | None = None


class CommandEvent(BaseModel):
    """Streaming event. ``type`` is one of stdout, stderr, exit, error."""

    type: Literal["stdout", "stderr", "exit", "error"]
    text: str = ""
    exit_code: int | None = None
    status: CommandStatus | None = None
    seq: int = 0

    def to_sse(self) -> str:
        return f"event: {self.type}\ndata: {self.model_dump_json()}\n\n"


class CommandLogs(BaseModel):
    command_id: str
    stdout: str
    stderr: str
    truncated: bool = False
