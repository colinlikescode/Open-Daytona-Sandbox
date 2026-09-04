"""Long-running operation model."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from sandboxpilot.schemas.common import OperationStatus
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.ids import new_id


class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: new_id("op"))
    type: str
    status: OperationStatus = OperationStatus.PENDING
    pool_id: str | None = None
    worker_id: str | None = None
    sandbox_id: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    result: dict[str, Any] = Field(default_factory=dict)

    def start(self, now: datetime | None = None) -> None:
        self.status = OperationStatus.RUNNING
        self.started_at = now or utcnow()

    def succeed(self, result: dict[str, Any] | None = None, now: datetime | None = None) -> None:
        self.status = OperationStatus.SUCCEEDED
        self.completed_at = now or utcnow()
        if result:
            self.result.update(result)

    def fail(self, error: str, now: datetime | None = None) -> None:
        self.status = OperationStatus.FAILED
        self.completed_at = now or utcnow()
        self.error = error

    def cancel(self, now: datetime | None = None) -> None:
        self.status = OperationStatus.CANCELLED
        self.completed_at = now or utcnow()
