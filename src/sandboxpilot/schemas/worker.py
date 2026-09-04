"""Worker models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from sandboxpilot.schemas.common import CloudProvider, WorkerState
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.ids import new_id, short_id


class WorkerCapacity(BaseModel):
    """Capacity as reported by a worker (totals and what may be allocated)."""

    cpu_millis_total: int = 0
    cpu_millis_allocatable: int = 0
    memory_bytes_total: int = 0
    memory_bytes_allocatable: int = 0
    cpu_millis_allocated: int = 0
    memory_bytes_allocated: int = 0
    sandboxes_running: int = 0
    sandboxes_warm: int = 0
    sandboxes_pending: int = 0
    disk_free_bytes: int | None = None
    disk_total_bytes: int | None = None
    disk_pressure: bool = False
    disk_limit_enforced: bool = False

    @property
    def cpu_millis_available(self) -> int:
        return max(0, self.cpu_millis_allocatable - self.cpu_millis_allocated)

    @property
    def memory_bytes_available(self) -> int:
        return max(0, self.memory_bytes_allocatable - self.memory_bytes_allocated)


class WorkerHealth(BaseModel):
    """Response of the worker ``GET /v1/health`` endpoint."""

    status: str = "healthy"
    worker_id: str
    sandboxpilot_version: str
    worker_protocol_version: int
    runtime: str
    docker_version: str | None = None
    runsc_version: str | None = None
    capacity: WorkerCapacity
    sandboxes: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    draining: bool = False
    uptime_seconds: float | None = None


class WorkerRecord(BaseModel):
    """Persisted worker state (internal; contains the worker token)."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: new_id("wrk"))
    pool_id: str
    pool_name: str
    state: WorkerState = WorkerState.PROVISIONING

    cloud: CloudProvider | None = None
    region: str | None = None
    zone: str | None = None
    instance_type: str | None = None
    use_spot: bool = False
    hourly_cost: float | None = None

    provider_cluster: str
    provider_metadata: dict[str, Any] = Field(default_factory=dict)

    token: str = Field(repr=False)
    tunnel_port: int | None = None

    capacity: WorkerCapacity = Field(default_factory=WorkerCapacity)
    version: str | None = None
    protocol_version: int | None = None

    draining: bool = False
    terminate_when_empty: bool = False

    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    healthy_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    idle_since: datetime | None = None
    consecutive_failures: int = 0
    last_error: str | None = None
    provision_seconds: float | None = None

    @property
    def short_id(self) -> str:
        return short_id(self.id, 6)

    def to_view(self, sandboxes_running: int | None = None) -> WorkerView:
        data = self.model_dump(exclude={"token"})
        data["short_id"] = self.short_id
        if sandboxes_running is not None:
            data["capacity"]["sandboxes_running"] = sandboxes_running
        return WorkerView.model_validate(data)


class WorkerView(BaseModel):
    """Public worker representation (never includes the token)."""

    id: str
    short_id: str
    pool_id: str
    pool_name: str
    state: WorkerState
    cloud: CloudProvider | None
    region: str | None
    zone: str | None
    instance_type: str | None
    use_spot: bool
    hourly_cost: float | None
    provider_cluster: str
    provider_metadata: dict[str, Any]
    tunnel_port: int | None
    capacity: WorkerCapacity
    version: str | None
    protocol_version: int | None
    draining: bool
    terminate_when_empty: bool
    created_at: datetime
    updated_at: datetime
    healthy_at: datetime | None
    last_heartbeat_at: datetime | None
    idle_since: datetime | None
    consecutive_failures: int
    last_error: str | None
    provision_seconds: float | None
