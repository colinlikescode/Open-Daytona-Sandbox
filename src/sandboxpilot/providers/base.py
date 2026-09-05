"""ComputeProvider interface and shared models."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from sandboxpilot.schemas.common import CloudProvider
from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.utils.ids import short_id


def cluster_name_for(pool: WorkerPool, worker_id: str) -> str:
    """``sp-<pool-name>-<8 random hex chars of the worker id>``.

    Cluster names must be unique per cloud account: SkyPilot treats a launch with an
    existing name as an update of that cluster.
    """
    pool_part = pool.name[:20].rstrip("-")
    return f"sp-{pool_part}-{short_id(worker_id, 8)}"


class WorkerProvisionRequest(BaseModel):
    worker_id: str
    pool: WorkerPool
    cluster_name: str
    worker_token: str = Field(repr=False)
    sandboxpilot_version: str
    preload_images: list[str] = Field(default_factory=list)
    labels: dict[str, str] = Field(default_factory=dict)


class ProvisionedWorker(BaseModel):
    cluster_name: str
    cloud: CloudProvider | None = None
    region: str | None = None
    zone: str | None = None
    instance_type: str | None = None
    use_spot: bool = False
    hourly_cost: float | None = None
    ssh_alias: str | None = None
    endpoint: str | None = Field(
        default=None, description="Direct base URL (fake/local providers only)"
    )
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProviderWorkerStatus(BaseModel):
    exists: bool
    status: str  # UP | INIT | STOPPED | MISSING
    raw: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_up(self) -> bool:
        return self.exists and self.status == "UP"


class WorkerEstimate(BaseModel):
    hourly_cost: float | None = None
    cloud: CloudProvider | None = None
    instance_type: str | None = None
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    note: str | None = None


class CloudCheck(BaseModel):
    cloud: CloudProvider
    enabled: bool
    reason: str | None = None


class ProviderDoctorResult(BaseModel):
    provider: str
    installed: bool
    version: str | None = None
    clouds: list[CloudCheck] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    hints: list[str] = Field(default_factory=list)

    @property
    def enabled_clouds(self) -> list[CloudProvider]:
        return [c.cloud for c in self.clouds if c.enabled]


class ComputeProvider(ABC):
    name: str = "abstract"

    @abstractmethod
    async def doctor(self) -> ProviderDoctorResult: ...

    @abstractmethod
    async def provision_worker(self, request: WorkerProvisionRequest) -> ProvisionedWorker: ...

    @abstractmethod
    async def get_worker_status(self, worker: WorkerRecord) -> ProviderWorkerStatus: ...

    @abstractmethod
    async def terminate_worker(self, worker: WorkerRecord) -> None: ...

    @abstractmethod
    async def estimate_worker(self, request: WorkerProvisionRequest) -> WorkerEstimate: ...

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Release provider resources."""
