"""Worker pool models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sandboxpilot.schemas.common import (
    V1_AUTO_CLOUDS,
    CloudProvider,
    CloudStrategy,
    NetworkPolicy,
)
from sandboxpilot.schemas.sandbox import SandboxSpec
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.ids import new_id
from sandboxpilot.utils.sizes import parse_bytes, parse_duration


class CloudPolicy(BaseModel):
    """Which clouds SkyPilot may choose from and how it should optimize."""

    model_config = ConfigDict(extra="forbid")

    providers: list[CloudProvider] = Field(default_factory=lambda: list(V1_AUTO_CLOUDS))
    strategy: CloudStrategy = CloudStrategy.COST
    region: str | None = None
    zone: str | None = None
    instance_type: str | None = None

    @field_validator("providers", mode="before")
    @classmethod
    def _normalize_providers(cls, value: Any) -> Any:
        if value is None or value == "auto" or value == ["auto"]:
            return list(V1_AUTO_CLOUDS)
        if isinstance(value, str):
            return [value]
        return value

    @model_validator(mode="after")
    def _region_requires_single_cloud(self) -> CloudPolicy:
        if not self.providers:
            raise ValueError("cloud policy must include at least one provider")
        if (self.region or self.zone or self.instance_type) and len(self.providers) != 1:
            raise ValueError("region, zone and instance_type require exactly one cloud provider")
        return self

    @property
    def is_auto(self) -> bool:
        return set(self.providers) == set(V1_AUTO_CLOUDS)

    def describe(self) -> str:
        if self.is_auto:
            return "auto (AWS / GCP / Azure)"
        parts = " / ".join(p.value.upper() for p in self.providers)
        if self.region:
            parts += f" ({self.region})"
        return parts


class SandboxDefaults(BaseModel):
    """Pool-level defaults applied when a create request omits values."""

    model_config = ConfigDict(extra="forbid")

    image: str = "python:3.12-slim"
    cpus: float = 1.0
    memory_bytes: int = 2 * 1024**3
    pids_limit: int = 1024
    timeout_seconds: int = 3600
    max_timeout_seconds: int = 24 * 3600
    network: NetworkPolicy = NetworkPolicy.INTERNET
    workdir: str = "/workspace"

    @field_validator("memory_bytes", mode="before")
    @classmethod
    def _memory(cls, v: Any) -> Any:
        return parse_bytes(v) if isinstance(v, str) else v

    @field_validator("timeout_seconds", "max_timeout_seconds", mode="before")
    @classmethod
    def _dur(cls, v: Any) -> Any:
        return int(parse_duration(v)) if isinstance(v, str) else v


class WorkerReserve(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpus: float = 1.0
    memory_bytes: int = 2 * 1024**3

    @field_validator("memory_bytes", mode="before")
    @classmethod
    def _memory(cls, v: Any) -> Any:
        return parse_bytes(v) if isinstance(v, str) else v


class WorkerPool(BaseModel):
    """Persisted worker pool definition."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: new_id("pool"))
    name: str = Field(min_length=1, max_length=63, pattern=r"^[a-z0-9][a-z0-9-]*$")

    cloud_policy: CloudPolicy = Field(default_factory=CloudPolicy)

    worker_cpus: int = 8
    worker_memory_bytes: int = 32 * 1024**3
    worker_disk_gb: int = 100
    worker_reserve: WorkerReserve = Field(default_factory=WorkerReserve)

    min_workers: int = 0
    max_workers: int = 4
    use_spot: bool = False
    worker_idle_ttl_seconds: int = 900
    max_sandboxes_per_worker: int | None = None
    warm_slots: int = 2

    runtime: str = "gvisor"
    default_image: str = "python:3.12-slim"
    sandbox_defaults: SandboxDefaults = Field(default_factory=SandboxDefaults)
    preload_images: list[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @field_validator("worker_memory_bytes", mode="before")
    @classmethod
    def _memory(cls, v: Any) -> Any:
        return parse_bytes(v) if isinstance(v, str) else v

    @field_validator("worker_idle_ttl_seconds", mode="before")
    @classmethod
    def _ttl(cls, v: Any) -> Any:
        return int(parse_duration(v)) if isinstance(v, str) else v

    @model_validator(mode="after")
    def _validate(self) -> WorkerPool:
        if self.min_workers < 0:
            raise ValueError("min_workers must be >= 0")
        if self.max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if self.min_workers > self.max_workers:
            raise ValueError("min_workers must be <= max_workers")
        if self.worker_cpus < 1:
            raise ValueError("worker_cpus must be >= 1")
        if self.worker_memory_bytes < 512 * 1024**2:
            raise ValueError("worker memory must be at least 512MB")
        if self.worker_idle_ttl_seconds < 0:
            raise ValueError("worker_idle_ttl_seconds must be >= 0")
        if self.runtime not in {"gvisor", "fake", "docker-unsafe"}:
            raise ValueError("runtime must be one of: gvisor, fake, docker-unsafe")
        if self.sandbox_defaults.image != self.default_image:
            # Keep the two in sync; default_image is the user-facing field.
            self.sandbox_defaults = self.sandbox_defaults.model_copy(
                update={"image": self.default_image}
            )
        return self

    @property
    def short_id(self) -> str:
        return self.name

    def warm_spec(self) -> SandboxSpec:
        """The spec warm slots are booted with: the pool's sandbox defaults."""
        d = self.sandbox_defaults
        return SandboxSpec(
            image=d.image,
            cpus=d.cpus,
            memory_bytes=d.memory_bytes,
            pids_limit=d.pids_limit,
            timeout_seconds=d.timeout_seconds,
            workdir=d.workdir,
            network=d.network,
        )


class PoolCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    cloud: CloudPolicy | None = None
    worker_cpus: int | None = None
    worker_memory: str | int | None = None
    worker_disk_gb: int | None = None
    worker_reserve: WorkerReserve | None = None
    min_workers: int | None = None
    max_workers: int | None = None
    use_spot: bool | None = None
    idle_ttl: str | int | None = None
    max_sandboxes_per_worker: int | None = None
    warm_slots: int | None = None
    runtime: str | None = None
    default_image: str | None = None
    sandbox_defaults: SandboxDefaults | None = None
    preload_images: list[str] | None = None

    def to_pool(self, base: WorkerPool | None = None) -> WorkerPool:
        data = base.model_dump() if base else {}
        data["name"] = self.name
        if self.cloud is not None:
            data["cloud_policy"] = self.cloud.model_dump()
        for src, dst in (
            ("worker_cpus", "worker_cpus"),
            ("worker_disk_gb", "worker_disk_gb"),
            ("min_workers", "min_workers"),
            ("max_workers", "max_workers"),
            ("use_spot", "use_spot"),
            ("max_sandboxes_per_worker", "max_sandboxes_per_worker"),
            ("warm_slots", "warm_slots"),
            ("runtime", "runtime"),
            ("default_image", "default_image"),
            ("preload_images", "preload_images"),
        ):
            value = getattr(self, src)
            if value is not None:
                data[dst] = value
        if self.worker_memory is not None:
            data["worker_memory_bytes"] = parse_bytes(self.worker_memory)
        if self.idle_ttl is not None:
            data["worker_idle_ttl_seconds"] = int(parse_duration(self.idle_ttl))
        if self.worker_reserve is not None:
            data["worker_reserve"] = self.worker_reserve.model_dump()
        if self.sandbox_defaults is not None:
            data["sandbox_defaults"] = self.sandbox_defaults.model_dump()
        if self.cloud is not None:
            data["cloud_policy"] = self.cloud.model_dump()
        return WorkerPool.model_validate(data)


class PoolUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_workers: int | None = None
    max_workers: int | None = None
    idle_ttl: str | int | None = None
    default_image: str | None = None
    preload_images: list[str] | None = None
    use_spot: bool | None = None
    max_sandboxes_per_worker: int | None = None
    warm_slots: int | None = None
    cloud: CloudPolicy | None = None
    sandbox_defaults: SandboxDefaults | None = None

    def apply(self, pool: WorkerPool) -> WorkerPool:
        data = pool.model_dump()
        for field in (
            "min_workers",
            "max_workers",
            "default_image",
            "preload_images",
            "use_spot",
            "max_sandboxes_per_worker",
            "warm_slots",
        ):
            value = getattr(self, field)
            if value is not None:
                data[field] = value
        if self.idle_ttl is not None:
            data["worker_idle_ttl_seconds"] = int(parse_duration(self.idle_ttl))
        if self.sandbox_defaults is not None:
            data["sandbox_defaults"] = self.sandbox_defaults.model_dump()
        if self.cloud is not None:
            data["cloud_policy"] = self.cloud.model_dump()
        data["updated_at"] = utcnow()
        return WorkerPool.model_validate(data)
