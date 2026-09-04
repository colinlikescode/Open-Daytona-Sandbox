"""Sandbox models."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sandboxpilot.schemas.common import ImagePullPolicy, NetworkPolicy, SandboxState
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.ids import new_id, short_id
from sandboxpilot.utils.sizes import cpus_to_millis, parse_bytes, parse_duration

DEFAULT_KEEPALIVE_COMMAND: list[str] = ["/bin/sh", "-lc", "while true; do sleep 3600; done"]
DEFAULT_WORKDIR = "/workspace"


class SandboxResources(BaseModel):
    """Normalized sandbox resource request."""

    model_config = ConfigDict(extra="forbid")

    cpu_millis: int = Field(gt=0)
    memory_bytes: int = Field(gt=0)
    pids_limit: int = Field(gt=0, default=1024)

    @property
    def cpus(self) -> float:
        return self.cpu_millis / 1000.0


class SandboxSpec(BaseModel):
    """Fully resolved sandbox specification handed to a worker."""

    model_config = ConfigDict(extra="forbid")

    image: str = Field(min_length=1)
    cpus: float = Field(gt=0)
    memory_bytes: int = Field(gt=0)
    pids_limit: int = Field(gt=0, default=1024)
    timeout_seconds: int = Field(gt=0, default=3600)
    env: dict[str, str] = Field(default_factory=dict)
    workdir: str = DEFAULT_WORKDIR
    network: NetworkPolicy = NetworkPolicy.INTERNET
    user: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    read_only_root: bool = False
    tmpfs_size_bytes: int | None = None
    keepalive_command: list[str] = Field(default_factory=lambda: list(DEFAULT_KEEPALIVE_COMMAND))
    image_pull_policy: ImagePullPolicy = ImagePullPolicy.IF_NOT_PRESENT

    @field_validator("workdir")
    @classmethod
    def _abs_workdir(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError("workdir must be an absolute path")
        return v

    @field_validator("env")
    @classmethod
    def _env_keys(cls, v: dict[str, str]) -> dict[str, str]:
        for key in v:
            if not key or "=" in key or "\0" in key:
                raise ValueError(f"invalid environment variable name: {key!r}")
        return v

    @property
    def resources(self) -> SandboxResources:
        return SandboxResources(
            cpu_millis=cpus_to_millis(self.cpus),
            memory_bytes=self.memory_bytes,
            pids_limit=self.pids_limit,
        )


class SandboxCreateRequest(BaseModel):
    """User-facing creation request; unspecified fields fall back to template/pool defaults."""

    model_config = ConfigDict(extra="forbid")

    pool: str | None = None
    template: str | None = None
    image: str | None = None
    cpus: float | None = None
    memory: str | int | None = None
    pids_limit: int | None = None
    timeout: str | int | None = None
    env: dict[str, str] = Field(default_factory=dict)
    workdir: str | None = None
    network: NetworkPolicy | None = None
    user: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    read_only_root: bool | None = None
    tmpfs: str | int | None = None
    keepalive_command: list[str] | None = None
    image_pull_policy: ImagePullPolicy | None = None
    create_timeout: float | None = Field(default=None, description="Seconds to wait for RUNNING")
    wait: bool = True

    @field_validator("cpus")
    @classmethod
    def _cpus(cls, v: float | None) -> float | None:
        if v is not None:
            cpus_to_millis(v)
        return v

    @model_validator(mode="after")
    def _normalize(self) -> SandboxCreateRequest:
        if self.memory is not None:
            parse_bytes(self.memory)
        if self.timeout is not None and parse_duration(self.timeout) <= 0:
            raise ValueError("timeout must be positive")
        if self.tmpfs is not None:
            parse_bytes(self.tmpfs)
        return self

    def memory_bytes(self) -> int | None:
        return parse_bytes(self.memory) if self.memory is not None else None

    def timeout_seconds(self) -> int | None:
        return int(parse_duration(self.timeout)) if self.timeout is not None else None

    def tmpfs_bytes(self) -> int | None:
        return parse_bytes(self.tmpfs) if self.tmpfs is not None else None


class SandboxRecord(BaseModel):
    """Persisted control-plane sandbox state."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: new_id("sbx"))
    pool_id: str
    pool_name: str
    worker_id: str | None = None
    state: SandboxState = SandboxState.PENDING
    spec: SandboxSpec
    runtime_id: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    expires_at: datetime | None = None
    error: str | None = None
    idempotency_key: str | None = None
    metrics: dict[str, float] = Field(default_factory=dict)

    @property
    def short_id(self) -> str:
        return short_id(self.id, 8)

    def to_info(self, *, worker_state: str | None = None) -> SandboxInfo:
        return SandboxInfo(
            id=self.id,
            short_id=self.short_id,
            pool=self.pool_name,
            worker_id=self.worker_id,
            state=self.state,
            image=self.spec.image,
            cpus=self.spec.cpus,
            memory_bytes=self.spec.memory_bytes,
            pids_limit=self.spec.pids_limit,
            network=self.spec.network,
            workdir=self.spec.workdir,
            labels=self.spec.labels,
            metadata=self.spec.metadata,
            created_at=self.created_at,
            started_at=self.started_at,
            ended_at=self.ended_at,
            expires_at=self.expires_at,
            error=self.error,
            metrics=self.metrics,
            env_keys=sorted(self.spec.env),
            worker_state=worker_state,
        )


class SandboxInfo(BaseModel):
    """Public sandbox representation. Environment values are never exposed."""

    id: str
    short_id: str
    pool: str
    worker_id: str | None
    state: SandboxState
    image: str
    cpus: float
    memory_bytes: int
    pids_limit: int
    network: NetworkPolicy
    workdir: str
    labels: dict[str, str]
    metadata: dict[str, str]
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    expires_at: datetime | None
    error: str | None
    metrics: dict[str, float] = Field(default_factory=dict)
    env_keys: list[str] = Field(default_factory=list)
    worker_state: str | None = None

    def model_dump_public(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class SandboxTimeoutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timeout: str | int = Field(description="New lifetime from now (seconds or duration string)")

    def seconds(self) -> int:
        return int(parse_duration(self.timeout))


class RuntimeSandboxSpec(BaseModel):
    """What the worker hands to the runtime: spec + identity + expiration."""

    model_config = ConfigDict(extra="forbid")

    sandbox_id: str
    pool_id: str
    worker_id: str
    spec: SandboxSpec
    expires_at: datetime
    created_at: datetime = Field(default_factory=utcnow)


class RuntimeSandbox(BaseModel):
    """Runtime-level handle of a created sandbox."""

    sandbox_id: str
    runtime_id: str
    image_digest: str | None = None
    created_at: datetime = Field(default_factory=utcnow)


class RuntimeSandboxState(BaseModel):
    sandbox_id: str
    runtime_id: str | None
    exists: bool
    running: bool
    exit_code: int | None = None
    oom_killed: bool = False
    expires_at: datetime | None = None
    resources: SandboxResources | None = None
    runtime_name: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    error: str | None = None
