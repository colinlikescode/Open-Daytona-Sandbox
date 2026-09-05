"""Configuration models (validated with Pydantic)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sandboxpilot.config import defaults as d
from sandboxpilot.schemas.common import CloudProvider, CloudStrategy, NetworkPolicy
from sandboxpilot.schemas.pool import CloudPolicy, SandboxDefaults, WorkerPool, WorkerReserve
from sandboxpilot.utils.sizes import parse_bytes, parse_duration


class ApiConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = d.DEFAULT_API_HOST
    port: int = d.DEFAULT_API_PORT
    token: str | None = Field(default=None, repr=False)
    external_url: str | None = Field(
        default=None, description="Base URL clients should use for proxy URLs when remote"
    )

    @property
    def is_loopback(self) -> bool:
        return self.host in {"127.0.0.1", "localhost", "::1"}

    @property
    def url(self) -> str:
        if self.external_url:
            return self.external_url.rstrip("/")
        host = "127.0.0.1" if self.host in {"0.0.0.0", "::"} else self.host
        return f"http://{host}:{self.port}"


class DefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pool: str = d.DEFAULT_POOL_NAME
    # None defers to the pool's ``sandbox.timeout``; a value here (config file or
    # SANDBOXPILOT_SANDBOX_TIMEOUT) overrides every pool's default, per the documented precedence.
    sandbox_timeout: str | int | None = None
    create_timeout: float = d.DEFAULT_CREATE_TIMEOUT_SECONDS

    @field_validator("sandbox_timeout")
    @classmethod
    def _timeout(cls, v: str | int | None) -> str | int | None:
        if v is not None and parse_duration(v) <= 0:
            raise ValueError("sandbox_timeout must be positive")
        return v

    @property
    def sandbox_timeout_seconds(self) -> int | None:
        return (
            int(parse_duration(self.sandbox_timeout)) if self.sandbox_timeout is not None else None
        )


class LimitsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_command_output_bytes: int = d.DEFAULT_MAX_COMMAND_OUTPUT_BYTES
    max_upload_size: int = d.DEFAULT_MAX_UPLOAD_BYTES
    max_download_size: int = d.DEFAULT_MAX_DOWNLOAD_BYTES
    proxy_url_ttl_seconds: int = d.DEFAULT_PROXY_URL_TTL_SECONDS
    idempotency_ttl_seconds: int = d.DEFAULT_IDEMPOTENCY_TTL_SECONDS

    @field_validator(
        "max_command_output_bytes", "max_upload_size", "max_download_size", mode="before"
    )
    @classmethod
    def _sizes(cls, v: Any) -> Any:
        return parse_bytes(v) if isinstance(v, str) else v


class ReconcileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval_seconds: float = d.DEFAULT_RECONCILE_INTERVAL_SECONDS
    health_failures_before_lost: int = d.DEFAULT_HEALTH_FAILURES_BEFORE_LOST
    worker_provision_timeout_seconds: float = d.DEFAULT_WORKER_PROVISION_TIMEOUT_SECONDS
    worker_health_timeout_seconds: float = 5.0


class PoolCloudConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    providers: list[CloudProvider] | str = Field(default="auto")
    strategy: CloudStrategy = CloudStrategy.COST
    region: str | None = None
    zone: str | None = None
    instance_type: str | None = None

    def to_policy(self) -> CloudPolicy:
        return CloudPolicy(
            providers=self.providers,  # type: ignore[arg-type]
            strategy=self.strategy,
            region=self.region,
            zone=self.zone,
            instance_type=self.instance_type,
        )


class PoolWorkersConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpus: int = d.DEFAULT_WORKER_CPUS
    memory: str | int = d.DEFAULT_WORKER_MEMORY
    disk: str | int = d.DEFAULT_WORKER_DISK_GB
    spot: bool = False
    reserve: WorkerReserve = Field(default_factory=WorkerReserve)
    max_sandboxes: int | None = None
    warm_slots: int = 2  # pre-booted sandboxes per worker, claimed in ~20ms instead of a cold boot

    @property
    def memory_bytes(self) -> int:
        return parse_bytes(self.memory)

    @property
    def disk_gb(self) -> int:
        if isinstance(self.disk, int):
            return self.disk
        return max(1, parse_bytes(self.disk) // 1000**3)


class PoolScalingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_workers: int = d.DEFAULT_MIN_WORKERS
    max_workers: int = d.DEFAULT_MAX_WORKERS
    idle_ttl: str | int = d.DEFAULT_IDLE_TTL_SECONDS

    @property
    def idle_ttl_seconds(self) -> int:
        return int(parse_duration(self.idle_ttl))


class PoolRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = "gvisor"


class PoolSandboxConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image: str = d.DEFAULT_IMAGE
    cpus: float = 1.0
    memory: str | int = "2GB"
    pids_limit: int = 1024
    network: NetworkPolicy = NetworkPolicy.INTERNET
    timeout: str | int = d.DEFAULT_SANDBOX_TIMEOUT_SECONDS
    max_timeout: str | int = d.DEFAULT_MAX_SANDBOX_TIMEOUT_SECONDS
    workdir: str = "/workspace"


class PoolImagesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preload: list[str] = Field(default_factory=list)


class PoolConfig(BaseModel):
    """A pool as written in ``config.yaml``."""

    model_config = ConfigDict(extra="forbid")

    cloud: PoolCloudConfig = Field(default_factory=PoolCloudConfig)
    workers: PoolWorkersConfig = Field(default_factory=PoolWorkersConfig)
    scaling: PoolScalingConfig = Field(default_factory=PoolScalingConfig)
    runtime: PoolRuntimeConfig = Field(default_factory=PoolRuntimeConfig)
    sandbox: PoolSandboxConfig = Field(default_factory=PoolSandboxConfig)
    images: PoolImagesConfig = Field(default_factory=PoolImagesConfig)

    def to_pool(self, name: str, existing_id: str | None = None) -> WorkerPool:
        data: dict[str, Any] = {
            "name": name,
            "cloud_policy": self.cloud.to_policy().model_dump(),
            "worker_cpus": self.workers.cpus,
            "worker_memory_bytes": self.workers.memory_bytes,
            "worker_disk_gb": self.workers.disk_gb,
            "worker_reserve": self.workers.reserve.model_dump(),
            "min_workers": self.scaling.min_workers,
            "max_workers": self.scaling.max_workers,
            "use_spot": self.workers.spot,
            "worker_idle_ttl_seconds": self.scaling.idle_ttl_seconds,
            "max_sandboxes_per_worker": self.workers.max_sandboxes,
            "warm_slots": self.workers.warm_slots,
            "runtime": self.runtime.type,
            "default_image": self.sandbox.image,
            "sandbox_defaults": SandboxDefaults(
                image=self.sandbox.image,
                cpus=self.sandbox.cpus,
                memory_bytes=parse_bytes(self.sandbox.memory),
                pids_limit=self.sandbox.pids_limit,
                timeout_seconds=int(parse_duration(self.sandbox.timeout)),
                max_timeout_seconds=int(parse_duration(self.sandbox.max_timeout)),
                network=self.sandbox.network,
                workdir=self.sandbox.workdir,
            ).model_dump(),
            "preload_images": list(self.images.preload),
        }
        if existing_id:
            data["id"] = existing_id
        return WorkerPool.model_validate(data)


class ProviderConfig(BaseModel):
    """Which compute provider the control plane uses."""

    model_config = ConfigDict(extra="forbid")

    type: str = "skypilot"  # skypilot | fake
    fake: dict[str, Any] = Field(default_factory=dict)


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: str = "INFO"
    format: str = "text"  # text | json


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api: ApiConfig = Field(default_factory=ApiConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    reconcile: ReconcileConfig = Field(default_factory=ReconcileConfig)
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    pools: dict[str, PoolConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self) -> Config:
        if not self.api.is_loopback and not self.api.token:
            raise ValueError(
                "Binding the API to a non-loopback address requires an API token. "
                "Set SANDBOXPILOT_API_TOKEN or api.token in the configuration."
            )
        return self

    def pool_models(self) -> list[WorkerPool]:
        return [cfg.to_pool(name) for name, cfg in self.pools.items()]
