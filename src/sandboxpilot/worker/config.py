"""Worker daemon configuration (environment-driven; written by bootstrap to /etc/sandboxpilot/worker.env)."""

from __future__ import annotations

import base64
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from sandboxpilot.config import defaults as d
from sandboxpilot.errors import ConfigurationError
from sandboxpilot.schemas.sandbox import SandboxSpec
from sandboxpilot.utils.sizes import parse_bytes

UNSAFE_RUNTIME_FLAG = "SANDBOXPILOT_DEV_UNSAFE_RUNTIME"


class WorkerConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SANDBOXPILOT_WORKER_", extra="ignore")

    id: str = Field(default="local-worker")
    pool_id: str = Field(default="local-pool")
    token: str = Field(default="", repr=False)
    host: str = d.WORKER_HOST
    port: int = d.WORKER_PORT
    runtime: str = "gvisor"  # gvisor | fake | docker-unsafe
    reserve_cpus: float = d.DEFAULT_WORKER_RESERVE_CPUS
    reserve_memory: str = d.DEFAULT_WORKER_RESERVE_MEMORY
    max_sandboxes: int | None = None
    state_dir: Path = Path("/var/lib/sandboxpilot")
    network_name: str = d.SANDBOX_NETWORK_NAME
    network_subnet: str = d.SANDBOX_NETWORK_SUBNET
    # Resolvers written into every sandbox's /etc/resolv.conf. gVisor cannot reach
    # Docker's embedded DNS, and the cloud metadata resolver is firewalled on purpose.
    sandbox_dns: str = "8.8.8.8,1.1.1.1"
    preload_images: str = ""
    # Pre-booted sandboxes kept ready per worker. A create request whose spec is
    # compatible claims one instead of paying the ~100-300 ms gVisor cold boot.
    warm_slots: int = 0
    warm_spec: str = ""  # SandboxSpec for warm slots: JSON, or base64-encoded JSON
    max_command_output_bytes: int = d.DEFAULT_MAX_COMMAND_OUTPUT_BYTES
    max_upload_size: int = d.DEFAULT_MAX_UPLOAD_BYTES
    max_download_size: int = d.DEFAULT_MAX_DOWNLOAD_BYTES
    reaper_interval_seconds: float = 5.0
    disk_min_free_bytes: int = d.DISK_PRESSURE_MIN_FREE_BYTES
    disk_min_free_fraction: float = d.DISK_PRESSURE_MIN_FREE_FRACTION
    docker_host: str | None = None
    log_level: str = "INFO"
    log_format: str = "text"

    @field_validator(
        "max_command_output_bytes", "max_upload_size", "max_download_size", mode="before"
    )
    @classmethod
    def _sizes(cls, v: object) -> object:
        return parse_bytes(v) if isinstance(v, str) else v

    @property
    def reserve_memory_bytes(self) -> int:
        return parse_bytes(self.reserve_memory)

    @property
    def reserve_cpu_millis(self) -> int:
        return int(self.reserve_cpus * 1000)

    @property
    def warm_spec_model(self) -> SandboxSpec | None:
        if not self.warm_spec:
            return None
        raw = self.warm_spec.strip()
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode()
        return SandboxSpec.model_validate_json(raw)

    @property
    def preload_image_list(self) -> list[str]:
        return [i.strip() for i in self.preload_images.split(",") if i.strip()]

    @property
    def sandbox_dns_list(self) -> list[str]:
        return [s.strip() for s in self.sandbox_dns.split(",") if s.strip()]

    def validate_for_serving(self, env: dict[str, str]) -> None:
        if not self.token or len(self.token) < 32:
            raise ConfigurationError(
                "SANDBOXPILOT_WORKER_TOKEN must be set to a random token of at least 32 characters"
            )
        if self.runtime not in {"gvisor", "fake", "docker-unsafe"}:
            raise ConfigurationError(f"Unsupported worker runtime {self.runtime!r}")
        if self.runtime == "docker-unsafe" and env.get(UNSAFE_RUNTIME_FLAG) != "1":
            raise ConfigurationError(
                "The docker-unsafe runtime (plain runc) is for development only. "
                f"Set {UNSAFE_RUNTIME_FLAG}=1 to acknowledge that it provides no gVisor isolation."
            )
        if (
            self.host not in {"127.0.0.1", "localhost", "::1"}
            and env.get("SANDBOXPILOT_WORKER_ALLOW_NON_LOOPBACK") != "1"
        ):
            raise ConfigurationError(
                "The worker API must bind to loopback; the control plane reaches it through an SSH tunnel."
            )
