"""Runtime abstraction.

The worker talks to sandboxes exclusively through this interface, which keeps
the scheduler, control plane, APIs and SDKs independent of the isolation
technology. ``GVisorDockerRuntime`` implements it today; a Firecracker runtime
can implement it later without touching anything above the worker.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Literal

from pydantic import BaseModel, Field

from sandboxpilot.schemas.sandbox import (
    RuntimeSandbox,
    RuntimeSandboxSpec,
    RuntimeSandboxState,
)

StreamName = Literal["stdout", "stderr"]

MANAGED_LABEL = "sandboxpilot.managed"
LABEL_SANDBOX_ID = "sandboxpilot.sandbox_id"
LABEL_POOL_ID = "sandboxpilot.pool_id"
LABEL_WORKER_ID = "sandboxpilot.worker_id"
LABEL_CREATED_AT = "sandboxpilot.created_at"
LABEL_EXPIRES_AT = "sandboxpilot.expires_at"
LABEL_VERSION = "sandboxpilot.version"
LABEL_CPU_MILLIS = "sandboxpilot.cpu_millis"
LABEL_MEMORY_BYTES = "sandboxpilot.memory_bytes"
LABEL_PIDS_LIMIT = "sandboxpilot.pids_limit"


class RuntimeDoctorResult(BaseModel):
    ok: bool
    runtime: str
    docker_version: str | None = None
    runsc_version: str | None = None
    checks: dict[str, bool] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ImageInfo(BaseModel):
    reference: str
    digest: str | None = None
    pulled: bool = False
    pull_seconds: float | None = None


class HostResources(BaseModel):
    cpu_millis: int
    memory_bytes: int
    disk_total_bytes: int | None = None
    disk_free_bytes: int | None = None
    external_containers: int = 0


class ExecHandle(ABC):
    """A running process inside a sandbox."""

    exec_id: str

    @abstractmethod
    def stream(self) -> AsyncIterator[tuple[StreamName, bytes]]:
        """Yield output chunks in arrival order until the process exits."""

    @abstractmethod
    async def wait(self) -> int:
        """Wait for process exit and return the exit code."""

    @abstractmethod
    async def kill(self) -> None:
        """Terminate the process (and its children) if still running."""


class SandboxRuntime(ABC):
    name: str = "abstract"

    @abstractmethod
    async def doctor(self) -> RuntimeDoctorResult: ...

    @abstractmethod
    async def host_resources(self) -> HostResources: ...

    @abstractmethod
    async def ensure_image(self, reference: str, policy: str) -> ImageInfo: ...

    @abstractmethod
    async def list_images(self) -> list[ImageInfo]: ...

    @abstractmethod
    async def create(self, spec: RuntimeSandboxSpec) -> RuntimeSandbox: ...

    @abstractmethod
    async def start(self, sandbox_id: str) -> None: ...

    async def adopt(self, old_id: str, spec: RuntimeSandboxSpec) -> RuntimeSandbox | None:
        """Turn an already-running sandbox ``old_id`` into ``spec.sandbox_id``.

        Used by warm slots: the worker pre-boots sandboxes with the pool's default
        spec and, when a matching create request arrives, hands one over instead
        of paying the cold boot. Resource limits are adjusted to ``spec``. Returns
        None when the runtime cannot do this, in which case the caller falls back
        to a cold create.
        """
        return None

    def remember(self, sandbox_id: str, runtime_id: str) -> None:  # noqa: B027 - optional hook
        """Seed the sandbox-id -> runtime-id mapping from persisted worker state.

        Needed so adopted sandboxes (whose immutable labels still carry the warm
        slot id) resolve to the right id after a worker restart.
        """

    @abstractmethod
    async def stop(self, sandbox_id: str, timeout: float = 5.0) -> None: ...

    @abstractmethod
    async def remove(self, sandbox_id: str) -> None: ...

    @abstractmethod
    async def inspect(self, sandbox_id: str) -> RuntimeSandboxState: ...

    @abstractmethod
    async def list_managed(self) -> list[RuntimeSandboxState]:
        """All sandboxes carrying the ``sandboxpilot.managed=true`` label."""

    @abstractmethod
    async def exec(
        self,
        sandbox_id: str,
        argv: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        user: str | None = None,
        stdin: bytes | None = None,
    ) -> ExecHandle: ...

    @abstractmethod
    async def upload(self, sandbox_id: str, path: str, archive: AsyncIterator[bytes]) -> None:
        """Extract a tar archive at ``path`` inside the sandbox."""

    @abstractmethod
    def download(self, sandbox_id: str, path: str) -> AsyncIterator[bytes]:
        """Stream a tar archive of ``path`` from the sandbox."""

    @abstractmethod
    async def endpoint(self, sandbox_id: str, port: int) -> tuple[str, int]:
        """Return the (host, port) the worker can connect to for ``port`` inside the sandbox."""

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Release runtime resources."""
