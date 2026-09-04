"""Shared enums and state machines."""

from __future__ import annotations

from enum import StrEnum

from sandboxpilot.errors import StateError


class CloudProvider(StrEnum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


V1_AUTO_CLOUDS: tuple[CloudProvider, ...] = (
    CloudProvider.AWS,
    CloudProvider.GCP,
    CloudProvider.AZURE,
)


class CloudStrategy(StrEnum):
    COST = "cost"
    TIME = "time"


class NetworkPolicy(StrEnum):
    INTERNET = "internet"
    NONE = "none"


class ImagePullPolicy(StrEnum):
    IF_NOT_PRESENT = "if-not-present"
    ALWAYS = "always"
    NEVER = "never"


class SandboxState(StrEnum):
    PENDING = "PENDING"
    WAITING_FOR_CAPACITY = "WAITING_FOR_CAPACITY"
    PROVISIONING_WORKER = "PROVISIONING_WORKER"
    CREATING = "CREATING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    LOST = "LOST"
    EXPIRED = "EXPIRED"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_SANDBOX_STATES

    @property
    def is_active(self) -> bool:
        return self not in _TERMINAL_SANDBOX_STATES


_TERMINAL_SANDBOX_STATES = frozenset(
    {
        SandboxState.STOPPED,
        SandboxState.FAILED,
        SandboxState.LOST,
        SandboxState.EXPIRED,
    }
)

_SANDBOX_TRANSITIONS: dict[SandboxState, frozenset[SandboxState]] = {
    SandboxState.PENDING: frozenset(
        {
            SandboxState.WAITING_FOR_CAPACITY,
            SandboxState.PROVISIONING_WORKER,
            SandboxState.CREATING,
            SandboxState.FAILED,
        }
    ),
    SandboxState.WAITING_FOR_CAPACITY: frozenset(
        {
            SandboxState.PROVISIONING_WORKER,
            SandboxState.CREATING,
            SandboxState.FAILED,
        }
    ),
    SandboxState.PROVISIONING_WORKER: frozenset(
        {SandboxState.WAITING_FOR_CAPACITY, SandboxState.CREATING, SandboxState.FAILED}
    ),
    SandboxState.CREATING: frozenset(
        {
            SandboxState.RUNNING,
            SandboxState.FAILED,
            SandboxState.LOST,
            SandboxState.STOPPING,
            SandboxState.WAITING_FOR_CAPACITY,
        }
    ),
    SandboxState.RUNNING: frozenset(
        {
            SandboxState.STOPPING,
            SandboxState.STOPPED,
            SandboxState.LOST,
            SandboxState.EXPIRED,
            SandboxState.FAILED,
        }
    ),
    SandboxState.STOPPING: frozenset(
        {SandboxState.STOPPED, SandboxState.LOST, SandboxState.FAILED}
    ),
    SandboxState.STOPPED: frozenset(),
    SandboxState.FAILED: frozenset(),
    SandboxState.LOST: frozenset(),
    SandboxState.EXPIRED: frozenset(),
}


def assert_sandbox_transition(current: SandboxState, new: SandboxState) -> None:
    if new == current:
        return
    if new not in _SANDBOX_TRANSITIONS[current]:
        raise StateError(f"Invalid sandbox state transition {current.value} -> {new.value}")


class WorkerState(StrEnum):
    PROVISIONING = "PROVISIONING"
    BOOTSTRAPPING = "BOOTSTRAPPING"
    CONNECTING = "CONNECTING"
    HEALTHY = "HEALTHY"
    DRAINING = "DRAINING"
    UNHEALTHY = "UNHEALTHY"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    LOST = "LOST"

    @property
    def is_terminal(self) -> bool:
        return self in {WorkerState.TERMINATED, WorkerState.LOST}

    @property
    def is_pending(self) -> bool:
        """Worker is being brought up and will (hopefully) contribute capacity."""
        return self in {WorkerState.PROVISIONING, WorkerState.BOOTSTRAPPING, WorkerState.CONNECTING}

    @property
    def counts_toward_max(self) -> bool:
        return self not in {WorkerState.TERMINATED, WorkerState.LOST}


_WORKER_TRANSITIONS: dict[WorkerState, frozenset[WorkerState]] = {
    WorkerState.PROVISIONING: frozenset(
        {
            WorkerState.BOOTSTRAPPING,
            WorkerState.CONNECTING,
            WorkerState.TERMINATING,
            WorkerState.LOST,
            WorkerState.UNHEALTHY,
        }
    ),
    WorkerState.BOOTSTRAPPING: frozenset(
        {WorkerState.CONNECTING, WorkerState.TERMINATING, WorkerState.LOST, WorkerState.UNHEALTHY}
    ),
    WorkerState.CONNECTING: frozenset(
        {WorkerState.HEALTHY, WorkerState.UNHEALTHY, WorkerState.TERMINATING, WorkerState.LOST}
    ),
    WorkerState.HEALTHY: frozenset(
        {
            WorkerState.DRAINING,
            WorkerState.UNHEALTHY,
            WorkerState.TERMINATING,
            WorkerState.LOST,
            WorkerState.CONNECTING,
        }
    ),
    WorkerState.DRAINING: frozenset(
        {WorkerState.HEALTHY, WorkerState.UNHEALTHY, WorkerState.TERMINATING, WorkerState.LOST}
    ),
    WorkerState.UNHEALTHY: frozenset(
        {
            WorkerState.HEALTHY,
            WorkerState.DRAINING,
            WorkerState.CONNECTING,
            WorkerState.TERMINATING,
            WorkerState.LOST,
        }
    ),
    WorkerState.TERMINATING: frozenset({WorkerState.TERMINATED, WorkerState.LOST}),
    WorkerState.TERMINATED: frozenset(),
    WorkerState.LOST: frozenset({WorkerState.TERMINATING, WorkerState.TERMINATED}),
}


def assert_worker_transition(current: WorkerState, new: WorkerState) -> None:
    if new == current:
        return
    if new not in _WORKER_TRANSITIONS[current]:
        raise StateError(f"Invalid worker state transition {current.value} -> {new.value}")


class OperationStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
        }
