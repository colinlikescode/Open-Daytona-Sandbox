"""Control-plane capacity ledger.

The ledger combines the last capacity snapshot reported by each worker with
local in-flight reservations (creates the worker has not yet acknowledged), so
two concurrent creations never both count the same free capacity. The worker
remains the final authority: it can still reject with CAPACITY_CHANGED.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sandboxpilot.schemas.sandbox import SandboxResources
from sandboxpilot.schemas.worker import WorkerCapacity


@dataclass
class _WorkerLedger:
    capacity: WorkerCapacity = field(default_factory=WorkerCapacity)
    pending: dict[str, SandboxResources] = field(default_factory=dict)
    local_allocated: dict[str, SandboxResources] = field(default_factory=dict)

    @property
    def cpu_available(self) -> int:
        used = self.capacity.cpu_millis_allocated + sum(r.cpu_millis for r in self.pending.values())
        used += sum(r.cpu_millis for sid, r in self.local_allocated.items())
        return self.capacity.cpu_millis_allocatable - used

    @property
    def memory_available(self) -> int:
        used = self.capacity.memory_bytes_allocated + sum(
            r.memory_bytes for r in self.pending.values()
        )
        used += sum(r.memory_bytes for r in self.local_allocated.values())
        return self.capacity.memory_bytes_allocatable - used

    @property
    def sandbox_count(self) -> int:
        return (
            self.capacity.sandboxes_running
            + self.capacity.sandboxes_pending
            + len(self.pending)
            + len(self.local_allocated)
        )


class CapacityLedger:
    def __init__(self) -> None:
        self._workers: dict[str, _WorkerLedger] = {}

    def _ledger(self, worker_id: str) -> _WorkerLedger:
        return self._workers.setdefault(worker_id, _WorkerLedger())

    def update(self, worker_id: str, capacity: WorkerCapacity) -> None:
        """Replace the worker-reported snapshot; local deltas acknowledged by the worker are dropped."""
        ledger = self._ledger(worker_id)
        ledger.capacity = capacity
        # Anything the worker now reports as allocated is included in the snapshot,
        # so local post-create adjustments are no longer needed.
        ledger.local_allocated.clear()

    def reserve(self, worker_id: str, sandbox_id: str, resources: SandboxResources) -> None:
        self._ledger(worker_id).pending[sandbox_id] = resources

    def confirm(self, worker_id: str, sandbox_id: str) -> None:
        """Worker accepted the sandbox: keep counting it until the next snapshot arrives."""
        ledger = self._ledger(worker_id)
        res = ledger.pending.pop(sandbox_id, None)
        if res is not None:
            ledger.local_allocated[sandbox_id] = res

    def release(self, worker_id: str, sandbox_id: str) -> None:
        ledger = self._ledger(worker_id)
        ledger.pending.pop(sandbox_id, None)
        ledger.local_allocated.pop(sandbox_id, None)

    def forget(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)

    def available(self, worker_id: str) -> tuple[int, int, int]:
        ledger = self._ledger(worker_id)
        return ledger.cpu_available, ledger.memory_available, ledger.sandbox_count

    def pending_count(self, worker_id: str) -> int:
        return len(self._ledger(worker_id).pending)

    def snapshot(self, worker_id: str) -> WorkerCapacity:
        ledger = self._ledger(worker_id)
        cap = ledger.capacity.model_copy()
        cap.cpu_millis_allocated = cap.cpu_millis_allocatable - ledger.cpu_available
        cap.memory_bytes_allocated = cap.memory_bytes_allocatable - ledger.memory_available
        return cap
