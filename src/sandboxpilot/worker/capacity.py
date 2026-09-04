"""Worker-side capacity accounting.

The worker is the final authority: even if the control plane believes a
sandbox fits, admission is decided here atomically under a lock, counting both
running SandboxPilot containers and pending reservations.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sandboxpilot.config import defaults as d
from sandboxpilot.schemas.sandbox import SandboxResources
from sandboxpilot.schemas.worker import WorkerCapacity


@dataclass
class Reservation:
    sandbox_id: str
    resources: SandboxResources
    committed: bool = False


WARM_PREFIX = "warm_"


class CapacityManager:
    def __init__(
        self,
        *,
        cpu_millis_total: int,
        memory_bytes_total: int,
        reserve_cpu_millis: int,
        reserve_memory_bytes: int,
        max_sandboxes: int | None = None,
        disk_min_free_bytes: int = d.DISK_PRESSURE_MIN_FREE_BYTES,
        disk_min_free_fraction: float = d.DISK_PRESSURE_MIN_FREE_FRACTION,
    ) -> None:
        self.cpu_millis_total = cpu_millis_total
        self.memory_bytes_total = memory_bytes_total
        self.cpu_millis_allocatable = max(0, cpu_millis_total - reserve_cpu_millis)
        self.memory_bytes_allocatable = max(0, memory_bytes_total - reserve_memory_bytes)
        self.max_sandboxes = max_sandboxes
        self.disk_min_free_bytes = disk_min_free_bytes
        self.disk_min_free_fraction = disk_min_free_fraction
        self._reservations: dict[str, Reservation] = {}
        self._lock = asyncio.Lock()
        self.disk_free_bytes: int | None = None
        self.disk_total_bytes: int | None = None
        self.draining = False

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @property
    def cpu_millis_allocated(self) -> int:
        return sum(r.resources.cpu_millis for r in self._reservations.values())

    @property
    def memory_bytes_allocated(self) -> int:
        return sum(r.resources.memory_bytes for r in self._reservations.values())

    @property
    def running_count(self) -> int:
        return sum(1 for r in self._reservations.values() if r.committed)

    @property
    def pending_count(self) -> int:
        return sum(1 for r in self._reservations.values() if not r.committed)

    def update_disk(self, free_bytes: int | None, total_bytes: int | None) -> None:
        self.disk_free_bytes = free_bytes
        self.disk_total_bytes = total_bytes

    @property
    def disk_pressure(self) -> bool:
        if self.disk_free_bytes is None:
            return False
        if self.disk_free_bytes < self.disk_min_free_bytes:
            return True
        if not self.disk_total_bytes:
            return False
        return self.disk_free_bytes / self.disk_total_bytes < self.disk_min_free_fraction

    def fits(self, resources: SandboxResources) -> bool:
        if self.max_sandboxes is not None and len(self._reservations) >= self.max_sandboxes:
            return False
        if resources.cpu_millis > self.cpu_millis_allocatable - self.cpu_millis_allocated:
            return False
        return resources.memory_bytes <= self.memory_bytes_allocatable - self.memory_bytes_allocated

    async def try_reserve(self, sandbox_id: str, resources: SandboxResources) -> bool:
        """Atomically reserve capacity; returns False when it no longer fits."""
        async with self._lock:
            if sandbox_id in self._reservations:
                return True
            if self.draining or self.disk_pressure or not self.fits(resources):
                return False
            self._reservations[sandbox_id] = Reservation(sandbox_id, resources)
            return True

    def adopt(self, sandbox_id: str, resources: SandboxResources) -> None:
        """Register an already-running sandbox discovered during reconciliation."""
        self._reservations[sandbox_id] = Reservation(sandbox_id, resources, committed=True)

    def commit(self, sandbox_id: str) -> None:
        res = self._reservations.get(sandbox_id)
        if res:
            res.committed = True

    def release(self, sandbox_id: str) -> None:
        self._reservations.pop(sandbox_id, None)

    def has(self, sandbox_id: str) -> bool:
        return sandbox_id in self._reservations

    def snapshot(self) -> WorkerCapacity:
        # Warm slots physically hold resources but are evicted the moment a real
        # sandbox needs the room, so the control plane sees them as free capacity.
        warm = [r for r in self._reservations.values() if r.sandbox_id.startswith(WARM_PREFIX)]
        warm_cpu = sum(r.resources.cpu_millis for r in warm)
        warm_mem = sum(r.resources.memory_bytes for r in warm)
        return WorkerCapacity(
            cpu_millis_total=self.cpu_millis_total,
            cpu_millis_allocatable=self.cpu_millis_allocatable,
            memory_bytes_total=self.memory_bytes_total,
            memory_bytes_allocatable=self.memory_bytes_allocatable,
            cpu_millis_allocated=self.cpu_millis_allocated - warm_cpu,
            memory_bytes_allocated=self.memory_bytes_allocated - warm_mem,
            sandboxes_running=self.running_count - len(warm),
            sandboxes_warm=len(warm),
            sandboxes_pending=self.pending_count,
            disk_free_bytes=self.disk_free_bytes,
            disk_total_bytes=self.disk_total_bytes,
            disk_pressure=self.disk_pressure,
            disk_limit_enforced=False,
        )
