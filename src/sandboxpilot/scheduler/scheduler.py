"""SandboxScheduler: pick a worker for a request or decide to scale."""

from __future__ import annotations

from dataclasses import dataclass

from sandboxpilot.scheduler.binpack import WorkerCandidate, select_worker
from sandboxpilot.scheduler.capacity import CapacityLedger
from sandboxpilot.scheduler.scaler import ScaleDecision, decide_scale_up
from sandboxpilot.schemas.common import WorkerState
from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.schemas.sandbox import SandboxResources
from sandboxpilot.schemas.worker import WorkerRecord


@dataclass(frozen=True)
class ScheduleResult:
    worker: WorkerRecord | None
    decision: ScaleDecision

    @property
    def placed(self) -> bool:
        return self.worker is not None


class SandboxScheduler:
    def __init__(self, ledger: CapacityLedger) -> None:
        self.ledger = ledger

    def candidates(self, pool: WorkerPool, workers: list[WorkerRecord]) -> list[WorkerCandidate]:
        out: list[WorkerCandidate] = []
        for w in workers:
            if w.state != WorkerState.HEALTHY or w.draining or w.terminate_when_empty:
                continue
            cpu, mem, count = self.ledger.available(w.id)
            out.append(
                WorkerCandidate(
                    worker=w,
                    cpu_millis_available=cpu,
                    memory_bytes_available=mem,
                    sandboxes=count,
                    max_sandboxes=pool.max_sandboxes_per_worker,
                )
            )
        return out

    def schedule(
        self,
        pool: WorkerPool,
        workers: list[WorkerRecord],
        resources: SandboxResources,
        *,
        exclude: set[str] | None = None,
    ) -> ScheduleResult:
        candidates = [
            c for c in self.candidates(pool, workers) if not exclude or c.worker.id not in exclude
        ]
        worker = select_worker(candidates, resources)
        if worker is not None:
            return ScheduleResult(worker=worker, decision=ScaleDecision.NONE)
        return ScheduleResult(worker=None, decision=decide_scale_up(pool, workers))

    def fits_empty_worker(self, pool: WorkerPool, resources: SandboxResources) -> bool:
        """Would this request ever fit on a fresh worker of this pool's size?"""
        cpu = pool.worker_cpus * 1000 - int(pool.worker_reserve.cpus * 1000)
        mem = pool.worker_memory_bytes - pool.worker_reserve.memory_bytes
        return resources.cpu_millis <= cpu and resources.memory_bytes <= mem
