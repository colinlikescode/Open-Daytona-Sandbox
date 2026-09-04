"""Best-fit binpacking.

Pack sandboxes onto already-busy workers so idle ones can be terminated.
Candidates that cannot fit any dimension are rejected. Among the rest, the
worker with the *least* remaining memory after placement wins; CPU is the
secondary key; creation time and id make the order deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass

from sandboxpilot.schemas.sandbox import SandboxResources
from sandboxpilot.schemas.worker import WorkerRecord


@dataclass(frozen=True)
class WorkerCandidate:
    worker: WorkerRecord
    cpu_millis_available: int
    memory_bytes_available: int
    sandboxes: int
    max_sandboxes: int | None = None

    def fits(self, resources: SandboxResources) -> bool:
        if self.max_sandboxes is not None and self.sandboxes >= self.max_sandboxes:
            return False
        return (
            resources.cpu_millis <= self.cpu_millis_available
            and resources.memory_bytes <= self.memory_bytes_available
        )

    def score(self, resources: SandboxResources) -> tuple[int, int, float, str]:
        return (
            self.memory_bytes_available - resources.memory_bytes,
            self.cpu_millis_available - resources.cpu_millis,
            self.worker.created_at.timestamp(),
            self.worker.id,
        )


def select_worker(
    candidates: list[WorkerCandidate], resources: SandboxResources
) -> WorkerRecord | None:
    fitting = [c for c in candidates if c.fits(resources)]
    if not fitting:
        return None
    return min(fitting, key=lambda c: c.score(resources)).worker
