"""Scale-up / scale-down decisions (pure functions; the control plane executes them)."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum

from sandboxpilot.schemas.common import WorkerState
from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.schemas.worker import WorkerRecord


class ScaleDecision(StrEnum):
    NONE = "none"
    SCALE_UP = "scale_up"
    WAIT = "wait"  # at max workers: queue until capacity frees up


def counted_workers(workers: list[WorkerRecord]) -> list[WorkerRecord]:
    """Workers that count toward ``max_workers`` (anything not terminated/lost)."""
    return [w for w in workers if w.state.counts_toward_max]


def decide_scale_up(pool: WorkerPool, workers: list[WorkerRecord]) -> ScaleDecision:
    """Called when no healthy worker can fit a request."""
    active = counted_workers(workers)
    pending = [w for w in active if w.state.is_pending]
    if pending:
        # A worker is already on its way; share it instead of provisioning another.
        return ScaleDecision.WAIT
    if len(active) < pool.max_workers:
        return ScaleDecision.SCALE_UP
    return ScaleDecision.WAIT


def replacements_needed(pool: WorkerPool, workers: list[WorkerRecord]) -> int:
    """How many workers must be provisioned to honour ``min_workers``."""
    active = [
        w
        for w in counted_workers(workers)
        if w.state != WorkerState.TERMINATING and not w.terminate_when_empty
    ]
    return max(0, pool.min_workers - len(active))


def workers_to_scale_down(
    pool: WorkerPool,
    workers: list[WorkerRecord],
    active_sandboxes: dict[str, int],
    pending_reservations: dict[str, int],
    now: datetime,
) -> list[WorkerRecord]:
    """Idle workers eligible for termination, oldest-idle first, honouring ``min_workers``."""
    active = counted_workers(workers)
    healthy_count = len(
        [
            w
            for w in active
            if w.state in {WorkerState.HEALTHY, WorkerState.DRAINING} or w.state.is_pending
        ]
    )
    candidates: list[WorkerRecord] = []
    for w in active:
        if w.state not in {WorkerState.HEALTHY, WorkerState.DRAINING, WorkerState.UNHEALTHY}:
            continue
        if active_sandboxes.get(w.id, 0) or pending_reservations.get(w.id, 0):
            continue
        if w.terminate_when_empty:
            candidates.append(w)
            continue
        idle_since = w.idle_since or w.healthy_at or w.created_at
        if now - idle_since >= timedelta(seconds=pool.worker_idle_ttl_seconds):
            candidates.append(w)
    candidates.sort(key=lambda w: ((w.idle_since or w.created_at), w.id))
    result: list[WorkerRecord] = []
    remaining = healthy_count
    for w in candidates:
        if w.terminate_when_empty or remaining - 1 >= pool.min_workers:
            result.append(w)
            remaining -= 1
    return result
