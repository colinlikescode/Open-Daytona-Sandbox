from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sandboxpilot.scheduler.binpack import WorkerCandidate, select_worker
from sandboxpilot.scheduler.scaler import (
    ScaleDecision,
    decide_scale_up,
    replacements_needed,
    workers_to_scale_down,
)
from sandboxpilot.schemas.common import WorkerState
from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.schemas.sandbox import SandboxResources
from sandboxpilot.schemas.worker import WorkerRecord

GB = 1024**3


def worker(name: str, state: WorkerState = WorkerState.HEALTHY, **kw: object) -> WorkerRecord:
    return WorkerRecord(
        id=f"wrk_{name}",
        pool_id="pool_x",
        pool_name="x",
        state=state,
        provider_cluster=f"sp-{name}",
        token="t" * 32,
        **kw,  # type: ignore[arg-type]
    )


def res(cpus: float = 1, gb: float = 1) -> SandboxResources:
    return SandboxResources(cpu_millis=int(cpus * 1000), memory_bytes=int(gb * GB), pids_limit=512)


def test_binpack_prefers_tightest_fit() -> None:
    roomy = WorkerCandidate(worker("a"), 8000, 30 * GB, 0)
    tight = WorkerCandidate(worker("b"), 2000, 2 * GB, 3)
    assert select_worker([roomy, tight], res()) is tight.worker


def test_binpack_respects_limits() -> None:
    full = WorkerCandidate(worker("a"), 8000, 30 * GB, 4, max_sandboxes=4)
    small = WorkerCandidate(worker("b"), 500, 30 * GB, 0)
    assert select_worker([full, small], res()) is None


def test_scale_up_waits_on_pending_worker() -> None:
    pool = WorkerPool(name="x", max_workers=3)
    assert decide_scale_up(pool, []) is ScaleDecision.SCALE_UP
    assert decide_scale_up(pool, [worker("a", WorkerState.PROVISIONING)]) is ScaleDecision.WAIT
    at_max = [worker(str(i)) for i in range(3)]
    assert decide_scale_up(pool, at_max) is ScaleDecision.WAIT


def test_replacements_ignore_terminated_workers() -> None:
    pool = WorkerPool(name="x", min_workers=2)
    assert replacements_needed(pool, [worker("a", WorkerState.TERMINATED)]) == 2
    assert replacements_needed(pool, [worker("a"), worker("b")]) == 0


def test_scale_down_only_idle_past_ttl_and_above_min() -> None:
    now = datetime.now(UTC)
    pool = WorkerPool(name="x", min_workers=1, worker_idle_ttl_seconds=60)
    old = worker("old", idle_since=now - timedelta(minutes=5))
    fresh = worker("fresh", idle_since=now - timedelta(seconds=5))
    busy = worker("busy", idle_since=now - timedelta(minutes=5))
    picked = workers_to_scale_down(pool, [old, fresh, busy], {busy.id: 1}, {}, now)
    assert [w.id for w in picked] == [old.id]
