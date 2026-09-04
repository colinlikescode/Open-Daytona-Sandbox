"""WorkerService against the fake runtime: create/exec/files/reaper/warm slots/restart."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest

from sandboxpilot.errors import CapacityChangedError
from sandboxpilot.schemas.commands import CommandRequest
from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.schemas.sandbox import SandboxSpec
from sandboxpilot.utils.tarstream import single_file_tar_bytes
from sandboxpilot.worker.config import WorkerConfig
from sandboxpilot.worker.runtime.fake import FakeSandboxRuntime
from sandboxpilot.worker.service import WorkerSandboxCreate, WorkerService

POOL = WorkerPool(name="p")


def make_config(tmp_path: Path, **kw: object) -> WorkerConfig:
    return WorkerConfig(
        runtime="fake",
        state_dir=tmp_path,
        token="t" * 32,
        reaper_interval_seconds=0.05,
        **kw,  # type: ignore[arg-type]
    )


@pytest.fixture
async def svc(tmp_path: Path) -> AsyncIterator[WorkerService]:
    service = WorkerService(make_config(tmp_path), FakeSandboxRuntime())
    await service.start()
    try:
        yield service
    finally:
        await service.shutdown()


def req(sid: str, svc: WorkerService, minutes: float = 5, **spec: object) -> WorkerSandboxCreate:
    fields: dict[str, object] = {"image": "python:3.12-slim", "cpus": 1.0, "memory_bytes": 1024**3}
    fields.update(spec)
    return WorkerSandboxCreate(
        sandbox_id=sid,
        pool_id=POOL.id,
        spec=SandboxSpec.model_validate(fields),
        expires_at=svc.clock.now() + timedelta(minutes=minutes),
    )


async def test_create_exec_files_kill(svc: WorkerService) -> None:
    view = await svc.create_sandbox(req("sbx_a", svc, env={"K": "v"}))
    assert view.state == "RUNNING"
    result = await svc.run_command("sbx_a", CommandRequest(command="echo $K"))
    assert result.exit_code == 0 and result.stdout.strip() == "v"
    failing = await svc.run_command("sbx_a", CommandRequest(command="exit 7"))
    assert failing.exit_code == 7

    async def chunks() -> AsyncIterator[bytes]:
        yield single_file_tar_bytes("x.txt", b"hello", 0o644)

    await svc.upload("sbx_a", "/workspace", chunks())
    result = await svc.run_command("sbx_a", CommandRequest(command="cat /workspace/x.txt"))
    assert result.stdout == "hello"
    archive = b"".join([c async for c in svc.download("sbx_a", "/workspace/x.txt")])
    assert b"hello" in archive
    killed = await svc.delete_sandbox("sbx_a")
    assert killed is not None and killed.state == "STOPPED"
    assert not svc.capacity_snapshot().sandboxes_running


async def test_create_is_idempotent(svc: WorkerService) -> None:
    a = await svc.create_sandbox(req("sbx_b", svc))
    b = await svc.create_sandbox(req("sbx_b", svc))
    assert a.runtime_id == b.runtime_id
    assert svc.capacity_snapshot().sandboxes_running == 1


async def test_reaper_kills_expired(svc: WorkerService) -> None:
    await svc.create_sandbox(req("sbx_c", svc, minutes=0.002))
    for _ in range(50):
        if svc.sandboxes["sbx_c"].state != "RUNNING":
            break
        await asyncio.sleep(0.05)
    assert svc.sandboxes["sbx_c"].state == "EXPIRED"


async def test_command_timeout(svc: WorkerService) -> None:
    await svc.create_sandbox(req("sbx_d", svc))
    result = await svc.run_command("sbx_d", CommandRequest(command="sleep 5", timeout=0.2))
    assert result.status.value == "TIMED_OUT"


async def test_capacity_rejects_when_full(tmp_path: Path) -> None:
    svc = WorkerService(make_config(tmp_path, max_sandboxes=1), FakeSandboxRuntime())
    await svc.start()
    try:
        await svc.create_sandbox(req("sbx_e", svc))
        with pytest.raises(CapacityChangedError):
            await svc.create_sandbox(req("sbx_f", svc))
    finally:
        await svc.shutdown()


async def test_state_survives_restart(tmp_path: Path) -> None:
    runtime = FakeSandboxRuntime()
    svc = WorkerService(make_config(tmp_path), runtime)
    await svc.start()
    await svc.create_sandbox(req("sbx_g", svc, env={"A": "1"}))
    await svc.shutdown()

    again = WorkerService(make_config(tmp_path), runtime)
    await again.start()
    try:
        assert again.sandboxes["sbx_g"].state == "RUNNING"
        assert again.sandboxes["sbx_g"].spec.env == {"A": "1"}
        assert again.capacity_snapshot().sandboxes_running == 1
    finally:
        await again.shutdown()


async def test_warm_slots_are_claimed_and_refilled(tmp_path: Path) -> None:
    runtime = FakeSandboxRuntime()
    cfg = make_config(tmp_path, warm_slots=2, warm_spec=POOL.warm_spec().model_dump_json())
    svc = WorkerService(cfg, runtime)
    await svc.start()
    try:
        for _ in range(100):
            if len(svc.warm) == 2:
                break
            await asyncio.sleep(0.02)
        assert len(svc.warm) == 2
        snap = svc.capacity_snapshot()
        assert snap.sandboxes_warm == 2 and snap.sandboxes_running == 0

        spec = POOL.warm_spec().model_copy(update={"cpus": 2.0, "env": {"FOO": "bar"}})
        view = await svc.create_sandbox(
            WorkerSandboxCreate(
                sandbox_id="sbx_h",
                pool_id=POOL.id,
                spec=spec,
                expires_at=svc.clock.now() + timedelta(minutes=5),
            )
        )
        assert view.metrics.get("warm_slot") == 1.0
        assert view.resources.cpu_millis == 2000
        assert runtime.adopt_count == 1
        result = await svc.run_command("sbx_h", CommandRequest(command="echo $FOO"))
        assert result.stdout.strip() == "bar"

        # Different image cannot use a warm slot: cold path.
        await runtime.ensure_image("node:20-slim", "if-not-present")
        cold = await svc.create_sandbox(req("sbx_i", svc, image="node:20-slim"))
        assert "warm_slot" not in cold.metrics

        for _ in range(100):
            if len(svc.warm) == 2:
                break
            await asyncio.sleep(0.02)
        assert len(svc.warm) == 2
    finally:
        await svc.shutdown()


async def test_warm_slots_yield_capacity_to_real_requests(tmp_path: Path) -> None:
    runtime = FakeSandboxRuntime()
    cfg = make_config(
        tmp_path, max_sandboxes=2, warm_slots=2, warm_spec=POOL.warm_spec().model_dump_json()
    )
    svc = WorkerService(cfg, runtime)
    await svc.start()
    try:
        for _ in range(100):
            if len(svc.warm) == 2:
                break
            await asyncio.sleep(0.02)
        await runtime.ensure_image("node:20-slim", "if-not-present")
        # Both slots are held by warm sandboxes; a non-matching request must still fit.
        view = await svc.create_sandbox(req("sbx_j", svc, image="node:20-slim"))
        assert view.state == "RUNNING"
        assert len(svc.warm) <= 1
    finally:
        await svc.shutdown()
