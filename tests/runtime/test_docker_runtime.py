"""Real Docker / gVisor tests. Skipped unless the runtime is present.

    pytest -m docker   # needs a Docker daemon (uses plain runc, dev only)
    pytest -m gvisor   # needs Docker with the runsc runtime configured

Both exercise the same WorkerService paths the fake runtime covers, plus the
isolation checks that only mean something on a real kernel.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

import pytest

from sandboxpilot.schemas.commands import CommandRequest
from sandboxpilot.schemas.common import NetworkPolicy
from sandboxpilot.schemas.sandbox import SandboxSpec
from sandboxpilot.worker.config import WorkerConfig
from sandboxpilot.worker.runtime.docker_gvisor import GVisorDockerRuntime
from sandboxpilot.worker.service import WorkerSandboxCreate, WorkerService

IMAGE = "alpine:3.20"


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0


def _runsc_ok() -> bool:
    if not _docker_ok():
        return False
    out = subprocess.run(
        ["docker", "info", "--format", "{{json .Runtimes}}"], capture_output=True, text=True
    )
    return '"runsc"' in out.stdout


DOCKER = pytest.mark.skipif(not _docker_ok(), reason="no Docker daemon available")
GVISOR = pytest.mark.skipif(not _runsc_ok(), reason="Docker has no runsc runtime")


async def _service(tmp_path: Path, *, unsafe_runc: bool) -> WorkerService:
    runtime = GVisorDockerRuntime(unsafe_runc=unsafe_runc, network_subnet=None)
    cfg = WorkerConfig(
        runtime="docker-unsafe" if unsafe_runc else "gvisor",
        state_dir=tmp_path,
        token="t" * 32,
        reaper_interval_seconds=0.2,
        preload_images=IMAGE,
    )
    svc = WorkerService(cfg, runtime)
    await svc.start()
    return svc


def _req(svc: WorkerService, sid: str, **spec: object) -> WorkerSandboxCreate:
    fields: dict[str, object] = {"image": IMAGE, "cpus": 0.5, "memory_bytes": 256 * 1024**2}
    fields.update(spec)
    return WorkerSandboxCreate(
        sandbox_id=sid,
        pool_id="pool_test",
        spec=SandboxSpec.model_validate(fields),
        expires_at=svc.clock.now() + timedelta(minutes=5),
    )


async def _lifecycle(svc: WorkerService) -> None:
    sid = "sbx_rt-" + str(id(svc))[-8:]
    view = await svc.create_sandbox(_req(svc, sid, env={"HELLO": "world"}))
    try:
        assert view.state == "RUNNING"
        r = await svc.run_command(sid, CommandRequest(command="echo $HELLO"))
        assert r.exit_code == 0 and r.stdout.strip() == "world"
        r = await svc.run_command(sid, CommandRequest(command="exit 9"))
        assert r.exit_code == 9
        r = await svc.run_command(sid, CommandRequest(command="sleep 10", timeout=0.5))
        assert r.status.value == "TIMED_OUT"
        # pids limit and memory limit are visible from inside
        r = await svc.run_command(
            sid,
            CommandRequest(
                command="cat /sys/fs/cgroup/pids.max 2>/dev/null || cat /sys/fs/cgroup/pids/pids.max"
            ),
        )
        assert r.exit_code == 0
    finally:
        await svc.delete_sandbox(sid)


@DOCKER
@pytest.mark.docker
async def test_docker_unsafe_runc_lifecycle(tmp_path: Path) -> None:
    svc = await _service(tmp_path, unsafe_runc=True)
    try:
        await _lifecycle(svc)
    finally:
        await svc.shutdown()


@GVISOR
@pytest.mark.gvisor
async def test_gvisor_lifecycle_and_isolation(tmp_path: Path) -> None:
    svc = await _service(tmp_path, unsafe_runc=False)
    sid = "sbx_gv-isolation"
    try:
        await _lifecycle(svc)
        view = await svc.create_sandbox(_req(svc, sid))
        assert view.state == "RUNNING"
        # gVisor's kernel identifies itself; a plain runc container would show the host kernel.
        r = await svc.run_command(
            sid, CommandRequest(command="dmesg 2>/dev/null | head -1; uname -r")
        )
        assert "gVisor" in r.stdout or "Starting gVisor" in r.stdout
        # No host capabilities, no way to see host processes.
        r = await svc.run_command(sid, CommandRequest(command="ls /proc | grep -c '^[0-9]' "))
        assert int(r.stdout.strip() or "0") < 10
        # Cloud metadata endpoint must be unreachable.
        r = await svc.run_command(
            sid,
            CommandRequest(
                command="wget -qO- --timeout=2 http://169.254.169.254/ && echo REACHED || echo BLOCKED"
            ),
        )
        assert "BLOCKED" in r.stdout
        # DNS must work even though gVisor cannot use Docker's embedded resolver.
        r = await svc.run_command(sid, CommandRequest(command="nslookup example.com", timeout=15))
        assert r.exit_code == 0, r.stderr
    finally:
        await svc.delete_sandbox(sid)
        await svc.shutdown()


@GVISOR
@pytest.mark.gvisor
async def test_gvisor_network_none(tmp_path: Path) -> None:
    svc = await _service(tmp_path, unsafe_runc=False)
    sid = "sbx_gv-nonet"
    try:
        await svc.create_sandbox(_req(svc, sid, network=NetworkPolicy.NONE))
        r = await svc.run_command(
            sid,
            CommandRequest(
                command="wget -qO- --timeout=2 http://1.1.1.1/ && echo REACHED || echo BLOCKED"
            ),
        )
        assert "BLOCKED" in r.stdout
    finally:
        await svc.delete_sandbox(sid)
        await svc.shutdown()


@GVISOR
@pytest.mark.gvisor
async def test_gvisor_warm_slot_adoption(tmp_path: Path) -> None:
    """The warm path renames + live-updates a real runsc container."""
    spec = SandboxSpec(image=IMAGE, cpus=0.5, memory_bytes=256 * 1024**2)
    runtime = GVisorDockerRuntime(network_subnet=None)
    cfg = WorkerConfig(
        runtime="gvisor",
        state_dir=tmp_path,
        token="t" * 32,
        warm_slots=1,
        warm_spec=spec.model_dump_json(),
    )
    svc = WorkerService(cfg, runtime)
    await svc.start()
    sid = "sbx_gv-warm"
    try:
        for _ in range(200):
            if svc.warm:
                break
            await asyncio.sleep(0.1)
        assert svc.warm, "warm slot never booted"
        view = await svc.create_sandbox(
            _req(svc, sid, cpus=1.0, memory_bytes=512 * 1024**2, env={"W": "1"})
        )
        assert view.metrics.get("warm_slot") == 1.0
        assert view.metrics["sandbox_start_seconds"] < 1.0
        state = await runtime.inspect(sid)
        assert state.resources and state.resources.cpu_millis == 1000
        r = await svc.run_command(sid, CommandRequest(command="echo $W"))
        assert r.stdout.strip() == "1"
    finally:
        await svc.delete_sandbox(sid)
        await svc.shutdown()
