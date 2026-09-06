"""Worker service: sandbox lifecycle on one host, backed by a SandboxRuntime."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from sandboxpilot.errors import (
    CapacityChangedError,
    RuntimeUnavailableError,
    SandboxNotFoundError,
    SandboxNotRunningError,
    SandboxRuntimeError,
    ValidationError,
)
from sandboxpilot.schemas.commands import CommandInfo, CommandLogs, CommandRequest, CommandResult
from sandboxpilot.schemas.sandbox import (
    RuntimeSandboxSpec,
    RuntimeSandboxState,
    SandboxResources,
    SandboxSpec,
)
from sandboxpilot.schemas.worker import WorkerCapacity, WorkerHealth
from sandboxpilot.utils.clock import Clock, SystemClock
from sandboxpilot.utils.ids import new_id
from sandboxpilot.utils.logging import bind_context, get_logger, reset_context
from sandboxpilot.version import WORKER_PROTOCOL_VERSION, __version__
from sandboxpilot.worker.capacity import WARM_PREFIX, CapacityManager
from sandboxpilot.worker.commands import CommandManager, CommandRun
from sandboxpilot.worker.config import WorkerConfig
from sandboxpilot.worker.runtime.base import (
    LABEL_CPU_MILLIS,
    LABEL_EXPIRES_AT,
    LABEL_MEMORY_BYTES,
    LABEL_PIDS_LIMIT,
    LABEL_POOL_ID,
    RuntimeDoctorResult,
    SandboxRuntime,
)

log = get_logger("worker.service")

# Plain (non-login) shell: proves /bin/sh works without paying for profile scripts.
READINESS_COMMAND = ["/bin/sh", "-c", "true"]

__all__ = [
    "WARM_PREFIX",
    "WorkerSandbox",
    "WorkerSandboxCreate",
    "WorkerSandboxView",
    "WorkerService",
]


class WorkerSandboxCreate(BaseModel):
    """Request body for ``POST /v1/sandboxes`` on the worker."""

    sandbox_id: str
    pool_id: str
    spec: SandboxSpec
    expires_at: datetime


class WorkerSandbox(BaseModel):
    sandbox_id: str
    pool_id: str
    runtime_id: str | None = None
    spec: SandboxSpec
    state: str = "CREATING"
    created_at: datetime
    expires_at: datetime
    image_digest: str | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    exit_code: int | None = None
    oom_killed: bool = False
    error: str | None = None


class WorkerSandboxView(BaseModel):
    sandbox_id: str
    pool_id: str
    runtime_id: str | None
    state: str
    created_at: datetime
    expires_at: datetime
    image: str
    image_digest: str | None
    resources: SandboxResources
    metrics: dict[str, float]
    exit_code: int | None
    oom_killed: bool
    error: str | None


class ExpirationUpdate(BaseModel):
    expires_at: datetime


class WorkerService:
    def __init__(
        self,
        config: WorkerConfig,
        runtime: SandboxRuntime,
        *,
        clock: Clock | None = None,
    ) -> None:
        self.config = config
        self.runtime = runtime
        self.clock = clock or SystemClock()
        self.sandboxes: dict[str, WorkerSandbox] = {}
        self.capacity: CapacityManager | None = None
        self.commands = CommandManager(self.clock, max_output_bytes=config.max_command_output_bytes)
        self.doctor_result: RuntimeDoctorResult | None = None
        self.started_at: float | None = None
        self.warnings: list[str] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._state_path = Path(config.state_dir) / "sandboxes.json"
        self._state_lock = asyncio.Lock()  # serializes state-file writes (done in a thread)
        self._ready = False
        self._sandbox_locks: dict[str, asyncio.Lock] = {}
        # Warm slots: pre-booted sandboxes waiting to be claimed. Keyed by their
        # placeholder id ("warm_..."). They hold real capacity reservations.
        self.warm: dict[str, WorkerSandbox] = {}
        self._warm_lock = asyncio.Lock()
        self._warm_wakeup = asyncio.Event()

    # -- lifecycle -----------------------------------------------------------------

    @property
    def ready(self) -> bool:
        return self._ready

    async def start(self) -> None:
        self.doctor_result = await self.runtime.doctor()
        if not self.doctor_result.ok:
            raise RuntimeUnavailableError(
                "Sandbox runtime verification failed; worker refuses to report READY.",
                details={"errors": self.doctor_result.errors},
            )
        self.warnings.extend(self.doctor_result.warnings)
        host = await self.runtime.host_resources()
        self.capacity = CapacityManager(
            cpu_millis_total=host.cpu_millis,
            memory_bytes_total=host.memory_bytes,
            reserve_cpu_millis=self.config.reserve_cpu_millis,
            reserve_memory_bytes=self.config.reserve_memory_bytes,
            max_sandboxes=self.config.max_sandboxes,
            disk_min_free_bytes=self.config.disk_min_free_bytes,
            disk_min_free_fraction=self.config.disk_min_free_fraction,
        )
        self.capacity.update_disk(host.disk_free_bytes, host.disk_total_bytes)
        if host.external_containers:
            self.warnings.append(
                f"{host.external_containers} non-SandboxPilot container(s) are running on this host and consume resources"
            )
        await self.reconcile()
        for image in self.config.preload_image_list:
            try:
                await self.runtime.ensure_image(image, "if-not-present")
            except Exception as exc:
                self.warnings.append(f"preload of {image} failed: {exc}")
        self.started_at = time.monotonic()
        self._ready = True
        self._tasks.append(asyncio.create_task(self._reaper_loop(), name="worker-reaper"))
        if self.config.warm_slots > 0 and self.config.warm_spec_model is not None:
            self._tasks.append(asyncio.create_task(self._warm_loop(), name="worker-warm"))

    async def shutdown(self) -> None:
        self._ready = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        await self.commands.shutdown()
        await self.runtime.close()

    async def reconcile(self) -> None:
        """Rebuild local state from labelled runtime containers (never touch anything else)."""
        assert self.capacity is not None
        persisted = await asyncio.to_thread(self._load_state)
        for known_id, known in persisted.items():
            if known.get("runtime_id"):
                self.runtime.remember(known_id, str(known["runtime_id"]))
        managed = await self.runtime.list_managed()
        seen: set[str] = set()
        for state in managed:
            sid = state.sandbox_id
            seen.add(sid)
            if sid.startswith(WARM_PREFIX):
                # Warm slots do not survive a restart; the warm loop boots fresh ones.
                with contextlib.suppress(Exception):
                    await self.runtime.remove(sid)
                continue
            saved = persisted.get(sid)
            resources = self._resources_from_state(state)
            expires_at = _parse_dt(saved.get("expires_at")) if saved else None
            if expires_at is None:
                expires_at = (
                    state.expires_at
                    or _parse_dt(state.labels.get(LABEL_EXPIRES_AT))
                    or (self.clock.now() + timedelta(hours=1))
                )
            spec = (
                SandboxSpec.model_validate(saved["spec"])
                if saved and "spec" in saved
                else SandboxSpec(
                    image=saved.get("image", "unknown") if saved else "unknown",
                    cpus=resources.cpu_millis / 1000,
                    memory_bytes=resources.memory_bytes,
                    pids_limit=resources.pids_limit,
                )
            )
            record = WorkerSandbox(
                sandbox_id=sid,
                pool_id=state.labels.get(LABEL_POOL_ID, saved.get("pool_id", "") if saved else ""),
                runtime_id=state.runtime_id,
                spec=spec,
                state="RUNNING" if state.running else "STOPPED",
                created_at=(_parse_dt(saved.get("created_at")) if saved else None)
                or self.clock.now(),
                expires_at=expires_at,
                exit_code=state.exit_code,
                oom_killed=state.oom_killed,
            )
            self.sandboxes[sid] = record
            if state.running:
                self.capacity.adopt(sid, resources)
            else:
                # Exited container left behind: clean up and free nothing (not counted).
                log.info("removing exited managed sandbox %s during reconciliation", sid)
                with contextlib.suppress(Exception):
                    await self.runtime.remove(sid)
                record.state = "STOPPED"
        for sid in list(persisted):
            if sid not in seen:
                log.info("dropping persisted sandbox %s: container no longer exists", sid)
        await self._save_state()
        await self.reap_expired()

    def _resources_from_state(self, state: RuntimeSandboxState) -> SandboxResources:
        if state.resources:
            return state.resources
        labels = state.labels
        return SandboxResources(
            cpu_millis=int(labels.get(LABEL_CPU_MILLIS, "1000")),
            memory_bytes=int(labels.get(LABEL_MEMORY_BYTES, str(1024**3))),
            pids_limit=int(labels.get(LABEL_PIDS_LIMIT, "1024")),
        )

    # -- persistence ------------------------------------------------------------------

    def _load_state(self) -> dict[str, dict[str, Any]]:
        try:
            if self._state_path.exists():
                data = json.loads(self._state_path.read_text())
                return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("could not read worker state file: %s", exc)
        return {}

    async def _save_state(self) -> None:
        """Persist live sandboxes atomically. The write runs in a thread; the lock keeps
        concurrent callers from interleaving and guarantees the newest snapshot wins."""
        async with self._state_lock:
            payload = {
                sid: {
                    "pool_id": sb.pool_id,
                    "runtime_id": sb.runtime_id,
                    "spec": sb.spec.model_dump(mode="json"),
                    "created_at": sb.created_at.isoformat(),
                    "expires_at": sb.expires_at.isoformat(),
                }
                for sid, sb in self.sandboxes.items()
                if sb.state in {"RUNNING", "CREATING"}
            }
            try:
                await asyncio.to_thread(self._write_state, json.dumps(payload))
            except OSError as exc:
                log.warning("could not persist worker state: %s", exc)

    def _write_state(self, text: str) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(text)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self._state_path)

    # -- health -----------------------------------------------------------------------

    async def health(self) -> WorkerHealth:
        assert self.capacity is not None
        with contextlib.suppress(Exception):
            host = await self.runtime.host_resources()
            self.capacity.update_disk(host.disk_free_bytes, host.disk_total_bytes)
        cap = self.capacity.snapshot()
        status = "healthy" if self._ready else "starting"
        if self.capacity.draining or cap.disk_pressure:
            status = "draining"
        return WorkerHealth(
            status=status,
            worker_id=self.config.id,
            sandboxpilot_version=__version__,
            worker_protocol_version=WORKER_PROTOCOL_VERSION,
            runtime=self.runtime.name,
            docker_version=self.doctor_result.docker_version if self.doctor_result else None,
            runsc_version=self.doctor_result.runsc_version if self.doctor_result else None,
            capacity=cap,
            sandboxes={"running": cap.sandboxes_running, "pending": cap.sandboxes_pending},
            warnings=list(self.warnings),
            draining=self.capacity.draining or cap.disk_pressure,
            uptime_seconds=(time.monotonic() - self.started_at) if self.started_at else None,
        )

    def capacity_snapshot(self) -> WorkerCapacity:
        assert self.capacity is not None
        return self.capacity.snapshot()

    def set_draining(self, draining: bool) -> None:
        assert self.capacity is not None
        self.capacity.draining = draining

    # -- sandboxes ----------------------------------------------------------------------

    def _lock_for(self, sandbox_id: str) -> asyncio.Lock:
        return self._sandbox_locks.setdefault(sandbox_id, asyncio.Lock())

    def _get(self, sandbox_id: str) -> WorkerSandbox:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            raise SandboxNotFoundError(f"Sandbox {sandbox_id} not found on this worker")
        return sb

    def view(self, sb: WorkerSandbox) -> WorkerSandboxView:
        return WorkerSandboxView(
            sandbox_id=sb.sandbox_id,
            pool_id=sb.pool_id,
            runtime_id=sb.runtime_id,
            state=sb.state,
            created_at=sb.created_at,
            expires_at=sb.expires_at,
            image=sb.spec.image,
            image_digest=sb.image_digest,
            resources=sb.spec.resources,
            metrics=sb.metrics,
            exit_code=sb.exit_code,
            oom_killed=sb.oom_killed,
            error=sb.error,
        )

    async def list_sandboxes(self) -> list[WorkerSandboxView]:
        return [self.view(sb) for sb in self.sandboxes.values()]

    async def get_sandbox(self, sandbox_id: str) -> WorkerSandboxView:
        sb = self._get(sandbox_id)
        if sb.state == "RUNNING":
            await self._refresh(sb)
        return self.view(sb)

    async def _refresh(self, sb: WorkerSandbox) -> None:
        state = await self.runtime.inspect(sb.sandbox_id)
        if not state.exists:
            sb.state = "LOST"
            sb.error = sb.error or "container disappeared"
            await self._release(sb.sandbox_id)
        elif not state.running and sb.state == "RUNNING":
            sb.state = "STOPPED"
            sb.exit_code = state.exit_code
            sb.oom_killed = state.oom_killed
            if state.oom_killed:
                sb.error = (
                    f"Sandbox process was terminated after exceeding its "
                    f"{sb.spec.memory_bytes / 1024**3:.1f} GB memory limit."
                )
            await self._release(sb.sandbox_id)
            with contextlib.suppress(Exception):
                await self.runtime.remove(sb.sandbox_id)

    async def _release(self, sandbox_id: str) -> None:
        if self.capacity:
            self.capacity.release(sandbox_id)
        await self._save_state()

    async def create_sandbox(self, req: WorkerSandboxCreate) -> WorkerSandboxView:
        if not self._ready or self.capacity is None:
            raise RuntimeUnavailableError("worker is not ready")
        token = bind_context(
            sandbox_id=req.sandbox_id, worker_id=self.config.id, pool_id=req.pool_id
        )
        try:
            return await self._create(req)
        finally:
            reset_context(token)

    async def _create(self, req: WorkerSandboxCreate) -> WorkerSandboxView:
        assert self.capacity is not None
        if req.sandbox_id in self.sandboxes and self.sandboxes[req.sandbox_id].state in {
            "RUNNING",
            "CREATING",
        }:
            return self.view(self.sandboxes[req.sandbox_id])
        resources = req.spec.resources
        t0 = time.monotonic()
        if self.capacity.draining or self.capacity.disk_pressure:
            # Do not evict warm slots for a request this worker cannot admit anyway.
            raise CapacityChangedError(
                "Worker is draining and not accepting new sandboxes",
                details={"worker_id": self.config.id},
            )
        claimed = await self._claim_warm(req)
        if claimed is not None:
            claimed.metrics["create_total_seconds"] = time.monotonic() - t0
            await self._save_state()
            self._warm_wakeup.set()
            return self.view(claimed)
        reserved = await self.capacity.try_reserve(req.sandbox_id, resources)
        while not reserved and self.warm:
            # A warm slot is holding capacity this request needs. Evict one.
            await self._drop_warm(next(iter(self.warm)))
            reserved = await self.capacity.try_reserve(req.sandbox_id, resources)
        if not reserved:
            raise CapacityChangedError(
                "Worker capacity changed; sandbox no longer fits on this worker",
                details={"worker_id": self.config.id},
            )
        record = WorkerSandbox(
            sandbox_id=req.sandbox_id,
            pool_id=req.pool_id,
            spec=req.spec,
            state="CREATING",
            created_at=self.clock.now(),
            expires_at=req.expires_at,
        )
        self.sandboxes[req.sandbox_id] = record
        metrics: dict[str, float] = {}
        try:
            image = await self.runtime.ensure_image(
                req.spec.image, req.spec.image_pull_policy.value
            )
            if image.pulled and image.pull_seconds is not None:
                metrics["image_pull_seconds"] = image.pull_seconds
            t_start = time.monotonic()
            created = await self.runtime.create(
                RuntimeSandboxSpec(
                    sandbox_id=req.sandbox_id,
                    pool_id=req.pool_id,
                    worker_id=self.config.id,
                    spec=req.spec,
                    expires_at=req.expires_at,
                )
            )
            record.runtime_id = created.runtime_id
            record.image_digest = created.image_digest or image.digest
            await self.runtime.start(req.sandbox_id)
            await self._verify_ready(req.sandbox_id, req.spec)
            metrics["sandbox_start_seconds"] = time.monotonic() - t_start
            metrics["create_total_seconds"] = time.monotonic() - t0
        except Exception as exc:
            record.state = "FAILED"
            record.error = str(exc)
            self.capacity.release(req.sandbox_id)
            with contextlib.suppress(Exception):
                await self.runtime.remove(req.sandbox_id)
            self.sandboxes.pop(req.sandbox_id, None)
            await self._save_state()
            if isinstance(exc, SandboxRuntimeError | ValidationError):
                raise
            raise SandboxRuntimeError(f"Sandbox creation failed: {exc}", cause=exc) from exc
        if record.state != "CREATING":
            # delete_sandbox() ran while the container was booting: honour it.
            log.info("sandbox was deleted while being created; discarding it")
            self.capacity.release(req.sandbox_id)
            with contextlib.suppress(Exception):
                await self.runtime.remove(req.sandbox_id)
            await self._save_state()
            return self.view(record)
        record.state = "RUNNING"
        record.metrics = metrics
        self.capacity.commit(req.sandbox_id)
        await self._save_state()
        log.info("sandbox running (%.0f ms)", metrics.get("sandbox_start_seconds", 0) * 1000)
        return self.view(record)

    # -- warm slots -------------------------------------------------------------------

    def _warm_compatible(self, slot: SandboxSpec, wanted: SandboxSpec) -> bool:
        """Everything that is baked into the container at create time must match.

        CPU, memory and pids limits are adjusted live on adoption, and the sandbox
        env is injected per exec, so they do not need to match.
        """
        return (
            slot.image == wanted.image
            and slot.network == wanted.network
            and slot.user == wanted.user
            and slot.workdir == wanted.workdir
            and slot.read_only_root == wanted.read_only_root
            and slot.tmpfs_size_bytes == wanted.tmpfs_size_bytes
            and slot.keepalive_command == wanted.keepalive_command
        )

    async def _claim_warm(self, req: WorkerSandboxCreate) -> WorkerSandbox | None:
        assert self.capacity is not None
        async with self._warm_lock:
            slot_id = next(
                (sid for sid, sb in self.warm.items() if self._warm_compatible(sb.spec, req.spec)),
                None,
            )
            if slot_id is None:
                return None
            slot = self.warm.pop(slot_id)
        # Swap the capacity reservation: free the slot's, take the request's.
        self.capacity.release(slot_id)
        if not await self.capacity.try_reserve(req.sandbox_id, req.spec.resources):
            self.capacity.adopt(slot_id, slot.spec.resources)
            async with self._warm_lock:
                self.warm[slot_id] = slot
            return None
        t0 = time.monotonic()
        try:
            adopted = await self.runtime.adopt(
                slot_id,
                RuntimeSandboxSpec(
                    sandbox_id=req.sandbox_id,
                    pool_id=req.pool_id,
                    worker_id=self.config.id,
                    spec=req.spec,
                    expires_at=req.expires_at,
                ),
            )
        except Exception as exc:
            log.warning(
                "warm slot %s could not be adopted, falling back to cold create: %s", slot_id, exc
            )
            adopted = None
        if adopted is None:
            self.capacity.release(req.sandbox_id)
            with contextlib.suppress(Exception):
                await self.runtime.remove(slot_id)
            return None
        record = WorkerSandbox(
            sandbox_id=req.sandbox_id,
            pool_id=req.pool_id,
            runtime_id=adopted.runtime_id,
            spec=req.spec,
            state="RUNNING",
            created_at=self.clock.now(),
            expires_at=req.expires_at,
            image_digest=adopted.image_digest or slot.image_digest,
            metrics={"sandbox_start_seconds": time.monotonic() - t0, "warm_slot": 1.0},
        )
        self.sandboxes[req.sandbox_id] = record
        self.capacity.commit(req.sandbox_id)
        log.info(
            "sandbox claimed warm slot (%.0f ms)", record.metrics["sandbox_start_seconds"] * 1000
        )
        return record

    async def _drop_warm(self, slot_id: str) -> None:
        assert self.capacity is not None
        async with self._warm_lock:
            self.warm.pop(slot_id, None)
        self.capacity.release(slot_id)
        with contextlib.suppress(Exception):
            await self.runtime.remove(slot_id)

    async def _boot_warm(self, spec: SandboxSpec) -> bool:
        """Boot one warm slot. Returns False when nothing was booted (caller should wait)."""
        assert self.capacity is not None
        slot_id = f"{WARM_PREFIX}{new_id().replace('-', '')}"
        if not await self.capacity.try_reserve(slot_id, spec.resources):
            return False
        expires_at = self.clock.now() + timedelta(days=365)
        try:
            await self.runtime.ensure_image(spec.image, spec.image_pull_policy.value)
            created = await self.runtime.create(
                RuntimeSandboxSpec(
                    sandbox_id=slot_id,
                    pool_id=self.config.pool_id,
                    worker_id=self.config.id,
                    spec=spec,
                    expires_at=expires_at,
                )
            )
            await self.runtime.start(slot_id)
            await self._verify_ready(slot_id, spec)
        except Exception as exc:
            self.capacity.release(slot_id)
            with contextlib.suppress(Exception):
                await self.runtime.remove(slot_id)
            log.warning("could not boot warm slot: %s", exc)
            await self.clock.sleep(5.0)
            return False
        self.capacity.commit(slot_id)
        record = WorkerSandbox(
            sandbox_id=slot_id,
            pool_id=self.config.pool_id,
            runtime_id=created.runtime_id,
            spec=spec,
            state="RUNNING",
            created_at=self.clock.now(),
            expires_at=expires_at,
            image_digest=created.image_digest,
        )
        async with self._warm_lock:
            self.warm[slot_id] = record
        return True

    def _wants_warm_slot(self, spec: SandboxSpec) -> bool:
        assert self.capacity is not None
        return (
            len(self.warm) < self.config.warm_slots
            and not self.capacity.draining
            and not self.capacity.disk_pressure
            and self.capacity.fits(spec.resources)
        )

    async def _warm_loop(self) -> None:
        """Keep ``warm_slots`` pre-booted sandboxes ready, without starving real requests."""
        spec = self.config.warm_spec_model
        assert spec is not None and self.capacity is not None
        while True:
            self._warm_wakeup.clear()
            try:
                # Boot back-to-back only while a boot actually succeeds; any refusal
                # (draining, disk pressure, capacity taken) falls through to the wait.
                if self._wants_warm_slot(spec) and await self._boot_warm(spec):
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("warm loop error: %s", exc)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._warm_wakeup.wait(), timeout=2.0)

    async def _verify_ready(self, sandbox_id: str, spec: SandboxSpec) -> None:
        handle = await self.runtime.exec(sandbox_id, READINESS_COMMAND, cwd="/")
        async for _ in handle.stream():
            pass
        code = await asyncio.wait_for(handle.wait(), 30)
        if code != 0:
            raise SandboxRuntimeError(
                f"Sandbox readiness check failed (exit {code}); the image must provide /bin/sh",
                hint="Use a shell-compatible image or supply keepalive_command for shell-less images.",
            )
        # Ensure the working directory exists (best effort; read-only roots use tmpfs mounts).
        mk = await self.runtime.exec(
            sandbox_id, ["/bin/sh", "-c", 'mkdir -p "$1"', "sp", spec.workdir], cwd="/"
        )
        async for _ in mk.stream():
            pass
        with contextlib.suppress(Exception):
            await asyncio.wait_for(mk.wait(), 10)

    async def delete_sandbox(
        self, sandbox_id: str, *, final_state: str = "STOPPED"
    ) -> WorkerSandboxView | None:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            # Idempotent: also remove any stray runtime object with this id.
            with contextlib.suppress(Exception):
                await self.runtime.remove(sandbox_id)
            return None
        async with self._lock_for(sandbox_id):
            if sb.state in {"STOPPED", "FAILED", "LOST", "EXPIRED"}:
                return self.view(sb)
            sb.state = "STOPPING"
            await self.commands.kill_all(sandbox_id)
            t0 = time.monotonic()
            with contextlib.suppress(Exception):
                await self.runtime.stop(sandbox_id, timeout=3.0)
            with contextlib.suppress(Exception):
                await self.runtime.remove(sandbox_id)
            sb.metrics["destroy_seconds"] = time.monotonic() - t0
            sb.state = final_state
            await self._release(sandbox_id)
            return self.view(sb)

    async def set_expiration(self, sandbox_id: str, expires_at: datetime) -> WorkerSandboxView:
        sb = self._get(sandbox_id)
        sb.expires_at = expires_at
        await self._save_state()
        return self.view(sb)

    # -- reaper -------------------------------------------------------------------------

    async def _reaper_loop(self) -> None:
        while True:
            try:
                await self.clock.sleep(self.config.reaper_interval_seconds)
                await self.reap_expired()
                await self._poll_running()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("reaper iteration failed: %s", exc)

    async def reap_expired(self) -> int:
        now = self.clock.now()
        reaped = 0
        for sb in list(self.sandboxes.values()):
            if sb.state in {"RUNNING", "CREATING"} and sb.expires_at <= now:
                log.info("sandbox %s expired; removing", sb.sandbox_id)
                await self.delete_sandbox(sb.sandbox_id, final_state="EXPIRED")
                reaped += 1
        return reaped

    async def _poll_running(self) -> None:
        for sb in list(self.sandboxes.values()):
            if sb.state == "RUNNING":
                with contextlib.suppress(Exception):
                    await self._refresh(sb)

    # -- commands ------------------------------------------------------------------------

    def _running(self, sandbox_id: str) -> WorkerSandbox:
        sb = self._get(sandbox_id)
        if sb.state != "RUNNING":
            raise SandboxNotRunningError(
                f"Sandbox {sandbox_id} is {sb.state}" + (f": {sb.error}" if sb.error else "")
            )
        return sb

    async def start_command(self, sandbox_id: str, request: CommandRequest) -> CommandRun:
        sb = self._running(sandbox_id)
        argv = request.argv()
        env = {**sb.spec.env, **request.env}
        cwd = request.cwd or sb.spec.workdir
        user = request.user or sb.spec.user

        async def factory() -> Any:
            return await self.runtime.exec(sandbox_id, argv, env=env, cwd=cwd, user=user)

        return await self.commands.start(sandbox_id, request, factory)

    async def run_command(self, sandbox_id: str, request: CommandRequest) -> CommandResult:
        run = await self.start_command(sandbox_id, request)
        await run.wait()
        return run.result()

    def get_command(self, command_id: str) -> CommandInfo:
        return self.commands.get(command_id).info()

    def command_result(self, command_id: str) -> CommandResult:
        return self.commands.get(command_id).result()

    def command_logs(self, command_id: str) -> CommandLogs:
        return self.commands.get(command_id).logs()

    async def kill_command(self, command_id: str) -> CommandInfo:
        return await self.commands.kill(command_id)

    def stream_command(self, command_id: str, from_seq: int = 0) -> AsyncIterator[Any]:
        return self.commands.get(command_id).subscribe(from_seq)

    # -- files -----------------------------------------------------------------------------

    async def upload(self, sandbox_id: str, path: str, archive: AsyncIterator[bytes]) -> None:
        self._running(sandbox_id)
        # Docker's put_archive needs the target directory to exist. Create it so
        # `sb.write("/anywhere/file")` just works.
        if path not in {"/", ""}:
            mk = await self.runtime.exec(
                sandbox_id, ["/bin/sh", "-c", 'mkdir -p "$1"', "sp", path], cwd="/"
            )
            await mk.wait()
        await self.runtime.upload(sandbox_id, path, archive)

    def download(self, sandbox_id: str, path: str) -> AsyncIterator[bytes]:
        self._running(sandbox_id)
        return self.runtime.download(sandbox_id, path)

    # -- network ----------------------------------------------------------------------------

    async def endpoint(self, sandbox_id: str, port: int) -> tuple[str, int]:
        self._running(sandbox_id)
        return await self.runtime.endpoint(sandbox_id, port)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
