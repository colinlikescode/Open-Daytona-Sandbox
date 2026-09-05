"""ControlPlane: the brain behind the REST API.

Owns pools, workers and sandboxes. Talks to the compute provider (SkyPilot or
fake) to get VMs, to the tunnel manager to reach them, and to worker daemons
to run sandboxes. All state lives in SQLite so a restart picks up where it
left off.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import shutil
import sqlite3
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sandboxpilot.api.metrics import Metrics
from sandboxpilot.config.models import Config
from sandboxpilot.control.proxy_tokens import ProxyClaims, ProxyTokenSigner
from sandboxpilot.control.templates import TemplateStore
from sandboxpilot.control.tunnels import TunnelManager
from sandboxpilot.control.worker_client import WorkerClient, safe_close
from sandboxpilot.errors import (
    AuthenticationError,
    CapacityChangedError,
    ConflictError,
    CreateTimeoutError,
    NoCapacityError,
    NotFoundError,
    SandboxLostError,
    SandboxNotFoundError,
    SandboxNotRunningError,
    SandboxPilotError,
    SandboxRuntimeError,
    ValidationError,
    WorkerProvisionError,
    WorkerUnavailableError,
)
from sandboxpilot.providers.base import ComputeProvider, WorkerProvisionRequest, cluster_name_for
from sandboxpilot.scheduler import (
    CapacityLedger,
    SandboxScheduler,
    ScaleDecision,
    replacements_needed,
    workers_to_scale_down,
)
from sandboxpilot.schemas.commands import (
    CommandEvent,
    CommandInfo,
    CommandLogs,
    CommandRequest,
    CommandResult,
)
from sandboxpilot.schemas.common import (
    SandboxState,
    WorkerState,
    assert_sandbox_transition,
    assert_worker_transition,
)
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.schemas.pool import PoolCreateRequest, PoolUpdateRequest, WorkerPool
from sandboxpilot.schemas.sandbox import (
    DEFAULT_KEEPALIVE_COMMAND,
    SandboxCreateRequest,
    SandboxRecord,
    SandboxSpec,
)
from sandboxpilot.schemas.template import Template
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.state.db import Database
from sandboxpilot.utils.clock import Clock, SystemClock
from sandboxpilot.utils.ids import new_token
from sandboxpilot.utils.logging import bind_context, get_logger, reset_context
from sandboxpilot.utils.paths import templates_dir
from sandboxpilot.utils.sizes import parse_bytes, parse_duration
from sandboxpilot.version import WORKER_PROTOCOL_VERSION, __version__

log = get_logger("control")

# Sandbox states in which a create request is still looking for a worker.
_UNPLACED_STATES = frozenset(
    {
        SandboxState.PENDING,
        SandboxState.WAITING_FOR_CAPACITY,
        SandboxState.PROVISIONING_WORKER,
        SandboxState.CREATING,
    }
)


class ControlPlane:
    def __init__(
        self,
        config: Config,
        db: Database,
        provider: ComputeProvider,
        tunnels: TunnelManager,
        *,
        clock: Clock | None = None,
        metrics: Metrics | None = None,
        templates: TemplateStore | None = None,
        run_background_loop: bool = True,
    ) -> None:
        self.config = config
        self.db = db
        self.provider = provider
        self.tunnels = tunnels
        self.clock = clock or SystemClock()
        self.metrics = metrics or Metrics()
        self.templates = templates or TemplateStore(templates_dir())
        self.ledger = CapacityLedger()
        self.scheduler = SandboxScheduler(self.ledger)
        self.signer: ProxyTokenSigner | None = None
        self.started_at: datetime | None = None
        self._run_loop = run_background_loop
        self._loop_task: asyncio.Task[None] | None = None
        self._clients: dict[str, WorkerClient] = {}
        self._pool_locks: dict[str, asyncio.Lock] = {}
        # Creates waiting for capacity in a pool; every waiter is woken on any change.
        self._pool_waiters: dict[str, list[asyncio.Event]] = {}
        self._provisioning: dict[str, asyncio.Task[WorkerRecord]] = {}
        self._background: set[asyncio.Task[Any]] = set()
        self._reconcile_lock = asyncio.Lock()
        self._stopping = False

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        await self.db.open()
        secret = await self.db.runtime_state.get_or_create_secret()
        self.signer = ProxyTokenSigner(secret)
        await self._sync_pools_from_config()
        await self._recover_after_restart()
        self.started_at = self.clock.now()
        if self._run_loop:
            self._loop_task = asyncio.create_task(self._background_loop(), name="control-reconcile")

    async def stop(self) -> None:
        self._stopping = True
        if self._loop_task:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._loop_task
        for task in list(self._provisioning.values()) + list(self._background):
            task.cancel()
        for task in list(self._provisioning.values()) + list(self._background):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        for client in self._clients.values():
            await safe_close(client)
        self._clients.clear()
        await self.tunnels.close_all()
        with contextlib.suppress(Exception):
            await self.provider.close()
        await self.db.close()

    async def _sync_pools_from_config(self) -> None:
        for name, pool_cfg in self.config.pools.items():
            existing = await self.db.pools.get_by_name(name)
            pool = pool_cfg.to_pool(name, existing.id if existing else None)
            if existing:
                pool.created_at = existing.created_at
            await self.db.pools.upsert(pool)

    async def _recover_after_restart(self) -> None:
        failed = await self.db.operations.fail_stale_running("control plane restarted")
        if failed:
            log.info("marked %d in-flight operations as failed after restart", failed)
        for sb in await self.db.sandboxes.list(states=_UNPLACED_STATES - {SandboxState.CREATING}):
            await self._set_sandbox_state(
                sb, SandboxState.FAILED, error="control plane restarted before placement"
            )
        for worker in await self.db.workers.list():
            if worker.state.is_pending:
                # We lost the provisioning task. Ask the provider whether the VM made it.
                try:
                    status = await self.provider.get_worker_status(worker)
                except SandboxPilotError:
                    status = None
                if status is not None and status.is_up:
                    await self._set_worker_state(worker, WorkerState.CONNECTING)
                else:
                    log.warning(
                        "worker %s was provisioning during restart; terminating", worker.short_id
                    )
                    await self._terminate_worker(
                        worker, reason="provisioning interrupted by restart"
                    )
        for sb in await self.db.sandboxes.list(states={SandboxState.CREATING}):
            # Ask the worker; if it has the sandbox running we keep it.
            host = await self.db.workers.get(sb.worker_id) if sb.worker_id else None
            adopted = False
            if host and not host.state.is_terminal:
                with contextlib.suppress(SandboxPilotError):
                    client = await self._client(host)
                    view = await client.get_sandbox(sb.id)
                    if view.state == "RUNNING":
                        sb.started_at = sb.started_at or self.clock.now()
                        await self._set_sandbox_state(sb, SandboxState.RUNNING)
                        adopted = True
            if not adopted:
                await self._set_sandbox_state(
                    sb, SandboxState.FAILED, error="control plane restarted during creation"
                )

    # ------------------------------------------------------------------ helpers

    def _pool_lock(self, pool_id: str) -> asyncio.Lock:
        return self._pool_locks.setdefault(pool_id, asyncio.Lock())

    def _notify_capacity(self, pool_id: str) -> None:
        """Wake every create currently waiting for capacity in ``pool_id``."""
        for waiter in list(self._pool_waiters.get(pool_id, [])):
            waiter.set()

    async def _wait_capacity(self, pool_id: str, timeout: float) -> None:
        ev = asyncio.Event()
        waiters = self._pool_waiters.setdefault(pool_id, [])
        waiters.append(ev)
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        finally:
            with contextlib.suppress(ValueError):
                waiters.remove(ev)

    def _spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro, name=name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        task.add_done_callback(_log_background_failure)
        return task

    async def _client(self, worker: WorkerRecord) -> WorkerClient:
        base = await self.tunnels.ensure(worker)
        client = self._clients.get(worker.id)
        if client is None or client.base_url != base.rstrip("/"):
            await safe_close(client)
            client = WorkerClient(base, worker.token)
            self._clients[worker.id] = client
        return client

    async def _set_worker_state(
        self, worker: WorkerRecord, state: WorkerState, *, error: str | None = None
    ) -> WorkerRecord:
        assert_worker_transition(worker.state, state)
        worker.state = state
        worker.updated_at = self.clock.now()
        if error:
            worker.last_error = error
        if state == WorkerState.HEALTHY and worker.healthy_at is None:
            worker.healthy_at = self.clock.now()
        await self.db.workers.upsert(worker)
        return worker

    async def _set_sandbox_state(
        self, sb: SandboxRecord, state: SandboxState, *, error: str | None = None
    ) -> SandboxRecord:
        assert_sandbox_transition(sb.state, state)
        previous = sb.state
        sb.state = state
        sb.updated_at = self.clock.now()
        if error:
            sb.error = error
        if state.is_terminal and sb.ended_at is None:
            sb.ended_at = self.clock.now()
        await self.db.sandboxes.upsert(sb)
        if previous == SandboxState.RUNNING and state.is_terminal:
            self.metrics.sandboxes_running.labels(sb.pool_name).dec()
        if state == SandboxState.RUNNING and previous != SandboxState.RUNNING:
            self.metrics.sandboxes_running.labels(sb.pool_name).inc()
        return sb

    async def resolve_pool(self, ref: str | None) -> WorkerPool:
        name = ref or self.config.defaults.pool
        pool = await self.db.pools.resolve(name)
        if pool is None:
            if ref is None:
                pool = WorkerPool(name=name)
                await self.db.pools.upsert(pool)
                log.info("created default pool %r", name)
                return pool
            raise NotFoundError(
                f"pool {ref!r} not found",
                hint="Create one with: sandboxpilot pool create <name> --cloud aws --size 16cpu-64gb --workers 1-4",
            )
        return pool

    # ------------------------------------------------------------------ status

    async def status(self) -> dict[str, Any]:
        pools = await self.db.pools.list()
        workers = await self.db.workers.list()
        sandboxes = await self.db.sandboxes.list(active_only=True)
        running = [s for s in sandboxes if s.state == SandboxState.RUNNING]
        by_worker: dict[str, int] = {}
        for s in running:
            if s.worker_id:
                by_worker[s.worker_id] = by_worker.get(s.worker_id, 0) + 1
        return {
            "version": __version__,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "provider": self.provider.name,
            "pools": [
                {
                    "name": p.name,
                    "id": p.id,
                    "cloud": p.cloud_policy.describe(),
                    "workers": {
                        "healthy": len(
                            [
                                w
                                for w in workers
                                if w.pool_id == p.id and w.state == WorkerState.HEALTHY
                            ]
                        ),
                        "total": len([w for w in workers if w.pool_id == p.id]),
                        "min": p.min_workers,
                        "max": p.max_workers,
                    },
                    "sandboxes_running": len([s for s in running if s.pool_id == p.id]),
                    "estimated_hourly_cost": _sum_cost([w for w in workers if w.pool_id == p.id]),
                }
                for p in pools
            ],
            "workers": [
                {
                    **w.to_view(by_worker.get(w.id, 0)).model_dump(mode="json"),
                    "capacity": self.ledger.snapshot(w.id).model_dump(mode="json")
                    if w.state == WorkerState.HEALTHY
                    else w.capacity.model_dump(mode="json"),
                }
                for w in workers
            ],
            "sandboxes": {"running": len(running), "active": len(sandboxes)},
        }

    # ------------------------------------------------------------------ pools

    async def create_pool(self, request: PoolCreateRequest) -> WorkerPool:
        if await self.db.pools.get_by_name(request.name):
            raise ConflictError(f"pool {request.name!r} already exists")
        try:
            pool = request.to_pool()
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        return await self.db.pools.upsert(pool)

    async def list_pools(self) -> list[WorkerPool]:
        return await self.db.pools.list()

    async def get_pool(self, ref: str) -> WorkerPool:
        pool = await self.db.pools.resolve(ref)
        if pool is None:
            raise NotFoundError(f"pool {ref!r} not found")
        return pool

    async def update_pool(self, ref: str, request: PoolUpdateRequest) -> WorkerPool:
        pool = await self.get_pool(ref)
        try:
            updated = request.apply(pool)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        await self.db.pools.upsert(updated)
        return updated

    async def delete_pool(self, ref: str, *, force: bool = False) -> None:
        pool = await self.get_pool(ref)
        workers = await self.db.workers.list(pool_id=pool.id)
        if workers and not force:
            raise ConflictError(
                f"pool {pool.name!r} still has {len(workers)} worker(s)",
                hint=f"Run: sandboxpilot pool down {pool.name}   (add --force to kill running sandboxes)",
            )
        if workers:
            await self.pool_down(pool.name, force=True)
        await self.db.pools.delete(pool.id)

    async def pool_up(self, ref: str, *, workers: int | None = None) -> list[Operation]:
        """Provision workers up to ``min_workers`` (or ``workers`` if given)."""
        pool = await self.get_pool(ref)
        current = [
            w for w in await self.db.workers.list(pool_id=pool.id) if w.state.counts_toward_max
        ]
        target = workers if workers is not None else max(pool.min_workers, 1 if not current else 0)
        needed = (
            max(0, min(target, pool.max_workers) - len(current))
            if workers is not None
            else max(0, target - len(current))
        )
        ops: list[Operation] = []
        for _ in range(needed):
            ops.append(await self._start_provisioning(pool, reason="pool up"))
        return ops

    async def pool_down(self, ref: str, *, force: bool = False) -> dict[str, Any]:
        pool = await self.get_pool(ref)
        active = await self.db.sandboxes.list(pool_id=pool.id, active_only=True)
        if active and not force:
            raise ConflictError(
                f"pool {pool.name!r} has {len(active)} active sandbox(es)",
                hint="Kill them first or use --force.",
            )
        killed = 0
        for sb in active:
            with contextlib.suppress(SandboxPilotError):
                await self.kill_sandbox(sb.id)
                killed += 1
        terminated = 0
        for worker in await self.db.workers.list(pool_id=pool.id):
            task = self._provisioning.get(worker.id)
            if task is not None:
                # The provisioning task terminates its own worker when cancelled.
                task.cancel()
                await asyncio.wait({task}, timeout=60)
                terminated += 1
                continue
            await self._terminate_worker(worker, reason="pool down")
            terminated += 1
        return {"sandboxes_killed": killed, "workers_terminated": terminated}

    # ------------------------------------------------------------------ workers

    async def list_workers(self, pool: str | None = None) -> list[WorkerRecord]:
        pool_id = (await self.get_pool(pool)).id if pool else None
        return await self.db.workers.list(pool_id=pool_id, include_terminal=False)

    async def get_worker(self, ref: str) -> WorkerRecord:
        worker = await self.db.workers.get(ref)
        if worker is None:
            for w in await self.db.workers.list(include_terminal=True):
                if w.short_id == ref or w.id.startswith(ref) or w.provider_cluster == ref:
                    return w
            raise NotFoundError(f"worker {ref!r} not found")
        return worker

    async def sandboxes_on_worker(self, worker_id: str) -> int:
        return await self.db.sandboxes.count_active(worker_id)

    async def remove_worker(self, ref: str, *, force: bool = False) -> WorkerRecord:
        worker = await self.get_worker(ref)
        active = await self.db.sandboxes.list(worker_id=worker.id, active_only=True)
        if active and not force:
            raise ConflictError(
                f"worker {worker.short_id} has {len(active)} active sandbox(es); use --force or drain it"
            )
        for sb in active:
            with contextlib.suppress(SandboxPilotError):
                await self.kill_sandbox(sb.id)
        await self._terminate_worker(worker, reason="removed by user")
        return worker

    async def drain_worker(self, ref: str, *, terminate_when_empty: bool = True) -> WorkerRecord:
        worker = await self.get_worker(ref)
        worker.draining = True
        worker.terminate_when_empty = terminate_when_empty
        if worker.state == WorkerState.HEALTHY:
            await self._set_worker_state(worker, WorkerState.DRAINING)
        else:
            await self.db.workers.upsert(worker)
        with contextlib.suppress(SandboxPilotError):
            client = await self._client(worker)
            await client.drain(True)
        return worker

    async def _start_provisioning(self, pool: WorkerPool, *, reason: str) -> Operation:
        """Create a PROVISIONING worker record and kick off the provider call in the background."""
        worker = WorkerRecord(
            pool_id=pool.id,
            pool_name=pool.name,
            provider_cluster="",
            token=new_token(32),
            use_spot=pool.use_spot,
        )
        worker.provider_cluster = cluster_name_for(pool, worker.id)
        worker.provider_metadata = {"ssh_alias": worker.provider_cluster, "reason": reason}
        op = Operation(type="worker.provision", pool_id=pool.id, worker_id=worker.id)
        op.start(self.clock.now())
        # Register the task before the first await so reconcile never sees a
        # PROVISIONING record that no task owns (it would terminate it as orphaned).
        task = asyncio.create_task(
            self._provision(pool, worker, op), name=f"provision-{worker.short_id}"
        )
        self._provisioning[worker.id] = task
        worker_id = worker.id
        task.add_done_callback(lambda _t: self._provisioning.pop(worker_id, None))
        # _provision logs its own failures; just retrieve the exception to keep asyncio quiet.
        task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        await self.db.workers.upsert(worker)
        await self.db.operations.upsert(op)
        return op

    async def _provision(
        self, pool: WorkerPool, worker: WorkerRecord, op: Operation
    ) -> WorkerRecord:
        ctx = bind_context(
            worker_id=worker.id,
            pool_id=pool.id,
            operation_id=op.id,
            provider_cluster=worker.provider_cluster,
        )
        t0 = time.monotonic()
        try:
            request = WorkerProvisionRequest(
                worker_id=worker.id,
                pool=pool,
                cluster_name=worker.provider_cluster,
                worker_token=worker.token,
                sandboxpilot_version=__version__,
                preload_images=list(dict.fromkeys([pool.default_image, *pool.preload_images])),
            )
            log.info("provisioning worker via %s", self.provider.name)
            provisioned = await asyncio.wait_for(
                self.provider.provision_worker(request),
                self.config.reconcile.worker_provision_timeout_seconds,
            )
            worker.cloud = provisioned.cloud
            worker.region = provisioned.region
            worker.zone = provisioned.zone
            worker.instance_type = provisioned.instance_type
            worker.use_spot = provisioned.use_spot
            worker.hourly_cost = provisioned.hourly_cost
            worker.provider_metadata.update(provisioned.metadata)
            if provisioned.ssh_alias:
                worker.provider_metadata["ssh_alias"] = provisioned.ssh_alias
            if provisioned.endpoint:
                worker.provider_metadata["endpoint"] = provisioned.endpoint
            await self._set_worker_state(worker, WorkerState.CONNECTING)
            await self._connect_worker(worker)
            worker.provision_seconds = time.monotonic() - t0
            worker.idle_since = self.clock.now()
            await self.db.workers.upsert(worker)
            self.metrics.worker_provision_seconds.labels(
                pool.name, worker.cloud.value if worker.cloud else "unknown"
            ).observe(worker.provision_seconds)
            op.succeed(
                {
                    "worker_id": worker.id,
                    "cloud": worker.cloud.value if worker.cloud else None,
                    "seconds": worker.provision_seconds,
                },
                self.clock.now(),
            )
            await self.db.operations.upsert(op)
            log.info(
                "worker healthy after %.0fs on %s",
                worker.provision_seconds,
                worker.cloud.value if worker.cloud else "?",
            )
            return worker
        except asyncio.CancelledError:
            op.cancel(self.clock.now())
            await self.db.operations.upsert(op)
            await self._terminate_worker(worker, reason="provisioning cancelled")
            raise
        except Exception as exc:
            message = (
                str(exc) if isinstance(exc, SandboxPilotError) else f"{type(exc).__name__}: {exc}"
            )
            if isinstance(exc, TimeoutError):
                message = f"worker provisioning timed out after {self.config.reconcile.worker_provision_timeout_seconds:.0f}s"
            log.error("worker provisioning failed: %s", message)
            self.metrics.worker_errors_total.labels(pool.name, type(exc).__name__).inc()
            op.fail(message, self.clock.now())
            await self.db.operations.upsert(op)
            await self._terminate_worker(worker, reason=message)
            raise WorkerProvisionError(message, cause=exc) from exc
        finally:
            self._notify_capacity(pool.id)
            reset_context(ctx)

    async def _connect_worker(self, worker: WorkerRecord) -> None:
        client = await self._client(worker)
        health = await client.health(timeout=self.config.reconcile.worker_health_timeout_seconds)
        if health.worker_id != worker.id:
            raise WorkerUnavailableError(
                f"worker reported id {health.worker_id}, expected {worker.id}"
            )
        if health.worker_protocol_version != WORKER_PROTOCOL_VERSION:
            raise WorkerUnavailableError(
                f"worker protocol version {health.worker_protocol_version} is incompatible with control plane version {WORKER_PROTOCOL_VERSION}",
                hint="Upgrade or reprovision the worker so both sides run the same SandboxPilot version.",
            )
        if health.status not in {"healthy", "draining"}:
            raise WorkerUnavailableError(f"worker status is {health.status}")
        worker.version = health.sandboxpilot_version
        worker.protocol_version = health.worker_protocol_version
        worker.capacity = health.capacity
        worker.last_heartbeat_at = self.clock.now()
        worker.consecutive_failures = 0
        self.ledger.update(worker.id, health.capacity)
        target = (
            WorkerState.DRAINING if (health.draining or worker.draining) else WorkerState.HEALTHY
        )
        await self._set_worker_state(worker, target)
        if health.sandboxpilot_version != __version__:
            log.warning(
                "worker %s runs SandboxPilot %s, control plane runs %s",
                worker.short_id,
                health.sandboxpilot_version,
                __version__,
            )

    async def _terminate_worker(self, worker: WorkerRecord, *, reason: str) -> None:
        if worker.state.is_terminal:
            return
        with contextlib.suppress(Exception):
            await self._set_worker_state(worker, WorkerState.TERMINATING, error=reason)
        await self.tunnels.close(worker.id)
        await safe_close(self._clients.pop(worker.id, None))
        self.ledger.forget(worker.id)
        await self.db.images.delete_for_worker(worker.id)
        for sb in await self.db.sandboxes.list(worker_id=worker.id, active_only=True):
            await self._set_sandbox_state(
                sb, SandboxState.LOST, error=f"worker terminated: {reason}"
            )
        try:
            await self.provider.terminate_worker(worker)
        except SandboxPilotError as exc:
            log.error("provider failed to terminate %s: %s", worker.provider_cluster, exc)
            worker.last_error = str(exc)
            self.metrics.worker_errors_total.labels(worker.pool_name, "terminate").inc()
            # Leave it TERMINATING so cleanup/reconcile retries; never pretend it is gone.
            await self.db.workers.upsert(worker)
            return
        await self._set_worker_state(worker, WorkerState.TERMINATED)
        log.info("worker %s terminated (%s)", worker.short_id, reason)
        self._notify_capacity(worker.pool_id)

    async def _mark_worker_lost(self, worker: WorkerRecord, reason: str) -> None:
        log.warning("worker %s lost: %s", worker.short_id, reason)
        await self.tunnels.close(worker.id)
        await safe_close(self._clients.pop(worker.id, None))
        self.ledger.forget(worker.id)
        for sb in await self.db.sandboxes.list(worker_id=worker.id, active_only=True):
            await self._set_sandbox_state(sb, SandboxState.LOST, error=f"worker lost: {reason}")
            self.metrics.sandbox_errors_total.labels(sb.pool_name, "lost").inc()
        await self._set_worker_state(worker, WorkerState.LOST, error=reason)
        self.metrics.worker_errors_total.labels(worker.pool_name, "lost").inc()
        # Best effort: make sure the provider is not still billing us.
        with contextlib.suppress(Exception):
            await self.provider.terminate_worker(worker)
        self._notify_capacity(worker.pool_id)

    # ------------------------------------------------------------------ sandboxes

    def build_spec(
        self, pool: WorkerPool, request: SandboxCreateRequest, template: Template | None
    ) -> SandboxSpec:
        """Resolve a spec: pool defaults < global/env default timeout < template < request."""
        defaults = pool.sandbox_defaults
        merged: dict[str, Any] = {
            "image": defaults.image,
            "cpus": defaults.cpus,
            "memory": defaults.memory_bytes,
            "pids_limit": defaults.pids_limit,
            "timeout": defaults.timeout_seconds,
            "workdir": defaults.workdir,
            "network": defaults.network,
            "env": {},
            "labels": {},
        }
        if self.config.defaults.sandbox_timeout is not None:
            merged["timeout"] = self.config.defaults.sandbox_timeout
        if template:
            merged.update(template.as_create_overrides())
        explicit = {
            "image": request.image,
            "cpus": request.cpus,
            "memory": request.memory_bytes(),
            "pids_limit": request.pids_limit,
            "timeout": request.timeout_seconds(),
            "workdir": request.workdir,
            "network": request.network,
            "user": request.user,
            "read_only_root": request.read_only_root,
            "tmpfs": request.tmpfs_bytes(),
            "keepalive_command": request.keepalive_command,
            "image_pull_policy": request.image_pull_policy,
        }
        merged.update({k: v for k, v in explicit.items() if v is not None})
        env = {**merged.get("env", {}), **request.env}
        labels = {**merged.get("labels", {}), **request.labels}
        timeout_seconds = int(parse_duration(merged["timeout"]))
        if timeout_seconds > defaults.max_timeout_seconds:
            raise ValidationError(
                f"timeout {timeout_seconds}s exceeds the pool maximum of {defaults.max_timeout_seconds}s",
                hint="Raise sandbox.max_timeout in the pool configuration if you need longer sandboxes.",
            )
        try:
            return SandboxSpec(
                image=merged["image"],
                cpus=float(merged["cpus"]),
                memory_bytes=parse_bytes(merged["memory"]),
                pids_limit=int(merged["pids_limit"]),
                timeout_seconds=timeout_seconds,
                env=env,
                workdir=merged["workdir"],
                network=merged["network"],
                user=merged.get("user"),
                labels=labels,
                metadata=dict(request.metadata),
                read_only_root=bool(merged.get("read_only_root", False)),
                tmpfs_size_bytes=parse_bytes(merged["tmpfs"])
                if merged.get("tmpfs") is not None
                else None,
                keepalive_command=merged.get("keepalive_command")
                or list(DEFAULT_KEEPALIVE_COMMAND),
                image_pull_policy=merged.get("image_pull_policy")
                or SandboxSpec.model_fields["image_pull_policy"].default,
            )
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

    async def create_sandbox(
        self, request: SandboxCreateRequest, *, idempotency_key: str | None = None
    ) -> SandboxRecord:
        timeout = request.create_timeout or self.config.defaults.create_timeout
        if idempotency_key:
            existing = await self._existing_for_key(idempotency_key)
            if existing is not None:
                return await self._settle_existing(existing, wait=request.wait, timeout=timeout)
        pool = await self.resolve_pool(request.pool)
        template = await self.templates.get(request.template) if request.template else None
        spec = self.build_spec(pool, request, template)
        resources = spec.resources
        if not self.scheduler.fits_empty_worker(pool, resources):
            raise NoCapacityError(
                f"a {spec.cpus:g} CPU / {spec.memory_bytes / 1024**3:.1f} GB sandbox can never fit on a "
                f"{pool.worker_cpus} CPU / {pool.worker_memory_bytes / 1024**3:.0f} GB worker of pool {pool.name!r} after reserves",
                hint="Ask for a smaller sandbox or create a pool with bigger workers (--size).",
            )
        record = SandboxRecord(
            pool_id=pool.id, pool_name=pool.name, spec=spec, idempotency_key=idempotency_key
        )
        try:
            await self.db.sandboxes.upsert(record)
        except sqlite3.IntegrityError:
            # Two requests with the same key raced past the lookup above; the unique
            # index on idempotency_key made this one lose. Join the winner.
            existing = await self._existing_for_key(idempotency_key or "")
            if existing is None:
                raise
            return await self._settle_existing(existing, wait=request.wait, timeout=timeout)
        if idempotency_key:
            await self.db.idempotency.put(idempotency_key, record.id)
        self.metrics.sandboxes_total.labels(pool.name).inc()
        if request.wait:
            return await self._place_and_start(record, pool, timeout)
        self._spawn(self._place_and_start(record, pool, timeout), name=f"create-{record.short_id}")
        return record

    async def _existing_for_key(self, key: str) -> SandboxRecord | None:
        existing_id = await self.db.idempotency.get(key)
        if existing_id:
            found = await self.db.sandboxes.get(existing_id)
            if found is not None:
                return found
        return await self.db.sandboxes.get_by_idempotency_key(key)

    async def _settle_existing(
        self, existing: SandboxRecord, *, wait: bool, timeout: float
    ) -> SandboxRecord:
        if wait and existing.state in _UNPLACED_STATES:
            await self._wait_until_placed(existing, timeout)
            return (await self.db.sandboxes.get(existing.id)) or existing
        return existing

    async def _wait_until_placed(self, record: SandboxRecord, timeout: float) -> None:
        deadline = self.clock.monotonic() + timeout
        while self.clock.monotonic() < deadline:
            current = await self.db.sandboxes.get(record.id)
            if current is None or current.state not in _UNPLACED_STATES:
                return
            await asyncio.sleep(0.05)

    async def _place_and_start(
        self, record: SandboxRecord, pool: WorkerPool, timeout: float
    ) -> SandboxRecord:
        ctx = bind_context(sandbox_id=record.id, pool_id=pool.id)
        t0 = time.monotonic()
        deadline = t0 + timeout
        excluded: set[str] = set()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise CreateTimeoutError(
                        f"no capacity became available within {timeout:.0f}s",
                        hint="The pool may be at max_workers or a worker is still provisioning. Increase create_timeout or max_workers.",
                    )
                async with self._pool_lock(pool.id):
                    # The user may have killed the sandbox while it was waiting.
                    current = await self.db.sandboxes.get(record.id)
                    if current is None or current.state.is_terminal:
                        log.info("sandbox was killed before placement; giving up")
                        return current or record
                    pool = await self.db.pools.get(pool.id) or pool
                    workers = await self.db.workers.list(pool_id=pool.id)
                    result = self.scheduler.schedule(
                        pool, workers, record.spec.resources, exclude=excluded
                    )
                    if result.worker is not None:
                        worker = result.worker
                        self.ledger.reserve(worker.id, record.id, record.spec.resources)
                        record.worker_id = worker.id
                        await self._set_sandbox_state(record, SandboxState.CREATING)
                    elif result.decision == ScaleDecision.SCALE_UP:
                        await self._start_provisioning(
                            pool, reason=f"capacity for sandbox {record.short_id}"
                        )
                        await self._set_sandbox_state(record, SandboxState.PROVISIONING_WORKER)
                        worker = None
                    else:
                        state = (
                            SandboxState.PROVISIONING_WORKER
                            if any(w.state.is_pending for w in workers)
                            else SandboxState.WAITING_FOR_CAPACITY
                        )
                        if record.state != state:
                            await self._set_sandbox_state(record, state)
                        worker = None
                if worker is None:
                    with contextlib.suppress(TimeoutError):
                        await self._wait_capacity(pool.id, min(remaining, 2.0))
                    continue
                # Off the lock: talk to the worker.
                try:
                    client = await self._client(worker)
                    expires_at = self.clock.now() + timedelta(seconds=record.spec.timeout_seconds)
                    view = await client.create_sandbox(record.id, pool.id, record.spec, expires_at)
                except CapacityChangedError:
                    log.info(
                        "worker %s rejected placement (capacity changed); rescheduling",
                        worker.short_id,
                    )
                    self.ledger.release(worker.id, record.id)
                    excluded.add(worker.id)
                    fresh = await self.db.workers.get(worker.id)
                    if fresh:
                        with contextlib.suppress(SandboxPilotError):
                            health = await (await self._client(fresh)).health()
                            self.ledger.update(fresh.id, health.capacity)
                    record.worker_id = None
                    await self._set_sandbox_state(record, SandboxState.WAITING_FOR_CAPACITY)
                    continue
                except WorkerUnavailableError as exc:
                    log.warning("worker %s unavailable during create: %s", worker.short_id, exc)
                    self.ledger.release(worker.id, record.id)
                    excluded.add(worker.id)
                    fresh = await self.db.workers.get(worker.id)
                    if fresh and fresh.state == WorkerState.HEALTHY:
                        fresh.consecutive_failures += 1
                        await self._set_worker_state(fresh, WorkerState.UNHEALTHY, error=str(exc))
                    record.worker_id = None
                    await self._set_sandbox_state(record, SandboxState.WAITING_FOR_CAPACITY)
                    continue
                except SandboxPilotError as exc:
                    self.ledger.release(worker.id, record.id)
                    self.metrics.sandbox_errors_total.labels(pool.name, exc.code).inc()
                    await self._set_sandbox_state(record, SandboxState.FAILED, error=str(exc))
                    raise
                # A kill that raced with the worker call sees CREATING and cannot know
                # whether the container exists yet. Re-check and clean up if so.
                current = await self.db.sandboxes.get(record.id)
                if current is not None and current.state != SandboxState.CREATING:
                    log.info("sandbox was killed during creation; removing it from the worker")
                    self.ledger.release(worker.id, record.id)
                    with contextlib.suppress(SandboxPilotError):
                        await client.delete_sandbox(record.id)
                    if not current.state.is_terminal:
                        await self._set_sandbox_state(current, SandboxState.STOPPED)
                    self._notify_capacity(pool.id)
                    return current
                if view.state != "RUNNING":
                    # The worker accepted the request but the sandbox is not usable.
                    self.ledger.release(worker.id, record.id)
                    with contextlib.suppress(SandboxPilotError):
                        await client.delete_sandbox(record.id)
                    error = view.error or f"worker returned sandbox in state {view.state}"
                    self.metrics.sandbox_errors_total.labels(pool.name, "runtime_error").inc()
                    await self._set_sandbox_state(record, SandboxState.FAILED, error=error)
                    raise SandboxRuntimeError(error)
                self.ledger.confirm(worker.id, record.id)
                record.runtime_id = view.runtime_id
                record.expires_at = view.expires_at
                record.started_at = self.clock.now()
                record.metrics = {**view.metrics, "create_total_seconds": time.monotonic() - t0}
                await self._set_sandbox_state(record, SandboxState.RUNNING)
                worker.idle_since = None
                await self.db.workers.upsert(worker)
                if view.image_digest:
                    await self.db.images.record(worker.id, record.spec.image, view.image_digest)
                self.metrics.sandbox_create_seconds.labels(pool.name).observe(
                    view.metrics.get("sandbox_start_seconds", time.monotonic() - t0)
                )
                log.info(
                    "sandbox running on worker %s", worker.short_id, extra={"worker_id": worker.id}
                )
                return record
        except (CreateTimeoutError, NoCapacityError) as exc:
            if record.worker_id:
                self.ledger.release(record.worker_id, record.id)
            self.metrics.sandbox_errors_total.labels(pool.name, exc.code).inc()
            await self._fail_unless_terminal(record, str(exc))
            raise
        except asyncio.CancelledError:
            if record.worker_id:
                self.ledger.release(record.worker_id, record.id)
            with contextlib.suppress(Exception):
                await self._fail_unless_terminal(record, "cancelled")
            raise
        finally:
            reset_context(ctx)

    async def _fail_unless_terminal(self, record: SandboxRecord, error: str) -> None:
        """Mark a create as FAILED unless a concurrent kill already finished it."""
        current = await self.db.sandboxes.get(record.id)
        if current is not None and current.state.is_terminal:
            return
        await self._set_sandbox_state(current or record, SandboxState.FAILED, error=error)

    async def get_sandbox(self, ref: str) -> SandboxRecord:
        sb = await self.db.sandboxes.resolve(ref)
        if sb is None:
            raise SandboxNotFoundError(f"sandbox {ref!r} not found")
        return sb

    async def list_sandboxes(
        self, *, pool: str | None = None, active_only: bool = False, limit: int | None = None
    ) -> list[SandboxRecord]:
        pool_id = (await self.get_pool(pool)).id if pool else None
        return await self.db.sandboxes.list(pool_id=pool_id, active_only=active_only, limit=limit)

    async def _running(self, ref: str) -> tuple[SandboxRecord, WorkerRecord, WorkerClient]:
        sb = await self.get_sandbox(ref)
        if sb.state == SandboxState.LOST:
            raise SandboxLostError(
                f"sandbox {sb.short_id} was lost: {sb.error or 'worker disappeared'}"
            )
        if sb.state != SandboxState.RUNNING or not sb.worker_id:
            raise SandboxNotRunningError(
                f"sandbox {sb.short_id} is {sb.state.value}" + (f": {sb.error}" if sb.error else "")
            )
        worker = await self.db.workers.get(sb.worker_id)
        if worker is None or worker.state.is_terminal:
            raise SandboxLostError(f"sandbox {sb.short_id}'s worker is gone")
        return sb, worker, await self._client(worker)

    async def kill_sandbox(self, ref: str) -> SandboxRecord:
        return await self._stop_sandbox(ref, final=SandboxState.STOPPED)

    async def _stop_sandbox(
        self, ref: str, *, final: SandboxState, error: str | None = None
    ) -> SandboxRecord:
        """Tear a sandbox down and leave it in ``final`` (STOPPED for kills, EXPIRED for timeouts)."""
        sb = await self.get_sandbox(ref)
        if sb.state.is_terminal:
            return sb
        ctx = bind_context(sandbox_id=sb.id, pool_id=sb.pool_id, worker_id=sb.worker_id)
        try:
            if sb.state not in {SandboxState.RUNNING, SandboxState.CREATING}:
                # Never placed. Wake the placement loop so it sees the terminal state now.
                await self._set_sandbox_state(sb, SandboxState.STOPPED, error=error)
                self._notify_capacity(sb.pool_id)
                return sb
            await self._set_sandbox_state(sb, SandboxState.STOPPING)
            t0 = time.monotonic()
            worker = await self.db.workers.get(sb.worker_id) if sb.worker_id else None
            if worker and not worker.state.is_terminal:
                try:
                    client = await self._client(worker)
                    await client.delete_sandbox(sb.id)
                except SandboxNotFoundError:
                    pass
                except WorkerUnavailableError as exc:
                    # The worker's reaper will remove it when the sandbox expires; record what we know.
                    log.warning(
                        "could not reach worker %s to kill sandbox: %s", worker.short_id, exc
                    )
                    self.ledger.release(worker.id, sb.id)
                    await self._set_sandbox_state(
                        sb, SandboxState.LOST, error=f"worker unreachable during kill: {exc}"
                    )
                    return sb
                self.ledger.release(worker.id, sb.id)
                if await self.db.sandboxes.count_active(worker.id) <= 1:
                    worker.idle_since = self.clock.now()
                    await self.db.workers.upsert(worker)
            sb.metrics["destroy_seconds"] = time.monotonic() - t0
            await self._set_sandbox_state(sb, final, error=error)
            self._notify_capacity(sb.pool_id)
            return sb
        finally:
            reset_context(ctx)

    async def set_timeout(self, ref: str, seconds: int) -> SandboxRecord:
        sb, _worker, client = await self._running(ref)
        pool = await self.db.pools.get(sb.pool_id)
        max_seconds = pool.sandbox_defaults.max_timeout_seconds if pool else seconds
        if seconds <= 0:
            raise ValidationError("timeout must be positive")
        if seconds > max_seconds:
            raise ValidationError(f"timeout {seconds}s exceeds the pool maximum of {max_seconds}s")
        expires_at = self.clock.now() + timedelta(seconds=seconds)
        await client.set_expiration(sb.id, expires_at)
        sb.expires_at = expires_at
        sb.updated_at = self.clock.now()
        await self.db.sandboxes.upsert(sb)
        return sb

    # ------------------------------------------------------------------ commands

    async def run_command(self, ref: str, request: CommandRequest) -> CommandResult:
        sb, _worker, client = await self._running(ref)
        t0 = time.monotonic()
        try:
            result = await client.run_command(sb.id, request)
        except SandboxPilotError as exc:
            self.metrics.command_errors_total.labels(sb.pool_name, exc.code).inc()
            raise
        self.metrics.command_duration_seconds.labels(sb.pool_name).observe(time.monotonic() - t0)
        return result

    async def start_command(self, ref: str, request: CommandRequest) -> CommandInfo:
        sb, _worker, client = await self._running(ref)
        return await client.start_command(sb.id, request)

    async def get_command(self, ref: str, command_id: str) -> CommandInfo:
        sb, _worker, client = await self._running(ref)
        return await client.get_command(sb.id, command_id)

    async def command_result(
        self, ref: str, command_id: str, *, wait: bool = True
    ) -> CommandResult:
        sb, _worker, client = await self._running(ref)
        return await client.command_result(sb.id, command_id, wait=wait)

    async def command_logs(self, ref: str, command_id: str) -> CommandLogs:
        sb, _worker, client = await self._running(ref)
        return await client.command_logs(sb.id, command_id)

    async def kill_command(self, ref: str, command_id: str) -> CommandInfo:
        sb, _worker, client = await self._running(ref)
        return await client.kill_command(sb.id, command_id)

    async def stream_command(
        self, ref: str, command_id: str, from_seq: int = 0
    ) -> AsyncIterator[CommandEvent]:
        sb, _worker, client = await self._running(ref)
        async for event in client.stream_command(sb.id, command_id, from_seq):
            yield event

    # ------------------------------------------------------------------ files

    async def upload(
        self,
        ref: str,
        path: str,
        content: AsyncIterator[bytes] | bytes,
        *,
        archive: bool,
        mode: int | None,
        size: int | None,
    ) -> None:
        sb, _worker, client = await self._running(ref)
        await client.upload(sb.id, path, content, archive=archive, mode=mode, size=size)

    async def download(self, ref: str, path: str, *, archive: bool) -> AsyncIterator[bytes]:
        sb, _worker, client = await self._running(ref)
        async for chunk in client.download(sb.id, path, archive=archive):
            yield chunk

    # ------------------------------------------------------------------ proxy

    def sign_url(
        self, sandbox_id: str, port: int, expires_in: int | None = None
    ) -> tuple[str, datetime]:
        assert self.signer is not None
        ttl = expires_in or self.config.limits.proxy_url_ttl_seconds
        expires_at = self.clock.now() + timedelta(seconds=ttl)
        token = self.signer.sign(sandbox_id, port, expires_at)
        return f"{self.config.api.url}/v1/proxy/{sandbox_id}/{port}/{token}/", expires_at

    async def get_url(self, ref: str, port: int, expires_in: int | None = None) -> dict[str, Any]:
        sb = await self.get_sandbox(ref)
        if not 1 <= port <= 65535:
            raise ValidationError("port must be between 1 and 65535")
        url, expires_at = self.sign_url(sb.id, port, expires_in)
        return {"url": url, "expires_at": expires_at, "sandbox_id": sb.id, "port": port}

    def verify_proxy_token(self, token: str, sandbox_id: str, port: int) -> ProxyClaims:
        assert self.signer is not None
        claims = self.signer.verify(token, now=self.clock.now())
        if claims.sandbox_id != sandbox_id or claims.port != port:
            raise AuthenticationError("proxy token does not match this sandbox/port")
        return claims

    async def proxy_target(self, sandbox_id: str, port: int) -> tuple[WorkerClient, str]:
        sb, _worker, client = await self._running(sandbox_id)
        return client, client.proxy_base(sb.id, port)

    # ------------------------------------------------------------------ images

    async def preload_image(self, reference: str, *, pool: str | None = None) -> Operation:
        pool_obj = await self.get_pool(pool) if pool else None
        op = Operation(type="image.preload", pool_id=pool_obj.id if pool_obj else None)
        op.start(self.clock.now())
        await self.db.operations.upsert(op)
        workers = [
            w
            for w in await self.db.workers.list(pool_id=pool_obj.id if pool_obj else None)
            if w.state in {WorkerState.HEALTHY, WorkerState.DRAINING}
        ]
        results: dict[str, Any] = {}
        errors: list[str] = []

        async def pull_on(worker: WorkerRecord) -> None:
            try:
                client = await self._client(worker)
                info = await client.pull_image(reference)
                await self.db.images.record(worker.id, reference, info.digest)
                results[worker.id] = {
                    "pulled": info.pulled,
                    "digest": info.digest,
                    "seconds": info.pull_seconds,
                }
            except SandboxPilotError as exc:
                errors.append(f"{worker.short_id}: {exc.message}")

        # Pulls are network-bound on each VM; run them side by side.
        await asyncio.gather(*(pull_on(w) for w in workers))
        if pool_obj and reference not in pool_obj.preload_images:
            pool_obj.preload_images.append(reference)
            await self.db.pools.upsert(pool_obj)
        if errors and not results:
            op.fail("; ".join(errors), self.clock.now())
        else:
            op.succeed(
                {"workers": results, "errors": errors, "reference": reference}, self.clock.now()
            )
        await self.db.operations.upsert(op)
        return op

    async def list_images(self, *, worker: str | None = None) -> list[dict[str, Any]]:
        worker_id = (await self.get_worker(worker)).id if worker else None
        return [dataclasses.asdict(r) for r in await self.db.images.list(worker_id)]

    # ------------------------------------------------------------------ operations

    async def get_operation(self, op_id: str) -> Operation:
        op = await self.db.operations.get(op_id)
        if op is None:
            raise NotFoundError(f"operation {op_id!r} not found")
        return op

    async def wait_operation(self, op_id: str, timeout: float) -> Operation:
        deadline = self.clock.monotonic() + timeout
        while True:
            op = await self.get_operation(op_id)
            if op.status.is_terminal or self.clock.monotonic() >= deadline:
                return op
            await asyncio.sleep(0.1)

    # ------------------------------------------------------------------ reconciliation

    async def _background_loop(self) -> None:
        while not self._stopping:
            try:
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("reconcile iteration failed: %s", exc)
            await self.clock.sleep(self.config.reconcile.interval_seconds)

    async def reconcile(self) -> None:
        """One pass: worker health, inventory, expirations, min workers, scale-down, housekeeping."""
        async with self._reconcile_lock:
            pools = {p.id: p for p in await self.db.pools.list()}
            workers = await self.db.workers.list()
            for worker in workers:
                if worker.id in self._provisioning:
                    continue  # the provisioning task owns it until it is CONNECTING or gone
                if worker.state in {WorkerState.PROVISIONING, WorkerState.BOOTSTRAPPING}:
                    # No task is driving this worker (crash mid-provision); never let it bill idly.
                    fresh = await self.db.workers.get(worker.id) or worker
                    if fresh.state.is_pending:
                        await self._terminate_worker(fresh, reason="provisioning task lost")
                    continue
                if worker.state == WorkerState.TERMINATING:
                    await self._terminate_worker(worker, reason=worker.last_error or "terminating")
                    continue
                await self._check_worker(worker)
            await self._reconcile_expirations()
            for pool in pools.values():
                await self._reconcile_pool(pool)
            await self._housekeeping()
            self._update_gauges(pools, await self.db.workers.list())

    async def _check_worker(self, worker: WorkerRecord) -> None:
        ctx = bind_context(worker_id=worker.id, pool_id=worker.pool_id)
        try:
            try:
                client = await self._client(worker)
                health = await client.health(
                    timeout=self.config.reconcile.worker_health_timeout_seconds
                )
            except SandboxPilotError as exc:
                worker.consecutive_failures += 1
                worker.last_error = str(exc)
                if worker.state in {WorkerState.HEALTHY, WorkerState.DRAINING}:
                    await self._set_worker_state(worker, WorkerState.UNHEALTHY, error=str(exc))
                    log.warning("worker %s unhealthy: %s", worker.short_id, exc)
                else:
                    await self.db.workers.upsert(worker)
                if worker.consecutive_failures >= self.config.reconcile.health_failures_before_lost:
                    try:
                        status = await self.provider.get_worker_status(worker)
                    except SandboxPilotError as perr:
                        log.warning(
                            "provider status check failed for %s: %s", worker.short_id, perr
                        )
                        return
                    if not status.exists or status.status in {"STOPPED", "MISSING"}:
                        await self._mark_worker_lost(worker, f"provider reports {status.status}")
                return
            worker.consecutive_failures = 0
            worker.last_heartbeat_at = self.clock.now()
            worker.capacity = health.capacity
            worker.version = health.sandboxpilot_version
            worker.protocol_version = health.worker_protocol_version
            self.ledger.update(worker.id, health.capacity)
            if health.worker_protocol_version != WORKER_PROTOCOL_VERSION:
                await self._set_worker_state(
                    worker,
                    WorkerState.UNHEALTHY,
                    error=f"incompatible worker protocol {health.worker_protocol_version}",
                )
                return
            target = (
                WorkerState.DRAINING
                if (health.draining or worker.draining)
                else WorkerState.HEALTHY
            )
            if worker.state != target:
                if worker.state == WorkerState.UNHEALTHY:
                    log.info("worker %s recovered", worker.short_id)
                await self._set_worker_state(worker, target)
            else:
                await self.db.workers.upsert(worker)
            await self._reconcile_inventory(worker, client)
        finally:
            reset_context(ctx)

    async def _reconcile_inventory(self, worker: WorkerRecord, client: WorkerClient) -> None:
        try:
            remote = {v.sandbox_id: v for v in await client.list_sandboxes()}
        except SandboxPilotError:
            return
        ours = await self.db.sandboxes.list(worker_id=worker.id, active_only=True)
        for sb in ours:
            if sb.state != SandboxState.RUNNING:
                continue
            view = remote.get(sb.id)
            if view is None:
                await self._set_sandbox_state(
                    sb, SandboxState.LOST, error="sandbox disappeared from worker"
                )
                self.ledger.release(worker.id, sb.id)
            elif view.state == "EXPIRED":
                await self._set_sandbox_state(sb, SandboxState.EXPIRED, error="sandbox timed out")
                self.ledger.release(worker.id, sb.id)
            elif view.state in {"STOPPED", "FAILED"}:
                await self._set_sandbox_state(
                    sb,
                    SandboxState.STOPPED
                    if view.exit_code is not None or view.oom_killed
                    else SandboxState.FAILED,
                    error=view.error,
                )
                self.ledger.release(worker.id, sb.id)
            elif view.state == "LOST":
                await self._set_sandbox_state(
                    sb, SandboxState.LOST, error=view.error or "container disappeared"
                )
                self.ledger.release(worker.id, sb.id)
        known = {sb.id for sb in ours}
        for sid, view in remote.items():
            if (
                sid not in known
                and view.state in {"RUNNING", "CREATING"}
                and view.pool_id == worker.pool_id
            ):
                record = await self.db.sandboxes.get(sid)
                # Unknown, finished, or re-placed elsewhere after this worker timed out
                # mid-create: in every case the container here is an orphan.
                if record is None or record.state.is_terminal or record.worker_id != worker.id:
                    log.info("removing orphan sandbox %s on worker %s", sid, worker.short_id)
                    with contextlib.suppress(SandboxPilotError):
                        await client.delete_sandbox(sid)
        if not await self.db.sandboxes.count_active(worker.id) and worker.idle_since is None:
            worker.idle_since = self.clock.now()
            await self.db.workers.upsert(worker)

    async def _reconcile_expirations(self) -> None:
        now = self.clock.now()
        for sb in await self.db.sandboxes.list(states={SandboxState.RUNNING}):
            if sb.expires_at and sb.expires_at <= now:
                log.info("sandbox %s expired; killing", sb.short_id)
                try:
                    await self._stop_sandbox(
                        sb.id, final=SandboxState.EXPIRED, error="sandbox timed out"
                    )
                except SandboxPilotError as exc:
                    log.warning("could not expire sandbox %s: %s", sb.short_id, exc)

    async def _reconcile_pool(self, pool: WorkerPool) -> None:
        workers = await self.db.workers.list(pool_id=pool.id)
        missing = replacements_needed(pool, workers)
        for _ in range(missing):
            log.info("pool %s below min_workers; provisioning replacement", pool.name)
            await self._start_provisioning(pool, reason="maintain min_workers")
        active_counts: dict[str, int] = {}
        pending_counts: dict[str, int] = {}
        for w in workers:
            active_counts[w.id] = await self.db.sandboxes.count_active(w.id)
            pending_counts[w.id] = self.ledger.pending_count(w.id)
        for w in workers_to_scale_down(
            pool, workers, active_counts, pending_counts, self.clock.now()
        ):
            reason = (
                "drained"
                if w.terminate_when_empty
                else f"idle for more than {pool.worker_idle_ttl_seconds}s"
            )
            log.info("scaling down worker %s (%s)", w.short_id, reason)
            await self._terminate_worker(w, reason=reason)

    async def _housekeeping(self) -> None:
        cutoff = (self.clock.now() - timedelta(days=7)).isoformat()
        for step in (
            self.db.operations.delete_terminal_older_than(cutoff),
            self.db.idempotency.expire(self.config.limits.idempotency_ttl_seconds),
            self.db.sandboxes.delete_terminal_older_than(cutoff),
        ):
            try:
                await step
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("housekeeping step failed: %s", exc)

    def _update_gauges(self, pools: dict[str, WorkerPool], workers: list[WorkerRecord]) -> None:
        for pool in pools.values():
            mine = [w for w in workers if w.pool_id == pool.id]
            healthy = [w for w in mine if w.state == WorkerState.HEALTHY]
            self.metrics.workers_total.labels(pool.name).set(len(mine))
            self.metrics.workers_healthy.labels(pool.name).set(len(healthy))
            self.metrics.worker_capacity_cpu_millis.labels(pool.name).set(
                sum(w.capacity.cpu_millis_allocatable for w in healthy)
            )
            self.metrics.worker_allocated_cpu_millis.labels(pool.name).set(
                sum(self.ledger.snapshot(w.id).cpu_millis_allocated for w in healthy)
            )
            self.metrics.worker_capacity_memory_bytes.labels(pool.name).set(
                sum(w.capacity.memory_bytes_allocatable for w in healthy)
            )
            self.metrics.worker_allocated_memory_bytes.labels(pool.name).set(
                sum(self.ledger.snapshot(w.id).memory_bytes_allocated for w in healthy)
            )

    # ------------------------------------------------------------------ cleanup

    async def cleanup(self, *, terminate_workers: bool = False) -> dict[str, Any]:
        """Reconcile stale state and optionally tear down every SandboxPilot-owned worker."""
        report: dict[str, Any] = {
            "operations_failed": 0,
            "sandboxes_marked": 0,
            "workers_terminated": 0,
            "tunnels_closed": 0,
        }
        report["operations_failed"] = await self.db.operations.fail_stale_running("cleanup")
        for sb in await self.db.sandboxes.list(active_only=True):
            worker = await self.db.workers.get(sb.worker_id) if sb.worker_id else None
            if worker is None or worker.state.is_terminal:
                await self._set_sandbox_state(sb, SandboxState.LOST, error="cleanup: worker gone")
                report["sandboxes_marked"] += 1
        for wid in list(self._clients):
            if not await self.tunnels.is_alive(wid):
                await self.tunnels.close(wid)
                await safe_close(self._clients.pop(wid, None))
                report["tunnels_closed"] += 1
        if terminate_workers:
            for worker in await self.db.workers.list():
                task = self._provisioning.get(worker.id)
                if task is not None:
                    task.cancel()  # the task terminates its own worker on cancellation
                    await asyncio.wait({task}, timeout=60)
                else:
                    await self._terminate_worker(worker, reason="cleanup")
                report["workers_terminated"] += 1
        await self.reconcile()
        return report

    async def doctor(self) -> dict[str, Any]:
        """Health report: control plane, state, provider/clouds, ssh, pools and workers."""
        checks: list[dict[str, str]] = []

        def add(name: str, status: str, message: str) -> None:
            checks.append({"name": name, "status": status, "message": message})

        add("control plane", "ok", f"SandboxPilot {__version__} at {self.config.api.url}")
        add(
            "state",
            "ok",
            f"{self.db.path} (schema v{await self.db.schema_version()})",
        )
        provider = await self.provider.doctor()
        if provider.installed:
            add("provider", "ok", f"{provider.provider} {provider.version or ''}".strip())
        else:
            add(
                "provider", "fail", "; ".join(provider.errors) or f"{provider.provider} unavailable"
            )
        for cloud in provider.clouds:
            add(
                f"cloud {cloud.cloud.value}",
                "ok" if cloud.enabled else "warn",
                "credentials ok" if cloud.enabled else (cloud.reason or "not enabled"),
            )
        if provider.installed and provider.clouds and not provider.enabled_clouds:
            add("clouds", "fail", "no cloud has working credentials; run `sky check`")
        for err in provider.errors if provider.installed else []:
            add("provider", "fail", err)
        if self.provider.name == "skypilot":
            ssh = await asyncio.to_thread(shutil.which, "ssh")
            add("ssh", "ok" if ssh else "fail", ssh or "openssh client not found on PATH")
        workers = await self.db.workers.list()
        for pool in await self.db.pools.list():
            mine = [w for w in workers if w.pool_id == pool.id]
            healthy = [w for w in mine if w.state == WorkerState.HEALTHY]
            unhealthy = [w for w in mine if w.state in {WorkerState.UNHEALTHY, WorkerState.LOST}]
            status = "warn" if unhealthy else "ok"
            add(
                f"pool {pool.name}",
                status,
                f"{len(healthy)}/{len(mine)} workers healthy"
                + (f", {len(unhealthy)} unhealthy" if unhealthy else "")
                + f" ({pool.cloud_policy.describe()})",
            )
        for hint in provider.hints:
            if hint:
                add("hint", "warn", hint)
        return {
            "ok": not any(c["status"] == "fail" for c in checks),
            "checks": checks,
            "provider": provider.model_dump(mode="json"),
        }

    def templates_path(self) -> Path:
        return self.templates.directory


def _sum_cost(workers: list[WorkerRecord]) -> float | None:
    costs = [
        w.hourly_cost for w in workers if w.hourly_cost is not None and w.state.counts_toward_max
    ]
    if not costs:
        return None
    return round(sum(costs), 4)


def _log_background_failure(task: asyncio.Task[Any]) -> None:
    """Surface exceptions from fire-and-forget tasks instead of 'never retrieved' noise."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    if isinstance(exc, SandboxPilotError):
        log.warning("background task %s failed: %s", task.get_name(), exc.message)
    else:
        log.error("background task %s crashed", task.get_name(), exc_info=exc)
