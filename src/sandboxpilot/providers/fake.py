"""Fake compute provider and fleet for tests and cloud-free development.

``FakeComputeProvider`` simulates SkyPilot: it "provisions" a worker VM by
starting a real in-process SandboxPilot worker (FastAPI + FakeSandboxRuntime)
on an ephemeral loopback port. The control plane then talks to it over HTTP
exactly as it would through an SSH tunnel to a real VM.

Failure modes: slow provisioning, out-of-capacity, credential failure, quota
errors, worker disappearance (spot preemption) and termination failures.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any

from sandboxpilot.errors import ProviderError, SkyPilotError, WorkerProvisionError
from sandboxpilot.providers.base import (
    CloudCheck,
    ComputeProvider,
    ProviderDoctorResult,
    ProviderWorkerStatus,
    ProvisionedWorker,
    WorkerEstimate,
    WorkerProvisionRequest,
)
from sandboxpilot.schemas.common import CloudProvider
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.utils.clock import Clock, SystemClock
from sandboxpilot.utils.logging import get_logger
from sandboxpilot.worker.app import create_worker_app, serve_in_process, stop_in_process
from sandboxpilot.worker.config import WorkerConfig
from sandboxpilot.worker.runtime.fake import FakeSandboxRuntime
from sandboxpilot.worker.service import WorkerService

log = get_logger("providers.fake")

FAKE_PRICES: dict[CloudProvider, float] = {
    CloudProvider.AWS: 0.68,
    CloudProvider.GCP: 0.61,
    CloudProvider.AZURE: 0.72,
}

FAKE_REGIONS: dict[CloudProvider, str] = {
    CloudProvider.AWS: "us-east-1",
    CloudProvider.GCP: "us-central1",
    CloudProvider.AZURE: "eastus",
}

FAKE_INSTANCE_TYPES: dict[CloudProvider, str] = {
    CloudProvider.AWS: "c7i.4xlarge",
    CloudProvider.GCP: "n2-standard-16",
    CloudProvider.AZURE: "Standard_D16s_v5",
}


@dataclass
class FakeWorkerVM:
    cluster_name: str
    worker_id: str
    pool_id: str
    token: str
    cloud: CloudProvider
    runtime: FakeSandboxRuntime
    service: WorkerService
    app: Any
    server: Any = None
    port: int | None = None
    status: str = "INIT"
    use_spot: bool = False

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"


class FakeFleet:
    """All fake worker VMs, addressable by cluster name (shared with FakeTunnelManager)."""

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = clock or SystemClock()
        self.vms: dict[str, FakeWorkerVM] = {}
        self.runtime_factory: Any = None

    async def boot(
        self,
        request: WorkerProvisionRequest,
        cloud: CloudProvider,
        *,
        cpu_millis: int | None = None,
        memory_bytes: int | None = None,
    ) -> FakeWorkerVM:
        pool = request.pool
        config = WorkerConfig(
            id=request.worker_id,
            pool_id=pool.id,
            token=request.worker_token,
            runtime="fake",
            reserve_cpus=pool.worker_reserve.cpus,
            reserve_memory=str(pool.worker_reserve.memory_bytes),
            max_sandboxes=pool.max_sandboxes_per_worker,
            state_dir=f"/tmp/sandboxpilot-fake/{request.worker_id}",  # type: ignore[arg-type]
            preload_images=",".join(request.preload_images),
            reaper_interval_seconds=0.05,
            warm_slots=pool.warm_slots,
            warm_spec=pool.warm_spec().model_dump_json() if pool.warm_slots else "",
        )
        runtime = (
            self.runtime_factory(request)
            if self.runtime_factory
            else FakeSandboxRuntime(
                clock=self.clock,
                cpu_millis=cpu_millis or pool.worker_cpus * 1000,
                memory_bytes=memory_bytes or pool.worker_memory_bytes,
            )
        )
        service = WorkerService(config, runtime, clock=self.clock)
        app = create_worker_app(config, runtime, clock=self.clock, service=service)
        vm = FakeWorkerVM(
            cluster_name=request.cluster_name,
            worker_id=request.worker_id,
            pool_id=pool.id,
            token=request.worker_token,
            cloud=cloud,
            runtime=runtime,
            service=service,
            app=app,
            use_spot=pool.use_spot,
        )
        vm.server, vm.port = await serve_in_process(app)
        vm.status = "UP"
        self.vms[request.cluster_name] = vm
        return vm

    async def stop(self, cluster_name: str) -> None:
        vm = self.vms.pop(cluster_name, None)
        if vm and vm.server is not None:
            await stop_in_process(vm.server)
            vm.server = None
        if vm:
            vm.status = "MISSING"

    async def disappear(self, cluster_name: str) -> None:
        """Simulate a preempted/terminated VM: the endpoint dies and status becomes MISSING."""
        await self.stop(cluster_name)

    def get(self, cluster_name: str) -> FakeWorkerVM | None:
        return self.vms.get(cluster_name)

    async def close(self) -> None:
        for name in list(self.vms):
            await self.stop(name)


@dataclass
class FakeProviderBehavior:
    provision_delay_seconds: float = 0.0
    fail_provision: Exception | None = None
    fail_provision_times: int = 0
    fail_terminate: Exception | None = None
    enabled_clouds: list[CloudProvider] = field(
        default_factory=lambda: [CloudProvider.AWS, CloudProvider.GCP, CloudProvider.AZURE]
    )
    installed: bool = True
    version: str = "0.13.0-fake"
    worker_cpu_millis: int | None = None
    worker_memory_bytes: int | None = None


class FakeComputeProvider(ComputeProvider):
    name = "fake"

    def __init__(
        self,
        fleet: FakeFleet | None = None,
        *,
        clock: Clock | None = None,
        behavior: FakeProviderBehavior | None = None,
    ) -> None:
        self.clock = clock or SystemClock()
        self.fleet = fleet or FakeFleet(self.clock)
        self.behavior = behavior or FakeProviderBehavior()
        self.provision_calls: list[WorkerProvisionRequest] = []
        self.terminate_calls: list[str] = []
        self._provision_failures = 0

    async def doctor(self) -> ProviderDoctorResult:
        clouds = [
            CloudCheck(
                cloud=c,
                enabled=c in self.behavior.enabled_clouds,
                reason=None if c in self.behavior.enabled_clouds else "not configured (fake)",
            )
            for c in CloudProvider
        ]
        return ProviderDoctorResult(
            provider=self.name,
            installed=self.behavior.installed,
            version=self.behavior.version if self.behavior.installed else None,
            clouds=clouds,
        )

    def _pick_cloud(self, request: WorkerProvisionRequest) -> CloudProvider:
        allowed = [
            c for c in request.pool.cloud_policy.providers if c in self.behavior.enabled_clouds
        ]
        if not allowed:
            wanted = ", ".join(c.value for c in request.pool.cloud_policy.providers)
            raise SkyPilotError(
                f"No enabled cloud satisfies the pool policy ({wanted}).",
                hint="Run `sky check` to see which clouds have working credentials.",
            )
        # "cost" strategy: cheapest first; deterministic.
        return min(allowed, key=lambda c: FAKE_PRICES[c])

    async def provision_worker(self, request: WorkerProvisionRequest) -> ProvisionedWorker:
        self.provision_calls.append(request)
        if self.behavior.fail_provision is not None and (
            self.behavior.fail_provision_times == 0
            or self._provision_failures < self.behavior.fail_provision_times
        ):
            self._provision_failures += 1
            raise self.behavior.fail_provision
        cloud = self._pick_cloud(request)
        if self.behavior.provision_delay_seconds:
            await self.clock.sleep(self.behavior.provision_delay_seconds)
        try:
            vm = await self.fleet.boot(
                request,
                cloud,
                cpu_millis=self.behavior.worker_cpu_millis,
                memory_bytes=self.behavior.worker_memory_bytes,
            )
        except Exception as exc:
            raise WorkerProvisionError(f"fake worker failed to boot: {exc}", cause=exc) from exc
        return ProvisionedWorker(
            cluster_name=request.cluster_name,
            cloud=cloud,
            region=request.pool.cloud_policy.region or FAKE_REGIONS[cloud],
            zone=request.pool.cloud_policy.zone,
            instance_type=request.pool.cloud_policy.instance_type or FAKE_INSTANCE_TYPES[cloud],
            use_spot=request.pool.use_spot,
            hourly_cost=FAKE_PRICES[cloud] * (0.3 if request.pool.use_spot else 1.0),
            ssh_alias=request.cluster_name,
            endpoint=vm.endpoint,
            metadata={"fake": True, "port": vm.port},
        )

    async def get_worker_status(self, worker: WorkerRecord) -> ProviderWorkerStatus:
        vm = self.fleet.get(worker.provider_cluster)
        if vm is None:
            return ProviderWorkerStatus(exists=False, status="MISSING")
        return ProviderWorkerStatus(exists=True, status=vm.status, raw={"port": vm.port})

    async def terminate_worker(self, worker: WorkerRecord) -> None:
        self.terminate_calls.append(worker.provider_cluster)
        if self.behavior.fail_terminate is not None:
            raise self.behavior.fail_terminate
        await self.fleet.stop(worker.provider_cluster)

    async def estimate_worker(self, request: WorkerProvisionRequest) -> WorkerEstimate:
        try:
            cloud = self._pick_cloud(request)
        except ProviderError as exc:
            return WorkerEstimate(note=str(exc))
        return WorkerEstimate(
            hourly_cost=FAKE_PRICES[cloud] * (0.3 if request.pool.use_spot else 1.0),
            cloud=cloud,
            instance_type=request.pool.cloud_policy.instance_type or FAKE_INSTANCE_TYPES[cloud],
            candidates=[
                {
                    "cloud": c.value,
                    "hourly_cost": FAKE_PRICES[c],
                    "instance_type": FAKE_INSTANCE_TYPES[c],
                }
                for c in request.pool.cloud_policy.providers
                if c in self.behavior.enabled_clouds
            ],
        )

    async def close(self) -> None:
        await self.fleet.close()


async def wait_for(predicate: Any, timeout: float = 5.0, interval: float = 0.01) -> None:
    """Test helper: poll ``predicate`` (sync or async) until truthy."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if loop.time() > deadline:
            raise TimeoutError("condition not met in time")
        with contextlib.suppress(Exception):
            await asyncio.sleep(interval)
