"""Build SkyPilot ``Resources``/``Task`` objects from a pool definition.

This is the only place where AWS/GCP/Azure differ, and even here the
difference is just the ``infra`` string handed to SkyPilot.
"""

from __future__ import annotations

from typing import Any

from sandboxpilot.providers.base import WorkerProvisionRequest
from sandboxpilot.providers.skypilot.compatibility import SkyPilotCompatibility
from sandboxpilot.schemas.common import CloudProvider, CloudStrategy
from sandboxpilot.schemas.pool import WorkerPool

SKY_CLOUD_NAMES: dict[CloudProvider, str] = {
    CloudProvider.AWS: "aws",
    CloudProvider.GCP: "gcp",
    CloudProvider.AZURE: "azure",
}


def worker_labels(request: WorkerProvisionRequest) -> dict[str, str]:
    return {
        "sandboxpilot-pool-id": _label_safe(request.pool.id),
        "sandboxpilot-worker-id": _label_safe(request.worker_id),
        "sandboxpilot-version": _label_safe(request.sandboxpilot_version),
        "sandboxpilot-managed": "true",
    }


def _label_safe(value: str) -> str:
    out = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in value.lower())
    return out[:63].strip("-_") or "x"


def resource_kwargs(
    pool: WorkerPool, cloud: CloudProvider, compat: SkyPilotCompatibility, labels: dict[str, str]
) -> dict[str, Any]:
    policy = pool.cloud_policy
    kwargs: dict[str, Any] = {
        "cpus": f"{pool.worker_cpus}+",
        "memory": f"{max(1, pool.worker_memory_bytes // 1024**3)}+",
        "disk_size": pool.worker_disk_gb,
        "use_spot": pool.use_spot,
        "labels": labels,
    }
    if policy.instance_type:
        kwargs["instance_type"] = policy.instance_type
        # An explicit instance type already pins CPU/memory.
        kwargs.pop("cpus")
        kwargs.pop("memory")
    cloud_name = SKY_CLOUD_NAMES[cloud]
    if compat.supports_infra:
        infra = cloud_name
        if policy.region:
            infra += f"/{policy.region}"
            if policy.zone:
                infra += f"/{policy.zone}"
        kwargs["infra"] = infra
    else:  # pragma: no cover - legacy API
        kwargs["cloud"] = _legacy_cloud(compat, cloud_name)
        if policy.region:
            kwargs["region"] = policy.region
        if policy.zone:
            kwargs["zone"] = policy.zone
    return kwargs


def _legacy_cloud(compat: SkyPilotCompatibility, name: str) -> Any:  # pragma: no cover - legacy API
    clouds = compat.module.clouds
    return {"aws": clouds.AWS, "gcp": clouds.GCP, "azure": clouds.Azure}[name]()


def build_resources(request: WorkerProvisionRequest, compat: SkyPilotCompatibility) -> list[Any]:
    """One ``sky.Resources`` per allowed cloud; SkyPilot's optimizer picks among them."""
    labels = worker_labels(request)
    return [
        compat.module.Resources(**resource_kwargs(request.pool, cloud, compat, labels))
        for cloud in request.pool.cloud_policy.providers
    ]


def optimize_target(compat: SkyPilotCompatibility, strategy: CloudStrategy) -> Any:
    target_enum = getattr(compat.module, "OptimizeTarget", None)
    if target_enum is None:
        return None
    return target_enum.TIME if strategy == CloudStrategy.TIME else target_enum.COST


def build_task(
    request: WorkerProvisionRequest,
    compat: SkyPilotCompatibility,
    *,
    setup: str,
    run: str,
    file_mounts: dict[str, str],
    envs: dict[str, str],
) -> Any:
    task = compat.module.Task(name=request.cluster_name, setup=setup, run=run, envs=envs or None)
    resources = build_resources(request, compat)
    task.set_resources(set(resources) if len(resources) > 1 else resources[0])
    if file_mounts:
        task.set_file_mounts(file_mounts)
    return task
