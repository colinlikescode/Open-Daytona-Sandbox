"""SkyPilotComputeProvider: worker VMs on AWS, GCP or Azure through SkyPilot.

SandboxPilot never calls cloud SDKs. Provisioning, credentials, region and
instance selection, spot handling and teardown are all delegated to SkyPilot.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

from sandboxpilot.errors import SkyPilotError
from sandboxpilot.providers.base import (
    CloudCheck,
    ComputeProvider,
    ProviderDoctorResult,
    ProviderWorkerStatus,
    ProvisionedWorker,
    WorkerEstimate,
    WorkerProvisionRequest,
)
from sandboxpilot.providers.skypilot import bootstrap as bootstrap_mod
from sandboxpilot.providers.skypilot.compatibility import SkyPilotCompatibility
from sandboxpilot.providers.skypilot.errors import classify
from sandboxpilot.providers.skypilot.resources import build_resources, build_task, optimize_target
from sandboxpilot.schemas.common import V1_AUTO_CLOUDS, CloudProvider
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.utils.logging import bind_context, get_logger, reset_context

log = get_logger("providers.skypilot")

_CLOUD_BY_NAME = {"aws": CloudProvider.AWS, "gcp": CloudProvider.GCP, "azure": CloudProvider.AZURE}


def ssh_config_path(cluster_name: str) -> Path:
    """SkyPilot writes one OpenSSH config per cluster."""
    return Path.home() / ".sky" / "generated" / "ssh" / cluster_name


class SkyPilotComputeProvider(ComputeProvider):
    name = "skypilot"

    def __init__(
        self, compat: SkyPilotCompatibility | None = None, *, install_mode: str | None = None
    ) -> None:
        self._compat = compat
        self.install_mode = install_mode

    @property
    def compat(self) -> SkyPilotCompatibility:
        if self._compat is None:
            self._compat = SkyPilotCompatibility.detect()
        return self._compat

    @property
    def sky(self) -> Any:
        return self.compat.module

    async def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    # -- doctor ---------------------------------------------------------------------------

    async def doctor(self) -> ProviderDoctorResult:
        try:
            compat = self.compat
        except SkyPilotError as exc:
            return ProviderDoctorResult(
                provider=self.name, installed=False, errors=[exc.message], hints=[exc.hint or ""]
            )
        clouds: list[CloudCheck] = []
        errors: list[str] = []
        try:
            enabled = await self._call(self._enabled_clouds)
            for cloud in V1_AUTO_CLOUDS:
                ok = cloud in enabled
                clouds.append(
                    CloudCheck(
                        cloud=cloud,
                        enabled=ok,
                        reason=None if ok else f"not enabled; run `sky check {cloud.value}`",
                    )
                )
        except Exception as exc:
            errors.append(f"sky check failed: {exc}")
            clouds = [
                CloudCheck(cloud=c, enabled=False, reason="sky check failed")
                for c in V1_AUTO_CLOUDS
            ]
        return ProviderDoctorResult(
            provider=self.name, installed=True, version=compat.version, clouds=clouds, errors=errors
        )

    def _enabled_clouds(self) -> set[CloudProvider]:
        """Ask SkyPilot which of AWS/GCP/Azure have working credentials.

        ``sky.check`` re-runs credential checks (slow but authoritative);
        ``sky.enabled_clouds`` returns the cached result. We run check first
        so `doctor` reflects the current state, and fall back to the cache.
        """
        sky = self.sky
        result: Any = None
        check = self.compat.sdk("check")
        if check is not None:
            infra = tuple(c.value for c in V1_AUTO_CLOUDS)
            for kwargs in (
                {"infra_list": infra, "verbose": False},
                {"clouds": list(infra), "verbose": False},
                {},
            ):
                try:
                    result = self.compat.resolve(check(**kwargs))
                    break
                except TypeError:
                    continue
        enabled = _clouds_from_check_result(result)
        if not enabled:
            enabled_fn = self.compat.sdk("enabled_clouds")
            if enabled_fn is not None:
                with contextlib.suppress(Exception):
                    enabled = _clouds_from_check_result(self.compat.resolve(enabled_fn()))
        if not enabled:
            registry = getattr(
                getattr(sky, "global_user_state", None), "get_cached_enabled_clouds", None
            )
            if callable(registry):
                with contextlib.suppress(Exception):
                    enabled = _clouds_from_check_result([str(c) for c in registry()])
        return enabled

    # -- provisioning ------------------------------------------------------------------------

    async def provision_worker(self, request: WorkerProvisionRequest) -> ProvisionedWorker:
        token = bind_context(
            worker_id=request.worker_id,
            pool_id=request.pool.id,
            provider_cluster=request.cluster_name,
        )
        payload = await self._call(
            bootstrap_mod.build_payload, request, install_mode=self.install_mode
        )
        try:
            task = build_task(
                request,
                self.compat,
                setup=payload.setup,
                run=payload.run,
                file_mounts=payload.file_mounts,
                envs=payload.envs,
            )
            log.info("launching SkyPilot cluster (install mode: %s)", payload.install_mode)
            await self._call(self._launch, task, request)
            record = await self._call(self._status_record, request.cluster_name, refresh=False)
            if record is None:
                raise SkyPilotError(
                    f"SkyPilot did not report cluster {request.cluster_name} after launch"
                )
            return _provisioned_from_record(request.cluster_name, record)
        except SkyPilotError:
            raise
        except Exception as exc:
            raise classify(exc, request.pool.cloud_policy) from exc
        finally:
            payload.cleanup()
            reset_context(token)

    def _sdk(self, name: str) -> Any:
        fn = self.compat.sdk(name)
        if fn is None:
            raise SkyPilotError(f"this SkyPilot build ({self.compat.version}) has no `{name}` API")
        return fn

    def _launch(self, task: Any, request: WorkerProvisionRequest) -> None:
        kwargs: dict[str, Any] = {"cluster_name": request.cluster_name, "retry_until_up": False}
        target = optimize_target(self.compat, request.pool.cloud_policy.strategy)
        if target is not None:
            kwargs["optimize_target"] = target
        try:
            result = self._sdk("launch")(task, **kwargs)
        except TypeError:
            kwargs.pop("optimize_target", None)
            result = self._sdk("launch")(task, **kwargs)
        self.compat.resolve(result, stream=True)

    def _status_record(self, cluster_name: str, *, refresh: bool) -> dict[str, Any] | None:
        kwargs: dict[str, Any] = {"cluster_names": [cluster_name]}
        if refresh:
            mode = getattr(self.sky, "StatusRefreshMode", None)
            kwargs["refresh"] = mode.FORCE if mode is not None else True
        records = self.compat.resolve(self._sdk("status")(**kwargs))
        for rec in records or []:
            record = _record_to_dict(rec)
            if record.get("name") == cluster_name:
                return record
        return None

    async def get_worker_status(self, worker: WorkerRecord) -> ProviderWorkerStatus:
        try:
            record = await self._call(self._status_record, worker.provider_cluster, refresh=True)
        except Exception as exc:
            raise classify(exc, action="query worker status") from exc
        if record is None:
            return ProviderWorkerStatus(exists=False, status="MISSING")
        status = record.get("status")
        status_name = getattr(status, "name", str(status)).upper()
        return ProviderWorkerStatus(exists=True, status=status_name, raw=_safe_record(record))

    async def terminate_worker(self, worker: WorkerRecord) -> None:
        try:
            await self._call(
                lambda: self.compat.resolve(self._sdk("down")(worker.provider_cluster))
            )
        except Exception as exc:
            if type(exc).__name__ == "ClusterDoesNotExist" or "does not exist" in str(exc).lower():
                return
            raise classify(exc, action="terminate a worker") from exc

    async def estimate_worker(self, request: WorkerProvisionRequest) -> WorkerEstimate:
        try:
            resources = await self._call(build_resources, request, self.compat)
        except Exception as exc:
            return WorkerEstimate(note=f"could not build resources: {exc}")
        candidates: list[dict[str, Any]] = []
        for res in resources:
            cost = None
            with contextlib.suppress(Exception):
                cost = float(res.get_cost(3600))
            candidates.append(
                {
                    "cloud": str(getattr(res, "cloud", "") or getattr(res, "infra", "")),
                    "instance_type": getattr(res, "instance_type", None),
                    "hourly_cost": cost,
                }
            )
        priced = [c for c in candidates if c["hourly_cost"] is not None]
        best = min(priced, key=lambda c: c["hourly_cost"]) if priced else None
        return WorkerEstimate(
            hourly_cost=best["hourly_cost"] if best else None,
            cloud=_CLOUD_BY_NAME.get(str(best["cloud"]).split("/")[0].lower()) if best else None,
            instance_type=best["instance_type"] if best else None,
            candidates=candidates,
            note=None
            if priced
            else "SkyPilot could not price the request without a concrete instance type",
        )


def _clouds_from_check_result(result: Any) -> set[CloudProvider]:
    """Pull AWS/GCP/Azure out of whatever shape SkyPilot hands back.

    Seen in the wild:
      - ``sky.check``: ``{workspace: {cloud: [capability, ...]}}`` (new)
        or ``{cloud: [capability, ...]}`` / ``[cloud, ...]`` (old)
      - ``sky.enabled_clouds``: ``[cloud, ...]``
    Cloud names may be ``"AWS"``, ``"aws"`` or cloud objects whose str() is the name.
    """
    found: set[CloudProvider] = set()

    def visit(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                cloud = _CLOUD_BY_NAME.get(str(key).lower())
                if cloud is not None:
                    # Cloud -> capabilities. Empty list means it is disabled.
                    if value is None or value:
                        found.add(cloud)
                elif isinstance(value, dict | list | tuple | set):
                    visit(value)  # workspace -> clouds
            return
        if isinstance(node, list | tuple | set):
            for item in node:
                cloud = _CLOUD_BY_NAME.get(str(item).lower())
                if cloud is not None:
                    found.add(cloud)
                elif isinstance(item, dict | list | tuple | set):
                    visit(item)
            return
        cloud = _CLOUD_BY_NAME.get(str(node).lower())
        if cloud is not None:
            found.add(cloud)

    visit(result)
    return found


def _record_to_dict(rec: Any) -> dict[str, Any]:
    """Status records were plain dicts for years; newer servers return pydantic models."""
    if isinstance(rec, dict):
        return dict(rec)
    dump = getattr(rec, "model_dump", None)
    if callable(dump):
        with contextlib.suppress(Exception):
            return dict(dump())
    return {
        k: getattr(rec, k)
        for k in dir(rec)
        if not k.startswith("_") and not callable(getattr(rec, k, None))
    }


def _provisioned_from_record(cluster_name: str, record: dict[str, Any]) -> ProvisionedWorker:
    handle = record.get("handle")
    launched = getattr(handle, "launched_resources", None)
    cloud_name = str(getattr(launched, "cloud", "") or "").lower()
    cloud = _CLOUD_BY_NAME.get(cloud_name)
    cost: float | None = None
    if launched is not None:
        with contextlib.suppress(Exception):
            cost = float(launched.get_cost(3600)) * int(getattr(handle, "launched_nodes", 1) or 1)
    return ProvisionedWorker(
        cluster_name=cluster_name,
        cloud=cloud,
        region=getattr(launched, "region", None),
        zone=getattr(launched, "zone", None),
        instance_type=getattr(launched, "instance_type", None),
        use_spot=bool(getattr(launched, "use_spot", False)),
        hourly_cost=cost,
        ssh_alias=cluster_name,
        metadata=_safe_record(record),
    )


def _safe_record(record: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in (
        "name",
        "launched_at",
        "status",
        "autostop",
        "to_down",
        "cluster_hash",
        "user_name",
        "workspace",
    ):
        if key in record:
            value = record[key]
            out[key] = (
                getattr(value, "name", value)
                if not isinstance(value, str | int | float | bool | None)
                else value
            )
    return out
