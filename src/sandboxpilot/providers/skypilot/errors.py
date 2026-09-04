"""Classify SkyPilot exceptions into actionable SandboxPilot errors."""

from __future__ import annotations

from sandboxpilot.errors import ProviderError, SkyPilotError, WorkerProvisionError
from sandboxpilot.schemas.pool import CloudPolicy


def classify(
    exc: BaseException, policy: CloudPolicy | None = None, *, action: str = "provision a worker"
) -> ProviderError:
    name = type(exc).__name__
    text = str(exc)
    lowered = text.lower()
    clouds = ", ".join(p.value for p in policy.providers) if policy else "the configured clouds"
    alternatives = _alternatives(policy)

    if (
        name in {"ResourcesUnavailableError"}
        or "resources unavailable" in lowered
        or "no resource" in lowered
    ):
        if "quota" in lowered:
            return WorkerProvisionError(
                f"SandboxPilot could not {action}.\n\nSkyPilot reports a cloud quota limit on {clouds}.",
                hint=f"Request a quota increase or use a smaller worker / different cloud:\n\n    {alternatives}",
                details={"skypilot_error": name, "message": text[:2000]},
                cause=exc,
            )
        return WorkerProvisionError(
            f"SandboxPilot could not {action}.\n\nSkyPilot found no available capacity on {clouds} for the requested worker size.",
            hint=f"Try another region, a different cloud or a smaller worker:\n\n    {alternatives}",
            details={"skypilot_error": name, "message": text[:2000]},
            cause=exc,
        )
    if (
        "credential" in lowered
        or "not enabled" in lowered
        or "no cloud" in lowered
        or "sky check" in lowered
        or name in {"CloudUserIdentityError", "NoCloudAccessError"}
    ):
        return WorkerProvisionError(
            f"SandboxPilot could not {action}.\n\nSkyPilot reports that credentials for {clouds} are unavailable.",
            hint=f"Try:\n\n    sky check\n\nor configure another provider:\n\n    {alternatives}",
            details={"skypilot_error": name, "message": text[:2000]},
            cause=exc,
        )
    if "quota" in lowered:
        return WorkerProvisionError(
            f"SandboxPilot could not {action}.\n\nSkyPilot reports a cloud quota error on {clouds}.",
            hint="Request a quota increase in the cloud console or choose a different instance type/cloud.",
            details={"skypilot_error": name, "message": text[:2000]},
            cause=exc,
        )
    if name in {"ClusterDoesNotExist", "ClusterNotUpError"}:
        return SkyPilotError(
            f"SkyPilot cluster does not exist or is not up: {text}",
            details={"skypilot_error": name},
            cause=exc,
        )
    if "timed out" in lowered or "timeout" in lowered or name == "CommandError":
        return WorkerProvisionError(
            f"SandboxPilot could not {action}.\n\nWorker bootstrap on the cloud VM failed or timed out.",
            hint="Inspect the SkyPilot logs for the cluster (sky logs <cluster>) and re-run `sandboxpilot pool up`.",
            details={"skypilot_error": name, "message": text[:2000]},
            cause=exc,
        )
    return SkyPilotError(
        f"SandboxPilot could not {action}.\n\nSkyPilot error ({name}): {text[:500]}",
        hint="Run with --verbose for the full SkyPilot traceback.",
        details={"skypilot_error": name, "message": text[:2000]},
        cause=exc,
    )


def _alternatives(policy: CloudPolicy | None) -> str:
    current = set(policy.providers) if policy else set()
    from sandboxpilot.schemas.common import CloudProvider

    others = [c for c in CloudProvider if c not in current] or list(CloudProvider)
    return "\n    ".join(f"sandboxpilot pool create <name> --cloud {c.value}" for c in others[:2])
