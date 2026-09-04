"""AWS, GCP and Azure must go through SkyPilot. Nothing else.

These tests fail loudly if someone adds a direct cloud SDK dependency or
imports one from production code. The only thing allowed to talk to a cloud
is the SkyPilot adapter, and it only talks to `sky`.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from sandboxpilot.providers.skypilot.provider import _clouds_from_check_result
from sandboxpilot.schemas.common import CloudProvider

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "sandboxpilot"

# Direct cloud SDKs. If you need one of these, you are doing it wrong.
FORBIDDEN_MODULES = (
    "boto3",
    "botocore",
    "google.cloud",
    "googleapiclient",
    "google.auth",
    "azure.mgmt",
    "azure.identity",
    "azure.compute",
)
IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+([\w.]+)", re.MULTILINE)


def _production_files() -> list[Path]:
    return sorted(p for p in SRC.rglob("*.py"))


def test_no_direct_cloud_sdk_imports() -> None:
    offenders: list[str] = []
    for path in _production_files():
        for match in IMPORT_RE.finditer(path.read_text()):
            module = match.group(1)
            if any(module == f or module.startswith(f + ".") for f in FORBIDDEN_MODULES):
                offenders.append(f"{path.relative_to(ROOT)}: {module}")
    assert not offenders, "cloud SDK imported directly; use SkyPilot:\n" + "\n".join(offenders)


def test_cloud_extras_only_pull_skypilot_extras() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = data["project"]["optional-dependencies"]
    for cloud in ("aws", "gcp", "azure"):
        for dep in extras[cloud]:
            assert dep.startswith("skypilot["), (
                f"{cloud} extra pulls {dep!r}; it must be a skypilot extra"
            )


def test_every_cloud_becomes_a_skypilot_infra_string() -> None:
    from types import SimpleNamespace

    from sandboxpilot.providers.skypilot.resources import SKY_CLOUD_NAMES, resource_kwargs
    from sandboxpilot.schemas.common import V1_AUTO_CLOUDS
    from sandboxpilot.schemas.pool import CloudPolicy, WorkerPool

    assert set(V1_AUTO_CLOUDS) == {CloudProvider.AWS, CloudProvider.GCP, CloudProvider.AZURE}
    compat = SimpleNamespace(supports_infra=True)
    for cloud in V1_AUTO_CLOUDS:
        name = SKY_CLOUD_NAMES[cloud]
        pool = WorkerPool(name="p", cloud_policy=CloudPolicy(providers=[cloud]))
        assert resource_kwargs(pool, cloud, compat, {})["infra"] == name  # type: ignore[arg-type]
        pool = WorkerPool(
            name="p", cloud_policy=CloudPolicy(providers=[cloud], region="r1", zone="r1a")
        )
        assert resource_kwargs(pool, cloud, compat, {})["infra"] == f"{name}/r1/r1a"  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "result, expected",
    [
        (None, set()),
        # sky.enabled_clouds()
        (["AWS", "GCP"], {CloudProvider.AWS, CloudProvider.GCP}),
        # sky.check() on recent versions: workspace -> cloud -> capabilities
        ({"default": {"aws": ["compute", "storage"], "azure": []}}, {CloudProvider.AWS}),
        # sky.check() on older versions: cloud -> capabilities
        ({"gcp": ["compute"], "aws": ["compute"]}, {CloudProvider.GCP, CloudProvider.AWS}),
        # kubernetes / other clouds are ignored
        (["Kubernetes", "azure", "lambda"], {CloudProvider.AZURE}),
    ],
)
def test_clouds_from_check_result(result: object, expected: set[CloudProvider]) -> None:
    assert _clouds_from_check_result(result) == expected
