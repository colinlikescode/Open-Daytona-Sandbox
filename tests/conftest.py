"""Shared fixtures. Everything runs against the fake provider + fake runtime
unless a test is marked gvisor/docker/cloud_*."""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

# Isolate every test run from the developer's real config/state.
_tmp = tempfile.mkdtemp(prefix="sandboxpilot-tests-")
os.environ["SANDBOXPILOT_STATE_DIR"] = os.path.join(_tmp, "state")
os.environ["SANDBOXPILOT_CONFIG_DIR"] = os.path.join(_tmp, "config")
os.environ.pop("SANDBOXPILOT_API_URL", None)
os.environ.pop("SANDBOXPILOT_API_TOKEN", None)

from sandboxpilot.api.app import create_app  # noqa: E402
from sandboxpilot.config.models import Config  # noqa: E402
from sandboxpilot.control.factory import build_control_plane  # noqa: E402
from sandboxpilot.control.service import ControlPlane  # noqa: E402


def fake_config(**pool_overrides: Any) -> Config:
    pool: dict[str, Any] = {
        "scaling": {"min_workers": 0, "max_workers": 3, "idle_ttl": "1h"},
        "runtime": {"type": "fake"},
        "workers": {"cpus": 8, "memory": "32GB", "warm_slots": 0},
        "sandbox": {"image": "python:3.12-slim", "cpus": 1, "memory": "1GB", "timeout": "10m"},
    }
    for key, value in pool_overrides.items():
        if isinstance(value, dict) and isinstance(pool.get(key), dict):
            pool[key].update(value)
        else:
            pool[key] = value
    return Config.model_validate({"provider": {"type": "fake"}, "pools": {"default": pool}})


@pytest.fixture
async def control_plane() -> AsyncIterator[ControlPlane]:
    cp = build_control_plane(fake_config(), db_path=":memory:", run_background_loop=False)
    await cp.start()
    try:
        yield cp
    finally:
        await cp.stop()


@pytest.fixture
async def api(control_plane: ControlPlane) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(control_plane, manage_lifecycle=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


async def create_sandbox(api: httpx.AsyncClient, **body: Any) -> dict[str, Any]:
    body.setdefault("timeout", "5m")
    resp = await api.post("/v1/sandboxes", json=body)
    assert resp.status_code == 201, resp.text
    data: dict[str, Any] = resp.json()
    return data
