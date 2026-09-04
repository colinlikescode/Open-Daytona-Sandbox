"""Control plane REST API on top of the fake provider (in-process ASGI)."""

from __future__ import annotations

import httpx
import pytest

from sandboxpilot.api.app import create_app
from sandboxpilot.control.factory import build_control_plane
from tests.conftest import create_sandbox, fake_config


async def test_health_and_status(api: httpx.AsyncClient) -> None:
    assert (await api.get("/v1/health")).json()["status"] == "ok"
    st = (await api.get("/v1/status")).json()
    assert st["provider"] == "fake"
    assert st["pools"][0]["name"] == "default"


async def test_sandbox_lifecycle_scales_from_zero(api: httpx.AsyncClient) -> None:
    sb = await create_sandbox(api, env={"GREETING": "hi"})
    assert sb["state"] == "RUNNING"
    assert sb["worker_id"]
    assert sb["env_keys"] == ["GREETING"]
    assert "GREETING" not in str(sb.get("env", ""))  # env values never leave the API

    r = await api.post(f"/v1/sandboxes/{sb['id']}/exec", json={"command": "echo $GREETING"})
    assert r.status_code == 200
    assert r.json()["stdout"].strip() == "hi"

    # Parent directory does not exist yet; write must create it.
    r = await api.put(
        f"/v1/sandboxes/{sb['id']}/files", params={"path": "/work/new/a.txt"}, content=b"abc"
    )
    assert r.status_code == 200
    r = await api.get(f"/v1/sandboxes/{sb['id']}/files", params={"path": "/work/new/a.txt"})
    assert r.content == b"abc"

    r = await api.post(f"/v1/sandboxes/{sb['id']}/timeout", json={"timeout": "2h"})
    assert r.status_code == 200

    r = await api.delete(f"/v1/sandboxes/{sb['id']}")
    assert r.json()["state"] == "STOPPED"
    r = await api.delete(f"/v1/sandboxes/{sb['id']}")  # idempotent
    assert r.status_code == 200

    workers = (await api.get("/v1/workers")).json()
    assert len(workers) == 1 and workers[0]["state"] == "HEALTHY"


async def test_background_command_stream_and_kill(api: httpx.AsyncClient) -> None:
    sb = await create_sandbox(api)
    r = await api.post(
        f"/v1/sandboxes/{sb['id']}/exec/start", json={"command": "echo one; sleep 0.1; echo two"}
    )
    assert r.status_code == 202
    cid = r.json()["command_id"]
    seen: list[str] = []
    async with api.stream("GET", f"/v1/sandboxes/{sb['id']}/exec/{cid}/stream") as resp:
        async for line in resp.aiter_lines():
            if line.startswith("data:"):
                seen.append(line)
    assert any("one" in s for s in seen) and any('"exit"' in s for s in seen)
    result = (await api.get(f"/v1/sandboxes/{sb['id']}/exec/{cid}/result")).json()
    assert result["exit_code"] == 0

    r = await api.post(f"/v1/sandboxes/{sb['id']}/exec/start", json={"command": "sleep 30"})
    cid = r.json()["command_id"]
    r = await api.delete(f"/v1/sandboxes/{sb['id']}/exec/{cid}")
    assert r.json()["status"] in {"KILLED", "RUNNING"}
    result = (
        await api.get(f"/v1/sandboxes/{sb['id']}/exec/{cid}/result", params={"wait": "true"})
    ).json()
    assert result["status"] == "KILLED"


async def test_idempotency_key_returns_same_sandbox(api: httpx.AsyncClient) -> None:
    headers = {"Idempotency-Key": "abc-123"}
    a = await api.post("/v1/sandboxes", json={"timeout": "5m"}, headers=headers)
    b = await api.post("/v1/sandboxes", json={"timeout": "5m"}, headers=headers)
    assert a.json()["id"] == b.json()["id"]
    assert len([s for s in (await api.get("/v1/sandboxes")).json()]) == 1


async def test_binpacking_and_scale_out(api: httpx.AsyncClient) -> None:
    # Worker has 8 cpus (1 reserved). Four 2-cpu sandboxes fit on one worker...
    boxes = [await create_sandbox(api, cpus=2, memory="1GB") for _ in range(3)]
    assert len({b["worker_id"] for b in boxes}) == 1
    # ...the fifth needs a second worker.
    more = [await create_sandbox(api, cpus=2, memory="1GB") for _ in range(2)]
    assert len({b["worker_id"] for b in boxes + more}) == 2


async def test_no_capacity_at_max_workers() -> None:
    cp = build_control_plane(
        fake_config(scaling={"min_workers": 0, "max_workers": 1}),
        db_path=":memory:",
        run_background_loop=False,
    )
    await cp.start()
    app = create_app(cp, manage_lifecycle=False)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as api:
            await create_sandbox(api, cpus=4, memory="1GB")
            r = await api.post(
                "/v1/sandboxes",
                json={"cpus": 4, "memory": "1GB", "timeout": "5m", "create_timeout": 1},
            )
            assert r.status_code in {503, 504}
            assert r.json()["error"]["code"] in {"no_capacity", "create_timeout"}
    finally:
        await cp.stop()


async def test_validation_errors_are_structured(api: httpx.AsyncClient) -> None:
    r = await api.post("/v1/sandboxes", json={"memory": "lots", "timeout": "5m"})
    assert r.status_code in {400, 422}
    assert "error" in r.json()
    r = await api.get("/v1/sandboxes/sbx_does-not-exist")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "sandbox_not_found"


async def test_pool_crud_and_up_down(api: httpx.AsyncClient) -> None:
    r = await api.post(
        "/v1/pools", json={"name": "gpu", "max_workers": 2, "runtime": "fake", "warm_slots": 0}
    )
    assert r.status_code == 201
    r = await api.patch("/v1/pools/gpu", json={"min_workers": 1})
    assert r.json()["min_workers"] == 1
    ops = (await api.post("/v1/pools/gpu/up", json={"workers": 1})).json()
    assert ops
    op = (await api.get(f"/v1/operations/{ops[0]['id']}", params={"wait": 10})).json()
    assert op["status"] == "SUCCEEDED"
    r = await api.post("/v1/pools/gpu/down")
    assert r.json()["workers_terminated"] == 1
    r = await api.delete("/v1/pools/gpu")
    assert r.status_code == 204
    r = await api.delete("/v1/pools/default")
    assert r.status_code in {204, 409}


async def test_templates(api: httpx.AsyncClient) -> None:
    r = await api.post(
        "/v1/templates", json={"name": "py", "image": "python:3.12-slim", "env": {"A": "1"}}
    )
    assert r.status_code == 201
    sb = await create_sandbox(api, template="py")
    assert sb["env_keys"] == ["A"]
    assert (await api.delete("/v1/templates/py")).status_code == 204


async def test_proxy_url_is_signed_and_scoped(api: httpx.AsyncClient) -> None:
    sb = await create_sandbox(api)
    url = (await api.post(f"/v1/sandboxes/{sb['id']}/url", json={"port": 8000})).json()["url"]
    assert f"/v1/proxy/{sb['id']}/8000/" in url
    tampered = url.replace("/8000/", "/22/")
    r = await api.get(tampered.replace("http://127.0.0.1:7070", "http://test"))
    assert r.status_code in {401, 403}


async def test_cleanup_and_metrics(api: httpx.AsyncClient) -> None:
    await create_sandbox(api)
    r = await api.post("/v1/cleanup", params={"terminate_workers": "true"})
    assert r.json()["workers_terminated"] >= 1
    assert (await api.get("/v1/sandboxes")).json() == []
    text = (await api.get("/v1/metrics")).text
    assert "sandboxpilot_" in text


@pytest.mark.parametrize("path", ["/v1/status", "/v1/sandboxes", "/v1/pools"])
async def test_token_required_when_configured(path: str) -> None:
    cfg = fake_config()
    cfg = cfg.model_copy(update={"api": cfg.api.model_copy(update={"token": "s3cret"})})
    cp = build_control_plane(cfg, db_path=":memory:", run_background_loop=False)
    await cp.start()
    app = create_app(cp, manage_lifecycle=False)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as api:
            assert (await api.get(path)).status_code == 401
            assert (
                await api.get(path, headers={"Authorization": "Bearer wrong"})
            ).status_code == 401
            assert (
                await api.get(path, headers={"Authorization": "Bearer s3cret"})
            ).status_code == 200
            assert (await api.get("/v1/health")).status_code == 200  # health stays open
    finally:
        await cp.stop()
