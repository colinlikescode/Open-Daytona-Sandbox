"""Control plane REST API on top of the fake provider (in-process ASGI)."""

from __future__ import annotations

import asyncio
from datetime import datetime

import httpx
import pytest

from sandboxpilot.api.app import create_app
from sandboxpilot.config.models import Config, ReconcileConfig
from sandboxpilot.control.factory import build_control_plane
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.common import WorkerState
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.worker.runtime.fake import FakeSandboxRuntime
from tests.conftest import create_sandbox, fake_config


async def _poll_state(api: httpx.AsyncClient, sid: str, wanted: set[str], tries: int = 100) -> str:
    state = ""
    for _ in range(tries):
        state = (await api.get(f"/v1/sandboxes/{sid}")).json()["state"]
        if state in wanted:
            break
        await asyncio.sleep(0.02)
    return state


class _Stack:
    """Control plane + ASGI client for one test with a custom config."""

    def __init__(self, cfg: Config) -> None:
        self.cp = build_control_plane(cfg, db_path=":memory:", run_background_loop=False)

    async def __aenter__(self) -> tuple[ControlPlane, httpx.AsyncClient]:
        await self.cp.start()
        app = create_app(self.cp, manage_lifecycle=False)
        self._lifespan = app.router.lifespan_context(app)
        await self._lifespan.__aenter__()
        self.api = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")
        return self.cp, self.api

    async def __aexit__(self, *exc: object) -> None:
        await self.api.aclose()
        await self._lifespan.__aexit__(None, None, None)
        await self.cp.stop()


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


async def test_default_pool_exists_implicitly_for_up() -> None:
    # Fresh install: no config-file pools, no sandboxes yet. `sandboxpilot up` must work.
    cfg = Config.model_validate({"provider": {"type": "fake"}})
    async with _Stack(cfg) as (_cp, api):
        assert (await api.get("/v1/pools")).json() == []
        r = await api.get("/v1/pools/default")
        assert r.status_code == 200 and r.json()["name"] == "default"
        r = await api.patch("/v1/pools/default", json={"cloud": {"providers": ["gcp"]}})
        assert r.json()["cloud_policy"]["providers"] == ["gcp"]
        assert (await api.get("/v1/pools/nope")).status_code == 404


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


async def test_kill_while_waiting_for_capacity_cancels_the_create() -> None:
    async with _Stack(fake_config(scaling={"min_workers": 0, "max_workers": 1})) as (_cp, api):
        await create_sandbox(api, cpus=4, memory="1GB")
        r = await api.post(
            "/v1/sandboxes",
            json={"cpus": 4, "memory": "1GB", "timeout": "5m", "wait": False, "create_timeout": 30},
        )
        assert r.status_code == 202
        sid = r.json()["id"]
        assert await _poll_state(api, sid, {"WAITING_FOR_CAPACITY"}) == "WAITING_FOR_CAPACITY"

        r = await api.delete(f"/v1/sandboxes/{sid}")
        assert r.status_code == 200, r.text
        assert r.json()["state"] == "STOPPED"
        # The placement loop must notice and stop; it must not resurrect the record.
        await asyncio.sleep(0.3)
        assert (await api.get(f"/v1/sandboxes/{sid}")).json()["state"] == "STOPPED"
        assert not [t for t in _cp._background if not t.done()]


async def test_kill_during_creating_removes_it_from_the_worker(control_plane: ControlPlane) -> None:
    fleet = control_plane.provider.fleet  # type: ignore[attr-defined]
    fleet.runtime_factory = lambda _req: FakeSandboxRuntime(create_delay=0.4)
    app = create_app(control_plane, manage_lifecycle=False)
    async with app.router.lifespan_context(app):
        api = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")
        async with api:
            r = await api.post("/v1/sandboxes", json={"timeout": "5m", "wait": False})
            sid = r.json()["id"]
            assert await _poll_state(api, sid, {"CREATING"}) == "CREATING"
            r = await api.delete(f"/v1/sandboxes/{sid}")
            assert r.json()["state"] == "STOPPED"
            await asyncio.sleep(0.8)
            assert (await api.get(f"/v1/sandboxes/{sid}")).json()["state"] == "STOPPED"
            worker = (await api.get("/v1/workers")).json()[0]
            vm = fleet.get(worker["provider_cluster"])
            assert vm is not None
            assert all(sb.state != "RUNNING" for sb in vm.service.sandboxes.values())
            assert vm.service.capacity_snapshot().sandboxes_running == 0


async def test_concurrent_creates_with_one_idempotency_key(api: httpx.AsyncClient) -> None:
    headers = {"Idempotency-Key": "race-1"}
    results = await asyncio.gather(
        *(api.post("/v1/sandboxes", json={"timeout": "5m"}, headers=headers) for _ in range(4))
    )
    ids = {r.json()["id"] for r in results}
    assert all(r.status_code in {200, 201} for r in results), [r.text for r in results]
    assert len(ids) == 1
    assert len((await api.get("/v1/sandboxes")).json()) == 1


async def test_expired_sandbox_ends_up_expired(control_plane: ControlPlane) -> None:
    app = create_app(control_plane, manage_lifecycle=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as api:
            sb = await create_sandbox(api, timeout=1)
            await asyncio.sleep(1.2)
            await control_plane.reconcile()
            info = (await api.get(f"/v1/sandboxes/{sb['id']}")).json()
            assert info["state"] == "EXPIRED"
            assert info["error"] == "sandbox timed out"
            assert info["ended_at"] is not None


async def test_reconcile_adopts_connecting_and_terminates_orphaned_workers(
    control_plane: ControlPlane,
) -> None:
    app = create_app(control_plane, manage_lifecycle=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as api:
            await create_sandbox(api)
    worker = (await control_plane.db.workers.list())[0]
    # Simulate what a control-plane restart leaves behind: a worker we know is up but
    # have not health-checked yet.
    worker.state = WorkerState.CONNECTING
    await control_plane.db.workers.upsert(worker)
    orphan = WorkerRecord(
        pool_id=worker.pool_id,
        pool_name=worker.pool_name,
        provider_cluster="sp-default-orphan",
        token="t" * 43,
    )
    await control_plane.db.workers.upsert(orphan)

    await control_plane.reconcile()

    assert (await control_plane.db.workers.get(worker.id)).state == WorkerState.HEALTHY  # type: ignore[union-attr]
    assert (await control_plane.db.workers.get(orphan.id)).state == WorkerState.TERMINATED  # type: ignore[union-attr]


async def test_idle_worker_is_scaled_down() -> None:
    async with _Stack(fake_config(scaling={"min_workers": 0, "max_workers": 2, "idle_ttl": 0})) as (
        cp,
        api,
    ):
        sb = await create_sandbox(api)
        assert len((await api.get("/v1/workers")).json()) == 1
        await cp.reconcile()
        assert len((await api.get("/v1/workers")).json()) == 1  # busy: kept
        await api.delete(f"/v1/sandboxes/{sb['id']}")
        await cp.reconcile()
        assert (await api.get("/v1/workers")).json() == []
        assert cp.provider.terminate_calls  # type: ignore[attr-defined]


async def test_vanished_worker_is_marked_lost_with_its_sandboxes() -> None:
    cfg = fake_config().model_copy(
        update={"reconcile": ReconcileConfig(health_failures_before_lost=1)}
    )
    async with _Stack(cfg) as (cp, api):
        sb = await create_sandbox(api)
        worker = (await api.get("/v1/workers")).json()[0]
        await cp.provider.fleet.disappear(worker["provider_cluster"])  # type: ignore[attr-defined]
        await cp.reconcile()
        assert (await api.get(f"/v1/workers/{worker['id']}")).json()["state"] == "LOST"
        info = (await api.get(f"/v1/sandboxes/{sb['id']}")).json()
        assert info["state"] == "LOST"
        r = await api.post(f"/v1/sandboxes/{sb['id']}/exec", json={"command": "true"})
        assert r.status_code == 410
        assert r.json()["error"]["code"] == "sandbox_lost"


async def test_pool_down_leaves_other_pools_provisioning() -> None:
    cfg = Config.model_validate(
        {
            "provider": {"type": "fake", "fake": {"provision_delay_seconds": 0.8}},
            "pools": {
                "a": {"runtime": {"type": "fake"}, "workers": {"warm_slots": 0}},
                "b": {"runtime": {"type": "fake"}, "workers": {"warm_slots": 0}},
            },
        }
    )
    async with _Stack(cfg) as (cp, api):
        ops_a = (await api.post("/v1/pools/a/up", json={"workers": 1})).json()
        await api.post("/v1/pools/b/up", json={"workers": 1})
        assert len(cp._provisioning) == 2
        r = await api.post("/v1/pools/b/down")
        assert r.json()["workers_terminated"] == 1
        # Pool a's worker is still on its way and must complete normally.
        op = (await api.get(f"/v1/operations/{ops_a[0]['id']}", params={"wait": 10})).json()
        assert op["status"] == "SUCCEEDED", op
        states = {w["pool_name"]: w["state"] for w in (await api.get("/v1/workers")).json()}
        assert states == {"a": "HEALTHY"}


async def test_default_sandbox_timeout_from_config_overrides_pool() -> None:
    cfg = fake_config()
    cfg = cfg.model_copy(
        update={"defaults": cfg.defaults.model_copy(update={"sandbox_timeout": "2m"})}
    )
    async with _Stack(cfg) as (_cp, api):
        sb = await create_sandbox(api, timeout=None)
        started = datetime.fromisoformat(sb["started_at"])
        expires = datetime.fromisoformat(sb["expires_at"])
        assert 110 <= (expires - started).total_seconds() <= 130
        explicit = await create_sandbox(api, timeout="5m")
        seconds = (
            datetime.fromisoformat(explicit["expires_at"])
            - datetime.fromisoformat(explicit["started_at"])
        ).total_seconds()
        assert 290 <= seconds <= 310


async def test_doctor_reports_structured_checks(api: httpx.AsyncClient) -> None:
    report = (await api.get("/v1/doctor")).json()
    assert report["ok"] is True
    names = {c["name"] for c in report["checks"]}
    assert {"control plane", "state", "provider", "cloud aws", "pool default"} <= names
    assert all(c["status"] in {"ok", "warn", "fail"} for c in report["checks"])
    assert report["provider"]["installed"] is True


async def test_proxy_round_trip_http(control_plane: ControlPlane) -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_line = await reader.readline()
        while (await reader.readline()) not in {b"\r\n", b""}:
            pass
        body = f"echo {request_line.decode().split()[1]}".encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    app = create_app(control_plane, manage_lifecycle=False)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://t"
            ) as api:
                sb = await create_sandbox(api)
                worker = (await api.get("/v1/workers")).json()[0]
                vm = control_plane.provider.fleet.get(worker["provider_cluster"])  # type: ignore[attr-defined]
                vm.runtime.port_targets[(sb["id"], 8080)] = ("127.0.0.1", port)
                url = (await api.post(f"/v1/sandboxes/{sb['id']}/url", json={"port": 8080})).json()[
                    "url"
                ]
                path = url.split("/v1/", 1)[1]
                r = await api.get(f"/v1/{path}hello?x=1")
                assert r.status_code == 200, r.text
                assert r.text == "echo /hello?x=1"
                # Nothing listening on another port -> clean error, not a hang.
                bad = url.replace("/8080/", "/9090/")
                r = await api.get(f"/v1/{bad.split('/v1/', 1)[1]}")
                assert r.status_code == 401  # token is bound to the port
    finally:
        server.close()
        await server.wait_closed()


async def test_templates_are_async_and_listed(api: httpx.AsyncClient) -> None:
    r = await api.post("/v1/templates", json={"name": "t1", "image": "node:22-slim"})
    assert r.status_code == 201
    names = [t["name"] for t in (await api.get("/v1/templates")).json()]
    assert "t1" in names
    assert (await api.get("/v1/templates/t1")).json()["image"] == "node:22-slim"
    assert (await api.get("/v1/templates/nope")).status_code == 404
    assert (await api.delete("/v1/templates/t1")).status_code == 204


async def test_upload_mode_is_validated(api: httpx.AsyncClient) -> None:
    sb = await create_sandbox(api)
    r = await api.put(
        f"/v1/sandboxes/{sb['id']}/files", params={"path": "/w/a", "mode": 99999}, content=b"x"
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"
    r = await api.put(
        f"/v1/sandboxes/{sb['id']}/files", params={"path": "/w/a", "mode": 0o600}, content=b"x"
    )
    assert r.status_code == 200


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
