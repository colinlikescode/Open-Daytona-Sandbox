"""SDK (sync + async) and CLI against a real uvicorn server on a random port,
fake provider underneath. This is the closest thing to a user's first run
that does not need a cloud account."""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn
import yaml

from sandboxpilot import AsyncSandbox, AsyncSandboxPilot, CommandError, Sandbox, SandboxPilot
from sandboxpilot.api.app import create_app
from sandboxpilot.control.factory import build_control_plane
from tests.conftest import fake_config


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def server_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    port = _free_port()
    cfg = fake_config(workers={"warm_slots": 1})
    cfg = cfg.model_copy(update={"api": cfg.api.model_copy(update={"port": port})})
    db = tmp_path_factory.mktemp("db") / "state.db"
    cp = build_control_plane(cfg, db_path=db)
    app = create_app(cp)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    import httpx

    for _ in range(100):
        try:
            if httpx.get(f"{url}/v1/health", timeout=0.5).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        import time

        time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield url
    server.should_exit = True
    thread.join(timeout=10)


def test_sync_sdk_round_trip(server_url: str, tmp_path: Path) -> None:
    with SandboxPilot(server_url, autostart=False) as client:
        with Sandbox.create(env={"NAME": "pilot"}, timeout="5m", client=client) as sb:
            assert sb.info.state.value == "RUNNING"
            assert sb.run("echo hello $NAME").stdout.strip() == "hello pilot"
            with pytest.raises(CommandError):
                sb.run("exit 2", check=True)

            sb.write("/workspace/a.txt", "content")
            assert sb.read_text("/workspace/a.txt") == "content"
            local = tmp_path / "in.txt"
            local.write_text("upload me")
            sb.upload(local, "/workspace/in.txt")
            out = sb.download("/workspace/in.txt", tmp_path / "out.txt")
            assert out.read_text() == "upload me"

            events = list(sb.stream("echo a; echo b >&2; exit 4"))
            assert [e.type for e in events][-1] == "exit"
            assert events[-1].exit_code == 4

            cmd = sb.run_background("sleep 30")
            cmd.kill()
            assert cmd.wait().status.value == "KILLED"

            assert "/v1/proxy/" in sb.get_url(8080)
            sid = sb.id
        assert client.get_sandbox_info(sid).state.value == "STOPPED"
        assert client.list_workers()


def test_warm_slot_claimed_via_sdk(server_url: str) -> None:
    with SandboxPilot(server_url, autostart=False) as client:
        # First create provisions the worker; the worker then pre-boots a warm slot.
        with Sandbox.create(timeout="5m", client=client):
            pass
        import time

        hit = False
        for _ in range(20):
            with Sandbox.create(timeout="5m", client=client) as sb:
                hit = bool(sb.info.metrics.get("warm_slot"))
            if hit:
                break
            time.sleep(0.1)
        assert hit, "expected a create to be served from a warm slot"


async def test_async_sdk(server_url: str) -> None:
    async with AsyncSandboxPilot(server_url, autostart=False) as client:
        async with await AsyncSandbox.create(timeout="5m", client=client) as sb:
            result = await sb.run(["sh", "-c", "echo async"])
            assert result.stdout.strip() == "async"
            events = [ev async for ev in sb.stream("echo x")]
            assert events[-1].type == "exit"
            results = await asyncio.gather(*(sb.run(f"echo {i}") for i in range(5)))
            assert sorted(r.stdout.strip() for r in results) == [str(i) for i in range(5)]
        assert (await client.get_sandbox_info(sb.id)).state.value == "STOPPED"


def test_cli_end_to_end(server_url: str, tmp_path: Path) -> None:
    env = {
        **os.environ,
        "SANDBOXPILOT_API_URL": server_url,
        "SANDBOXPILOT_CONFIG_DIR": str(tmp_path / "cfg"),
        "SANDBOXPILOT_STATE_DIR": str(tmp_path / "state"),
        "NO_COLOR": "1",
        "TERM": "dumb",
    }

    def cli(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            [sys.executable, "-m", "sandboxpilot.cli.main", *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )
        if check:
            assert proc.returncode == 0, proc.stderr
        return proc

    sid = cli("sandbox", "create", "-e", "X=1", "--timeout", "5m").stdout.strip()
    assert sid.startswith("sbx_")
    run = cli("sandbox", "exec", sid, "--", "sh", "-c", "echo got $X; exit 3", check=False)
    assert run.returncode == 3 and "got 1" in run.stdout
    src = tmp_path / "f.txt"
    src.write_text("file body")
    cli("sandbox", "upload", sid, str(src), "/workspace/f.txt")
    assert "file body" in cli("sandbox", "exec", sid, "--", "cat", "/workspace/f.txt").stdout
    assert sid[4:12] in cli("sandbox", "list").stdout
    assert '"state": "RUNNING"' in cli("--json", "sandbox", "get", sid).stdout
    cli("sandbox", "kill", sid)
    assert '"state": "STOPPED"' in cli("--json", "sandbox", "get", sid).stdout
    assert "default" in cli("pool", "list").stdout
    assert "HEALTHY" in cli("worker", "list").stdout
    assert cli("status").returncode == 0
    assert cli("doctor", check=False).returncode in {0, 1}

    cfg = yaml.safe_load(cli("--json", "pool", "get", "default").stdout)
    assert cfg["name"] == "default"
