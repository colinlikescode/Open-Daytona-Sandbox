"""Run the control plane as a local daemon.

The SDK and CLI call :func:`ensure_running` which returns quickly if a control
plane already answers on the configured URL, and otherwise spawns one in the
background (detached, logging to the state dir) and waits for it to be healthy.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

from sandboxpilot.config.models import Config
from sandboxpilot.errors import ConfigurationError
from sandboxpilot.utils.logging import get_logger
from sandboxpilot.utils.paths import daemon_log_file, daemon_pid_file, ensure_private_dir

log = get_logger("control.daemon")

DEFAULT_START_TIMEOUT = 20.0


def is_healthy(url: str, timeout: float = 1.0) -> bool:
    try:
        r = httpx.get(f"{url.rstrip('/')}/v1/health", timeout=timeout)
        return r.status_code == 200 and r.json().get("status") == "ok"
    except (httpx.HTTPError, ValueError):
        return False


def read_pid() -> int | None:
    try:
        return int(daemon_pid_file().read_text().strip())
    except (OSError, ValueError):
        return None


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def spawn_detached(extra_args: list[str] | None = None, env: dict[str, str] | None = None) -> int:
    """Start ``sandboxpilot daemon run`` as a detached background process."""
    ensure_private_dir(daemon_pid_file().parent)
    ensure_private_dir(daemon_log_file().parent)
    logfile = open(daemon_log_file(), "ab")  # noqa: SIM115 - handed to the child
    argv = [sys.executable, "-m", "sandboxpilot.cli.main", "daemon", "run", *(extra_args or [])]
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=logfile,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
        env={**os.environ, **(env or {})},
    )
    logfile.close()
    return proc.pid


def ensure_running(
    config: Config,
    *,
    autostart: bool = True,
    timeout: float = DEFAULT_START_TIMEOUT,
    extra_args: list[str] | None = None,
) -> str:
    """Return the API URL of a healthy control plane, starting one if needed."""
    url = config.api.url
    if is_healthy(url):
        return url
    if not autostart:
        raise ConfigurationError(
            f"No control plane is running at {url}.",
            hint="Start one with: sandboxpilot daemon start",
        )
    if not config.api.is_loopback:
        raise ConfigurationError(
            f"No control plane is answering at {url} and it is not a local address, so it cannot be started automatically.",
            hint="Start the control plane on that host, or point SANDBOXPILOT_API_URL at a local one.",
        )
    pid = read_pid()
    if pid is None or not pid_alive(pid):
        pid = spawn_detached(extra_args)
        log.info("started control plane daemon (pid %s)", pid)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_healthy(url):
            return url
        if not pid_alive(pid):
            break
        time.sleep(0.2)
    raise ConfigurationError(
        f"The control plane did not become healthy at {url} within {timeout:.0f}s.",
        hint=f"See the daemon log: {daemon_log_file()}",
    )


def stop(timeout: float = 10.0) -> bool:
    pid = read_pid()
    if pid is None or not pid_alive(pid):
        _remove_pid_file()
        return False
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and pid_alive(pid):
        time.sleep(0.1)
    if pid_alive(pid):
        os.kill(pid, signal.SIGKILL)
    _remove_pid_file()
    return True


def _remove_pid_file() -> None:
    with contextlib.suppress(OSError):
        daemon_pid_file().unlink()


async def serve(config: Config, *, db_path: Path | str | None = None) -> None:
    """Run the API server in the foreground until SIGTERM/SIGINT."""
    import uvicorn

    from sandboxpilot.api.app import create_app
    from sandboxpilot.control.factory import build_control_plane

    ensure_private_dir(daemon_pid_file().parent)
    daemon_pid_file().write_text(str(os.getpid()))
    try:
        cp = build_control_plane(config, db_path=db_path)
        app = create_app(cp)
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=config.api.host,
                port=config.api.port,
                log_level=config.logging.level.lower(),
                access_log=False,
                lifespan="on",
            )
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, server.handle_exit, sig, None)
        await server.serve()
    finally:
        if read_pid() == os.getpid():
            _remove_pid_file()
