"""XDG-aware paths for state, config and runtime files."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

APP_NAME = "sandboxpilot"


def _xdg(var: str, default: Path) -> Path:
    value = os.environ.get(var)
    return Path(value).expanduser() if value else default


def state_dir() -> Path:
    override = os.environ.get("SANDBOXPILOT_STATE_DIR")
    if override:
        return Path(override).expanduser()
    return _xdg("XDG_DATA_HOME", Path.home() / ".local" / "share") / APP_NAME


def config_dir() -> Path:
    override = os.environ.get("SANDBOXPILOT_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    return _xdg("XDG_CONFIG_HOME", Path.home() / ".config") / APP_NAME


def config_file() -> Path:
    override = os.environ.get("SANDBOXPILOT_CONFIG")
    if override:
        return Path(override).expanduser()
    return config_dir() / "config.yaml"


def runtime_dir() -> Path:
    override = os.environ.get("SANDBOXPILOT_RUNTIME_DIR")
    if override:
        return Path(override).expanduser()
    return state_dir() / "run"


def state_db_path() -> Path:
    return state_dir() / "state.db"


def daemon_pid_file() -> Path:
    return runtime_dir() / "daemon.pid"


def daemon_log_file() -> Path:
    return state_dir() / "daemon.log"


def templates_dir() -> Path:
    return config_dir() / "templates"


def ensure_private_dir(path: Path) -> Path:
    """Create ``path`` with user-only permissions (0700) where supported."""
    path.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):  # platform specific
        os.chmod(path, 0o700)
    return path


def ensure_private_file(path: Path) -> None:
    """Restrict ``path`` to user-only permissions (0600) where supported."""
    with contextlib.suppress(OSError):  # platform specific
        os.chmod(path, 0o600)
