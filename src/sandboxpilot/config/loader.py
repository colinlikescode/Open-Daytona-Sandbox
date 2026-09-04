"""Load configuration with the documented precedence.

Highest to lowest:

1. CLI arguments (applied by the CLI on top of the returned ``Config``)
2. SDK explicit parameters (applied by the SDK)
3. environment variables (``SANDBOXPILOT_*``)
4. pool configuration (``pools.<name>`` in the config file)
5. global configuration (the rest of the config file)
6. built-in defaults
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from sandboxpilot.config.models import Config
from sandboxpilot.errors import ConfigurationError
from sandboxpilot.utils.paths import config_file

ENV_PREFIX = "SANDBOXPILOT_"


def load_config_file(path: Path | None = None) -> dict[str, Any]:
    target = path or config_file()
    if not target.exists():
        return {}
    try:
        with target.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Could not parse {target}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigurationError(f"{target} must contain a mapping at the top level")
    return data


def _apply_env(data: dict[str, Any], env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env if env is not None else dict(os.environ)
    api = data.setdefault("api", {})
    if env.get(f"{ENV_PREFIX}API_TOKEN"):
        api["token"] = env[f"{ENV_PREFIX}API_TOKEN"]
    if env.get(f"{ENV_PREFIX}API_HOST"):
        api["host"] = env[f"{ENV_PREFIX}API_HOST"]
    if env.get(f"{ENV_PREFIX}API_PORT"):
        try:
            api["port"] = int(env[f"{ENV_PREFIX}API_PORT"])
        except ValueError as exc:
            raise ConfigurationError(f"{ENV_PREFIX}API_PORT must be an integer") from exc
    if env.get(f"{ENV_PREFIX}EXTERNAL_URL"):
        api["external_url"] = env[f"{ENV_PREFIX}EXTERNAL_URL"]
    defaults = data.setdefault("defaults", {})
    if env.get(f"{ENV_PREFIX}DEFAULT_POOL"):
        defaults["pool"] = env[f"{ENV_PREFIX}DEFAULT_POOL"]
    if env.get(f"{ENV_PREFIX}SANDBOX_TIMEOUT"):
        defaults["sandbox_timeout"] = env[f"{ENV_PREFIX}SANDBOX_TIMEOUT"]
    provider = data.setdefault("provider", {})
    if env.get(f"{ENV_PREFIX}PROVIDER"):
        provider["type"] = env[f"{ENV_PREFIX}PROVIDER"]
    logging_cfg = data.setdefault("logging", {})
    if env.get(f"{ENV_PREFIX}LOG_LEVEL"):
        logging_cfg["level"] = env[f"{ENV_PREFIX}LOG_LEVEL"]
    if env.get(f"{ENV_PREFIX}LOG_FORMAT"):
        logging_cfg["format"] = env[f"{ENV_PREFIX}LOG_FORMAT"]
    return data


def load_config(
    path: Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
) -> Config:
    data = load_config_file(path)
    data = _apply_env(data, env)
    if overrides:
        _deep_merge(data, overrides)
    try:
        return Config.model_validate(data)
    except PydanticValidationError as exc:
        raise ConfigurationError(f"Invalid configuration: {exc}") from exc


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> None:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        elif value is not None:
            base[key] = value


def api_url_from_env(default: str) -> str:
    return os.environ.get(f"{ENV_PREFIX}API_URL", default).rstrip("/")


def api_token_from_env() -> str | None:
    return os.environ.get(f"{ENV_PREFIX}API_TOKEN") or None
