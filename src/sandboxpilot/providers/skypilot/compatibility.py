"""SkyPilot version detection and API-shape compatibility.

SkyPilot >= 0.8 returns request IDs from ``sky.launch``/``sky.status``/``sky.down``
that must be resolved with ``sky.get``/``sky.stream_and_get``. Older versions
returned results directly. Everything version-specific is isolated here.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import types
from dataclasses import dataclass
from typing import Any

from sandboxpilot.errors import SkyPilotError

MIN_SUPPORTED = (0, 6, 0)


def _parse_version(text: str) -> tuple[int, ...]:
    parts: list[int] = []
    for piece in text.split(".")[:3]:
        digits = ""
        for ch in piece:
            if ch.isdigit():
                digits += ch
            else:
                break
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


@dataclass
class SkyPilotCompatibility:
    module: Any
    version: str
    version_tuple: tuple[int, ...]
    request_api: bool

    @classmethod
    def detect(cls, module: Any | None = None) -> SkyPilotCompatibility:
        sky = module
        if sky is None:
            try:
                sky = importlib.import_module("sky")
            except ImportError as exc:
                raise SkyPilotError(
                    "SkyPilot is not installed.",
                    hint="Install it with: pip install 'sandboxpilot[aws,gcp,azure]'  (or 'sandboxpilot[skypilot]')",
                ) from exc
        version = getattr(sky, "__version__", None)
        if not version:
            try:
                version = importlib.metadata.version("skypilot")
            except importlib.metadata.PackageNotFoundError:
                try:
                    version = importlib.metadata.version("skypilot-nightly")
                except importlib.metadata.PackageNotFoundError:
                    version = "0.0.0"
        vt = _parse_version(str(version))
        if vt < MIN_SUPPORTED and not str(version).startswith("1.0.0.dev"):
            raise SkyPilotError(
                f"SkyPilot {version} is too old; SandboxPilot requires >= {'.'.join(map(str, MIN_SUPPORTED))}.",
                hint="Upgrade with: pip install -U skypilot",
            )
        request_api = callable(getattr(sky, "get", None)) and callable(
            getattr(sky, "stream_and_get", None)
        )
        return cls(module=sky, version=str(version), version_tuple=vt, request_api=request_api)

    def sdk(self, name: str) -> Any | None:
        """Find an SDK function by name, or None if this version lacks it.

        Recent releases keep the real functions in ``sky.client.sdk`` and only
        re-export some of them at the top level. ``sky.check`` is a trap: at the
        top level it's the *module* ``sky/check.py``, not the function.
        """
        candidate: Any = None
        client_sdk = getattr(getattr(self.module, "client", None), "sdk", None)
        if client_sdk is not None:
            candidate = getattr(client_sdk, name, None)
        if not callable(candidate) or isinstance(candidate, types.ModuleType):
            candidate = getattr(self.module, name, None)
        if callable(candidate) and not isinstance(candidate, types.ModuleType):
            return candidate
        return None

    def resolve(self, result: Any, *, stream: bool = False) -> Any:
        """Turn a SkyPilot call result into a concrete value across API generations."""
        if not self.request_api:
            return result
        if isinstance(result, str) or (
            hasattr(result, "request_id") and not isinstance(result, list | tuple | dict)
        ):
            if stream:
                return self.module.stream_and_get(result)
            return self.module.get(result)
        return result

    @property
    def supports_infra(self) -> bool:
        """``sky.Resources(infra=...)`` exists from 0.9 onward."""
        try:
            import inspect

            return "infra" in inspect.signature(self.module.Resources.__init__).parameters
        except (TypeError, ValueError, AttributeError):
            return self.version_tuple >= (0, 9, 0)

    def summary(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "request_api": self.request_api,
            "infra_param": self.supports_infra,
        }
