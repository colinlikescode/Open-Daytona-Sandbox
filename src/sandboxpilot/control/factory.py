"""Wire a ControlPlane from a Config: pick the provider and tunnel manager.

``provider.type = skypilot`` (default) is the real thing. ``fake`` runs
in-process workers on a fake runtime and is what the test-suite and
``sandboxpilot --fake`` use.
"""

from __future__ import annotations

from pathlib import Path

from sandboxpilot.api.metrics import Metrics
from sandboxpilot.config.models import Config
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.control.tunnels import FakeTunnelManager, SSHTunnelManager, TunnelManager
from sandboxpilot.errors import ConfigurationError
from sandboxpilot.providers.base import ComputeProvider
from sandboxpilot.state.db import Database
from sandboxpilot.utils.clock import Clock
from sandboxpilot.utils.paths import state_db_path


def build_provider_and_tunnels(
    config: Config, *, clock: Clock | None = None
) -> tuple[ComputeProvider, TunnelManager]:
    kind = config.provider.type
    if kind == "skypilot":
        from sandboxpilot.providers.skypilot import SkyPilotComputeProvider

        return SkyPilotComputeProvider(), SSHTunnelManager()
    if kind == "fake":
        from sandboxpilot.providers.fake import FakeComputeProvider, FakeProviderBehavior

        opts = dict(config.provider.fake)
        behavior = FakeProviderBehavior(
            provision_delay_seconds=float(opts.get("provision_delay_seconds", 0.0)),
            worker_cpu_millis=opts.get("worker_cpu_millis"),
            worker_memory_bytes=opts.get("worker_memory_bytes"),
        )
        provider = FakeComputeProvider(clock=clock, behavior=behavior)
        return provider, FakeTunnelManager(provider.fleet)
    raise ConfigurationError(
        f"unknown provider type {kind!r}", hint="provider.type must be 'skypilot' or 'fake'."
    )


def build_control_plane(
    config: Config,
    *,
    db_path: Path | str | None = None,
    clock: Clock | None = None,
    metrics: Metrics | None = None,
    run_background_loop: bool = True,
) -> ControlPlane:
    provider, tunnels = build_provider_and_tunnels(config, clock=clock)
    db = Database(db_path if db_path is not None else state_db_path())
    return ControlPlane(
        config,
        db,
        provider,
        tunnels,
        clock=clock,
        metrics=metrics,
        run_background_loop=run_background_loop,
    )
