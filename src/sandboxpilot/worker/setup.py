"""Worker host setup: sandbox network and firewall rules (run as root on the worker).

Invoked by bootstrap and by the systemd unit's ``ExecStartPre`` so firewall
rules are re-applied after reboots.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from sandboxpilot.worker import firewall
from sandboxpilot.worker.config import WorkerConfig


async def _run(network: bool, apply_firewall: bool, doctor: bool) -> int:
    config = WorkerConfig()
    if config.runtime == "fake":
        print("[sandboxpilot/setup] fake runtime: nothing to configure")
        return 0
    from sandboxpilot.worker.runtime.docker_gvisor import GVisorDockerRuntime

    runtime = GVisorDockerRuntime(
        network_name=config.network_name,
        network_subnet=config.network_subnet,
        docker_host=config.docker_host,
        unsafe_runc=config.runtime == "docker-unsafe",
    )
    rc = 0
    try:
        if network or apply_firewall:
            network_id = await runtime.network_id()
            print(
                f"[sandboxpilot/setup] sandbox network '{config.network_name}' ready ({network_id[:12]})"
            )
            if apply_firewall:
                iface = firewall.bridge_interface_for_network(network_id)
                plan = firewall.build_rules(iface, config.network_subnet)
                warnings = await firewall.apply_rules(plan)
                for w in warnings:
                    print(f"[sandboxpilot/setup] WARNING: {w}", file=sys.stderr)
                if not await firewall.verify_rules(iface):
                    print(
                        "[sandboxpilot/setup] ERROR: firewall rules could not be verified",
                        file=sys.stderr,
                    )
                    rc = 1
                else:
                    print(f"[sandboxpilot/setup] firewall rules active on {iface}")
        if doctor:
            result = await runtime.doctor()
            for name, ok in result.checks.items():
                print(f"[sandboxpilot/setup] check {name}: {'ok' if ok else 'FAILED'}")
            for err in result.errors:
                print(f"[sandboxpilot/setup] ERROR: {err}", file=sys.stderr)
            if not result.ok:
                rc = 1
    finally:
        await runtime.close()
    return rc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m sandboxpilot.worker.setup")
    parser.add_argument(
        "--network", action="store_true", help="ensure the sandbox Docker network exists"
    )
    parser.add_argument("--firewall", action="store_true", help="apply egress firewall rules")
    parser.add_argument(
        "--doctor", action="store_true", help="run the runtime doctor (incl. gVisor smoke test)"
    )
    args = parser.parse_args(argv)
    if os.geteuid() != 0 and (args.firewall or args.network):
        print("[sandboxpilot/setup] must run as root", file=sys.stderr)
        return 2
    return asyncio.run(_run(args.network, args.firewall, args.doctor))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
