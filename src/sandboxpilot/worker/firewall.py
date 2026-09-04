"""Firewall rules for the sandbox network.

Rules are installed in Docker's ``DOCKER-USER`` chain and scoped to the bridge
interface of the ``sandboxpilot`` network so unrelated Docker workloads are
unaffected. They block:

* the cloud metadata endpoint (all of ``169.254.0.0/16``)
* private ranges (``10.0.0.0/8``, ``172.16.0.0/12``, ``192.168.0.0/16``)

while keeping established/related return traffic working. Only argument arrays
are used; no shell strings are constructed from runtime values.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

BLOCKED_RANGES: tuple[str, ...] = (
    "169.254.0.0/16",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
)

CHAIN = "SANDBOXPILOT"
_IFACE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,15}$")


@dataclass(frozen=True)
class FirewallPlan:
    interface: str
    rules: list[list[str]]


def bridge_interface_for_network(network_id: str) -> str:
    """Docker names bridge interfaces ``br-<first 12 chars of network id>``."""
    return f"br-{network_id[:12]}"


def build_rules(interface: str, subnet: str | None = None) -> FirewallPlan:
    """Return the iptables invocations (argument arrays) that enforce the sandbox egress policy."""
    if not _IFACE_RE.match(interface):
        raise ValueError(f"invalid interface name: {interface!r}")
    rules: list[list[str]] = [
        ["iptables", "-w", "-N", CHAIN],
        ["iptables", "-w", "-F", CHAIN],
        # Return traffic for connections sandboxes initiated is fine.
        [
            "iptables",
            "-w",
            "-A",
            CHAIN,
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "RETURN",
        ],
    ]
    for cidr in BLOCKED_RANGES:
        rules.append(
            [
                "iptables",
                "-w",
                "-A",
                CHAIN,
                "-i",
                interface,
                "-d",
                cidr,
                "-j",
                "REJECT",
                "--reject-with",
                "icmp-port-unreachable",
            ]
        )
    if subnet:
        # Sandboxes must not talk to each other either.
        rules.append(
            ["iptables", "-w", "-A", CHAIN, "-i", interface, "-o", interface, "-j", "REJECT"]
        )
    rules.append(["iptables", "-w", "-A", CHAIN, "-j", "RETURN"])
    # Hook into DOCKER-USER exactly once (check first, then insert).
    rules.append(["iptables", "-w", "-C", "DOCKER-USER", "-i", interface, "-j", CHAIN])
    rules.append(["iptables", "-w", "-I", "DOCKER-USER", "1", "-i", interface, "-j", CHAIN])
    return FirewallPlan(interface=interface, rules=rules)


async def _run(argv: list[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode(errors="replace")


async def apply_rules(plan: FirewallPlan) -> list[str]:
    """Apply the plan idempotently. Returns warnings (non-fatal failures)."""
    warnings: list[str] = []
    for argv in plan.rules:
        code, output = await _run(argv)
        if argv[2] == "-N" and code != 0:
            continue  # chain already exists
        if argv[2] == "-C":
            if code == 0:
                break  # hook already present; skip insert
            continue
        if code != 0:
            warnings.append(f"{' '.join(argv)} failed: {output.strip()}")
    return warnings


async def verify_rules(interface: str) -> bool:
    code, output = await _run(["iptables", "-w", "-S", CHAIN])
    if code != 0:
        return False
    return all(cidr in output for cidr in BLOCKED_RANGES) and interface in output
