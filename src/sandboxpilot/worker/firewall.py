"""Firewall rules for the sandbox network.

Two chains, both scoped to the bridge interface of the ``sandboxpilot`` network so
unrelated Docker workloads are unaffected:

* ``SANDBOXPILOT`` hooks into ``DOCKER-USER`` (forwarded traffic) and rejects
  sandbox egress to the cloud metadata endpoint (all of ``169.254.0.0/16``), the
  RFC 1918 private ranges and the carrier-grade NAT range ``100.64.0.0/10`` that
  some clouds use for internal services. Sandboxes cannot talk to each other.
* ``SANDBOXPILOT-INPUT`` hooks into ``INPUT`` and rejects everything a sandbox
  sends to the worker VM itself (sshd, the worker API, Docker), which the
  forward chain never sees.

Return traffic for connections the host or a sandbox opened stays allowed. Only
argument arrays are used; no shell strings are built from runtime values.
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
    "100.64.0.0/10",
)

CHAIN = "SANDBOXPILOT"
INPUT_CHAIN = "SANDBOXPILOT-INPUT"
_IFACE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,15}$")
_RETURN_ESTABLISHED = ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"]
_REJECT = ["-j", "REJECT", "--reject-with", "icmp-port-unreachable"]


@dataclass(frozen=True)
class FirewallPlan:
    interface: str
    rules: list[list[str]]


def bridge_interface_for_network(network_id: str) -> str:
    """Docker names bridge interfaces ``br-<first 12 chars of network id>``."""
    return f"br-{network_id[:12]}"


def _hook(parent: str, interface: str, chain: str) -> list[list[str]]:
    """Check-then-insert so the jump into ``chain`` exists exactly once."""
    return [
        ["iptables", "-w", "-C", parent, "-i", interface, "-j", chain],
        ["iptables", "-w", "-I", parent, "1", "-i", interface, "-j", chain],
    ]


def build_rules(interface: str, subnet: str | None = None) -> FirewallPlan:
    """Return the iptables invocations (argument arrays) that enforce the sandbox egress policy."""
    if not _IFACE_RE.match(interface):
        raise ValueError(f"invalid interface name: {interface!r}")
    rules: list[list[str]] = [
        ["iptables", "-w", "-N", CHAIN],
        ["iptables", "-w", "-F", CHAIN],
        ["iptables", "-w", "-A", CHAIN, *_RETURN_ESTABLISHED],
    ]
    for cidr in BLOCKED_RANGES:
        rules.append(["iptables", "-w", "-A", CHAIN, "-i", interface, "-d", cidr, *_REJECT])
    if subnet:
        # Sandboxes must not talk to each other either.
        rules.append(
            ["iptables", "-w", "-A", CHAIN, "-i", interface, "-o", interface, "-j", "REJECT"]
        )
    rules.append(["iptables", "-w", "-A", CHAIN, "-j", "RETURN"])
    rules += _hook("DOCKER-USER", interface, CHAIN)
    # Traffic addressed to the VM itself goes through INPUT, not DOCKER-USER.
    rules += [
        ["iptables", "-w", "-N", INPUT_CHAIN],
        ["iptables", "-w", "-F", INPUT_CHAIN],
        ["iptables", "-w", "-A", INPUT_CHAIN, *_RETURN_ESTABLISHED],
        ["iptables", "-w", "-A", INPUT_CHAIN, "-i", interface, *_REJECT],
        ["iptables", "-w", "-A", INPUT_CHAIN, "-j", "RETURN"],
    ]
    rules += _hook("INPUT", interface, INPUT_CHAIN)
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
    skip_next = False
    for argv in plan.rules:
        if skip_next:
            skip_next = False
            continue
        code, output = await _run(argv)
        if argv[2] == "-N" and code != 0:
            continue  # chain already exists
        if argv[2] == "-C":
            skip_next = code == 0  # hook already present; skip the insert that follows
            continue
        if code != 0:
            warnings.append(f"{' '.join(argv)} failed: {output.strip()}")
    return warnings


async def verify_rules(interface: str) -> bool:
    code, forward = await _run(["iptables", "-w", "-S", CHAIN])
    if code != 0:
        return False
    code, inbound = await _run(["iptables", "-w", "-S", INPUT_CHAIN])
    if code != 0:
        return False
    return (
        all(cidr in forward for cidr in BLOCKED_RANGES)
        and interface in forward
        and interface in inbound
    )
