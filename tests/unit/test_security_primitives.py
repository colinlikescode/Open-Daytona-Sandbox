"""Auth, proxy tokens, firewall rules, secret redaction."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from sandboxpilot.api.auth import check_api_token
from sandboxpilot.config.models import Config
from sandboxpilot.control.proxy_tokens import ProxyTokenSigner
from sandboxpilot.errors import AuthenticationError, WorkerAuthenticationError
from sandboxpilot.utils.logging import redact_text as redact
from sandboxpilot.worker.auth import check_bearer
from sandboxpilot.worker.firewall import BLOCKED_RANGES, build_rules


def test_api_token_optional_only_when_unset() -> None:
    check_api_token(None, None)  # loopback with no token configured
    with pytest.raises(AuthenticationError):
        check_api_token(None, "secret")
    with pytest.raises(AuthenticationError):
        check_api_token("Bearer nope", "secret")
    check_api_token("Bearer secret", "secret")


def test_worker_token_required() -> None:
    with pytest.raises(WorkerAuthenticationError):
        check_bearer(None, "w" * 32)
    with pytest.raises(WorkerAuthenticationError):
        check_bearer("Bearer " + "x" * 32, "w" * 32)
    with pytest.raises(WorkerAuthenticationError):
        check_bearer("Bearer x", "")  # unconfigured worker never authenticates anyone
    check_bearer("Bearer " + "w" * 32, "w" * 32)


def test_remote_bind_requires_token() -> None:
    with pytest.raises(ValueError, match="token"):
        Config.model_validate({"api": {"host": "0.0.0.0"}})
    Config.model_validate({"api": {"host": "0.0.0.0", "token": "t" * 32}})


def test_proxy_tokens_are_bound_and_expire() -> None:
    signer = ProxyTokenSigner("secret")
    now = datetime.now(UTC)
    token = signer.sign("sbx_1", 8080, now + timedelta(minutes=5))
    claims = signer.verify(token, now=now)
    assert (claims.sandbox_id, claims.port) == ("sbx_1", 8080)
    with pytest.raises(AuthenticationError):
        signer.verify(token, now=now + timedelta(minutes=6))
    with pytest.raises(AuthenticationError):
        ProxyTokenSigner("other").verify(token, now=now)
    with pytest.raises(AuthenticationError):
        signer.verify(token[:-3] + "AAA", now=now)


def test_firewall_blocks_metadata_and_private_ranges() -> None:
    plan = build_rules("br-abc123def456", "172.30.0.0/16")
    flat = " ".join(" ".join(rule) for rule in plan.rules)
    assert "169.254.0.0/16" in flat
    for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"):
        assert cidr in BLOCKED_RANGES
        assert cidr in flat
    assert "REJECT" in flat
    with pytest.raises(ValueError):
        build_rules("br-x; rm -rf /")


def test_firewall_protects_the_host_and_hooks_each_chain_once() -> None:
    plan = build_rules("br-abc123def456", "172.30.0.0/16")
    rules = plan.rules
    # Traffic to the VM itself goes through INPUT, which DOCKER-USER never sees.
    input_rules = [r for r in rules if r[3] == "SANDBOXPILOT-INPUT" and r[2] == "-A"]
    assert ["-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"] == input_rules[
        0
    ][4:]
    assert [
        "-i",
        "br-abc123def456",
        "-j",
        "REJECT",
        "--reject-with",
        "icmp-port-unreachable",
    ] == input_rules[1][4:]
    for parent, chain in (("DOCKER-USER", "SANDBOXPILOT"), ("INPUT", "SANDBOXPILOT-INPUT")):
        check = ["iptables", "-w", "-C", parent, "-i", "br-abc123def456", "-j", chain]
        insert = ["iptables", "-w", "-I", parent, "1", "-i", "br-abc123def456", "-j", chain]
        assert rules.index(insert) == rules.index(check) + 1  # check-then-insert pairs
    # Sandboxes may not talk to each other on the bridge.
    assert [
        "iptables",
        "-w",
        "-A",
        "SANDBOXPILOT",
        "-i",
        "br-abc123def456",
        "-o",
        "br-abc123def456",
        "-j",
        "REJECT",
    ] in rules


async def test_firewall_apply_skips_insert_when_hook_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sandboxpilot.worker import firewall

    executed: list[list[str]] = []

    async def fake_run(argv: list[str]) -> tuple[int, str]:
        executed.append(argv)
        if argv[2] == "-N":
            return 1, "Chain already exists."
        if argv[2] == "-C":
            return (0, "") if argv[3] == "DOCKER-USER" else (1, "")
        return 0, ""

    monkeypatch.setattr(firewall, "_run", fake_run)
    warnings = await firewall.apply_rules(build_rules("br-abc123def456", "10.211.0.0/16"))
    assert warnings == []
    inserts = [r for r in executed if r[2] == "-I"]
    assert [r[3] for r in inserts] == ["INPUT"]  # DOCKER-USER hook existed, INPUT hook was added


def test_sandbox_resolv_conf_uses_public_resolvers(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from sandboxpilot.worker.config import WorkerConfig
    from sandboxpilot.worker.runtime.docker_gvisor import (
        DEFAULT_SANDBOX_DNS,
        GVisorDockerRuntime,
        render_resolv_conf,
    )

    text = render_resolv_conf(DEFAULT_SANDBOX_DNS)
    assert "nameserver 8.8.8.8" in text and "nameserver 1.1.1.1" in text
    assert "169.254.169.254" not in text  # never the (firewalled) metadata resolver
    runtime = GVisorDockerRuntime(sandbox_dns=["9.9.9.9"], state_dir=tmp_path)
    path = runtime._ensure_resolv_conf()
    assert path.read_text() == "nameserver 9.9.9.9\noptions timeout:2 attempts:2\n"
    assert (path.stat().st_mode & 0o444) == 0o444
    assert WorkerConfig(sandbox_dns=" 1.1.1.1, ,8.8.8.8 ").sandbox_dns_list == [
        "1.1.1.1",
        "8.8.8.8",
    ]


def test_redaction_hides_tokens() -> None:
    text = redact("Authorization: Bearer abcdefghijklmnop1234 http://x/?token=xyz987654321")
    assert "abcdefghijklmnop1234" not in text
    assert "xyz987654321" not in text
