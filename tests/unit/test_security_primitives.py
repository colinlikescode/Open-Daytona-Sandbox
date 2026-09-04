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
    assert "169.254.169.254" in flat or "169.254.0.0/16" in flat
    for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"):
        assert cidr in BLOCKED_RANGES
        assert cidr in flat
    assert "DROP" in flat or "REJECT" in flat
    with pytest.raises(ValueError):
        build_rules("br-x; rm -rf /")


def test_redaction_hides_tokens() -> None:
    text = redact("Authorization: Bearer abcdefghijklmnop1234 http://x/?token=xyz987654321")
    assert "abcdefghijklmnop1234" not in text
    assert "xyz987654321" not in text
