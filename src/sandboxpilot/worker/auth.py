"""Worker bearer-token authentication (constant-time comparison)."""

from __future__ import annotations

import hmac

from sandboxpilot.errors import WorkerAuthenticationError


def check_bearer(authorization: str | None, expected_token: str) -> None:
    if not expected_token:
        raise WorkerAuthenticationError("worker token is not configured")
    if not authorization or not authorization.lower().startswith("bearer "):
        raise WorkerAuthenticationError("missing bearer token")
    presented = authorization.split(" ", 1)[1].strip()
    if not hmac.compare_digest(presented.encode(), expected_token.encode()):
        raise WorkerAuthenticationError("invalid worker token")
