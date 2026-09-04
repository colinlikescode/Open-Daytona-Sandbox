"""Bearer token check for the control plane API.

Loopback binds work without a token so local development stays frictionless.
Anything else must present ``Authorization: Bearer <token>``.
"""

from __future__ import annotations

import hmac

from sandboxpilot.errors import AuthenticationError


def check_api_token(authorization: str | None, expected: str | None) -> None:
    if expected is None:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise AuthenticationError("missing bearer token", hint="Set SANDBOXPILOT_API_TOKEN.")
    presented = authorization.split(" ", 1)[1].strip()
    if not hmac.compare_digest(presented.encode(), expected.encode()):
        raise AuthenticationError("invalid API token")
