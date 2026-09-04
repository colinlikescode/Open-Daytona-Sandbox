"""Key/value runtime state (proxy signing secret, daemon metadata, ...)."""

from __future__ import annotations

from sandboxpilot.state.repositories.base import Repository
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.ids import new_token

PROXY_SECRET_KEY = "proxy_signing_secret"
INSTANCE_ID_KEY = "control_plane_instance_id"


class RuntimeStateRepository(Repository):
    async def get(self, key: str) -> str | None:
        row = await self.db.fetchone("SELECT value FROM runtime_state WHERE key = ?", (key,))
        return str(row["value"]) if row else None

    async def set(self, key: str, value: str) -> None:
        await self.db.execute(
            """INSERT INTO runtime_state(key, value, updated_at) VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
            (key, value, utcnow().isoformat()),
        )

    async def get_or_create_secret(self, key: str = PROXY_SECRET_KEY) -> str:
        existing = await self.get(key)
        if existing:
            return existing
        secret = new_token(32)
        await self.set(key, secret)
        return secret
