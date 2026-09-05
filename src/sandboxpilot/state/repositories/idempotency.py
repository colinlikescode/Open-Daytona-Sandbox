"""Idempotency-key -> sandbox mapping with bounded TTL."""

from __future__ import annotations

from datetime import timedelta

from sandboxpilot.state.repositories.base import Repository
from sandboxpilot.utils.clock import utcnow


class IdempotencyRepository(Repository):
    async def get(self, key: str) -> str | None:
        row = await self.db.fetchone(
            "SELECT sandbox_id FROM idempotency_keys WHERE key = ?", (key,)
        )
        return str(row["sandbox_id"]) if row else None

    async def put(self, key: str, sandbox_id: str) -> None:
        await self.db.execute(
            """INSERT INTO idempotency_keys(key, sandbox_id, created_at) VALUES (?, ?, ?)
               ON CONFLICT(key) DO NOTHING""",
            (key, sandbox_id, utcnow().isoformat()),
        )

    async def expire(self, ttl_seconds: int) -> int:
        cutoff = (utcnow() - timedelta(seconds=ttl_seconds)).isoformat()
        return await self.db.execute_returning_rowcount(
            "DELETE FROM idempotency_keys WHERE created_at < ?", (cutoff,)
        )
