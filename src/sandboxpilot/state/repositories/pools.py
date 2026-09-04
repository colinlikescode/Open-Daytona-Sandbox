"""Pool repository."""

from __future__ import annotations

from sandboxpilot.schemas.pool import WorkerPool
from sandboxpilot.state.repositories.base import Repository


class PoolRepository(Repository):
    async def upsert(self, pool: WorkerPool) -> WorkerPool:
        await self.db.execute(
            """INSERT INTO pools(id, name, data, created_at, updated_at) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name, data=excluded.data, updated_at=excluded.updated_at""",
            (
                pool.id,
                pool.name,
                self.dump(pool),
                pool.created_at.isoformat(),
                pool.updated_at.isoformat(),
            ),
        )
        return pool

    async def get(self, pool_id: str) -> WorkerPool | None:
        row = await self.db.fetchone("SELECT data FROM pools WHERE id = ?", (pool_id,))
        return self.load(WorkerPool, row["data"]) if row else None

    async def get_by_name(self, name: str) -> WorkerPool | None:
        row = await self.db.fetchone("SELECT data FROM pools WHERE name = ?", (name,))
        return self.load(WorkerPool, row["data"]) if row else None

    async def resolve(self, ref: str) -> WorkerPool | None:
        """Look up by id or name."""
        return await self.get(ref) or await self.get_by_name(ref)

    async def list(self) -> list[WorkerPool]:
        rows = await self.db.fetchall("SELECT data FROM pools ORDER BY created_at")
        return [self.load(WorkerPool, r["data"]) for r in rows]

    async def delete(self, pool_id: str) -> None:
        await self.db.execute("DELETE FROM pools WHERE id = ?", (pool_id,))
