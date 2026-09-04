"""Sandbox repository."""

from __future__ import annotations

from sandboxpilot.schemas.common import SandboxState
from sandboxpilot.schemas.sandbox import SandboxRecord
from sandboxpilot.state.repositories.base import Repository

ACTIVE_STATES = {s for s in SandboxState if s.is_active}


class SandboxRepository(Repository):
    async def upsert(self, sandbox: SandboxRecord) -> SandboxRecord:
        await self.db.execute(
            """INSERT INTO sandboxes(id, pool_id, worker_id, state, idempotency_key, data, created_at, updated_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET worker_id=excluded.worker_id, state=excluded.state,
                 data=excluded.data, updated_at=excluded.updated_at, expires_at=excluded.expires_at""",
            (
                sandbox.id,
                sandbox.pool_id,
                sandbox.worker_id,
                sandbox.state.value,
                sandbox.idempotency_key,
                self.dump(sandbox),
                sandbox.created_at.isoformat(),
                sandbox.updated_at.isoformat(),
                self.iso(sandbox.expires_at),
            ),
        )
        return sandbox

    async def get(self, sandbox_id: str) -> SandboxRecord | None:
        row = await self.db.fetchone("SELECT data FROM sandboxes WHERE id = ?", (sandbox_id,))
        return self.load(SandboxRecord, row["data"]) if row else None

    async def get_by_idempotency_key(self, key: str) -> SandboxRecord | None:
        row = await self.db.fetchone("SELECT data FROM sandboxes WHERE idempotency_key = ?", (key,))
        return self.load(SandboxRecord, row["data"]) if row else None

    async def resolve(self, ref: str) -> SandboxRecord | None:
        """Resolve a full id or an unambiguous short id prefix."""
        found = await self.get(ref)
        if found:
            return found
        rows = await self.db.fetchall("SELECT data FROM sandboxes")
        matches = [self.load(SandboxRecord, r["data"]) for r in rows]
        matches = [m for m in matches if m.short_id.startswith(ref) or m.id.startswith(ref)]
        if len(matches) == 1:
            return matches[0]
        return None

    async def list(
        self,
        pool_id: str | None = None,
        worker_id: str | None = None,
        states: set[SandboxState] | None = None,
        active_only: bool = False,
        limit: int | None = None,
    ) -> list[SandboxRecord]:
        sql = "SELECT data FROM sandboxes"
        clauses: list[str] = []
        params: list[str | int] = []
        if pool_id:
            clauses.append("pool_id = ?")
            params.append(pool_id)
        if worker_id:
            clauses.append("worker_id = ?")
            params.append(worker_id)
        wanted = states or (ACTIVE_STATES if active_only else None)
        if wanted:
            clauses.append(f"state IN ({','.join('?' * len(wanted))})")
            params.extend(s.value for s in wanted)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        rows = await self.db.fetchall(sql, tuple(params))
        return [self.load(SandboxRecord, r["data"]) for r in rows]

    async def count_active(self, worker_id: str) -> int:
        row = await self.db.fetchone(
            f"SELECT COUNT(*) AS n FROM sandboxes WHERE worker_id = ? AND state IN ({','.join('?' * len(ACTIVE_STATES))})",  # noqa: S608
            (worker_id, *[s.value for s in ACTIVE_STATES]),
        )
        return int(row["n"]) if row else 0

    async def delete(self, sandbox_id: str) -> None:
        await self.db.execute("DELETE FROM sandboxes WHERE id = ?", (sandbox_id,))

    async def delete_terminal_older_than(self, cutoff_iso: str) -> int:
        terminal = [s.value for s in SandboxState if s.is_terminal]
        rows = await self.db.fetchall(
            f"SELECT id FROM sandboxes WHERE state IN ({','.join('?' * len(terminal))}) AND updated_at < ?",  # noqa: S608
            (*terminal, cutoff_iso),
        )
        for row in rows:
            await self.delete(row["id"])
        return len(rows)
