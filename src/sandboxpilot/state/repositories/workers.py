"""Worker repository."""

from __future__ import annotations

from sandboxpilot.schemas.common import WorkerState
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.state.repositories.base import Repository


class WorkerRepository(Repository):
    async def upsert(self, worker: WorkerRecord) -> WorkerRecord:
        await self.db.execute(
            """INSERT INTO workers(id, pool_id, state, provider_cluster, data, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET pool_id=excluded.pool_id, state=excluded.state,
                 provider_cluster=excluded.provider_cluster, data=excluded.data, updated_at=excluded.updated_at""",
            (
                worker.id,
                worker.pool_id,
                worker.state.value,
                worker.provider_cluster,
                self.dump(worker),
                worker.created_at.isoformat(),
                worker.updated_at.isoformat(),
            ),
        )
        return worker

    async def get(self, worker_id: str) -> WorkerRecord | None:
        row = await self.db.fetchone("SELECT data FROM workers WHERE id = ?", (worker_id,))
        return self.load(WorkerRecord, row["data"]) if row else None

    async def get_by_cluster(self, cluster: str) -> WorkerRecord | None:
        row = await self.db.fetchone(
            "SELECT data FROM workers WHERE provider_cluster = ?", (cluster,)
        )
        return self.load(WorkerRecord, row["data"]) if row else None

    async def list(
        self,
        pool_id: str | None = None,
        states: set[WorkerState] | None = None,
        include_terminal: bool = False,
    ) -> list[WorkerRecord]:
        sql = "SELECT data FROM workers"
        clauses: list[str] = []
        params: list[str] = []
        if pool_id:
            clauses.append("pool_id = ?")
            params.append(pool_id)
        if states:
            clauses.append(f"state IN ({','.join('?' * len(states))})")
            params.extend(s.value for s in states)
        elif not include_terminal:
            clauses.append("state NOT IN (?, ?)")
            params.extend([WorkerState.TERMINATED.value, WorkerState.LOST.value])
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at"
        rows = await self.db.fetchall(sql, tuple(params))
        return [self.load(WorkerRecord, r["data"]) for r in rows]

    async def delete(self, worker_id: str) -> None:
        await self.db.execute("DELETE FROM workers WHERE id = ?", (worker_id,))
