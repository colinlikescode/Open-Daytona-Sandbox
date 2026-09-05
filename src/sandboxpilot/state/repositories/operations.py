"""Operation repository."""

from __future__ import annotations

from sandboxpilot.schemas.common import OperationStatus
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.state.repositories.base import Repository


class OperationRepository(Repository):
    async def upsert(self, op: Operation) -> Operation:
        await self.db.execute(
            """INSERT INTO operations(id, type, status, pool_id, worker_id, data, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET status=excluded.status, worker_id=excluded.worker_id, data=excluded.data""",
            (
                op.id,
                op.type,
                op.status.value,
                op.pool_id,
                op.worker_id,
                self.dump(op),
                op.created_at.isoformat(),
            ),
        )
        return op

    async def get(self, op_id: str) -> Operation | None:
        row = await self.db.fetchone("SELECT data FROM operations WHERE id = ?", (op_id,))
        return self.load(Operation, row["data"]) if row else None

    async def list(
        self, status: OperationStatus | None = None, pool_id: str | None = None, limit: int = 100
    ) -> list[Operation]:
        sql = "SELECT data FROM operations"
        clauses: list[str] = []
        params: list[str | int] = []
        if status:
            clauses.append("status = ?")
            params.append(status.value)
        if pool_id:
            clauses.append("pool_id = ?")
            params.append(pool_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = await self.db.fetchall(sql, tuple(params))
        return [self.load(Operation, r["data"]) for r in rows]

    async def fail_stale_running(self, error: str) -> int:
        """Mark PENDING/RUNNING operations as failed (used after a control-plane restart)."""
        ops = await self.list(status=OperationStatus.RUNNING, limit=10_000)
        ops += await self.list(status=OperationStatus.PENDING, limit=10_000)
        for op in ops:
            op.fail(error)
            await self.upsert(op)
        return len(ops)

    async def delete_terminal_older_than(self, cutoff_iso: str) -> int:
        terminal = [s.value for s in OperationStatus if s.is_terminal]
        return await self.db.execute_returning_rowcount(
            f"DELETE FROM operations WHERE status IN ({','.join('?' * len(terminal))}) AND created_at < ?",  # noqa: S608
            (*terminal, cutoff_iso),
        )
