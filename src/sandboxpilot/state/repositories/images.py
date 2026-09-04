"""Image repository: which images are known to be present on which workers."""

from __future__ import annotations

from dataclasses import dataclass

from sandboxpilot.state.repositories.base import Repository
from sandboxpilot.utils.clock import utcnow


@dataclass(frozen=True)
class ImageRecord:
    worker_id: str
    reference: str
    digest: str | None
    pulled_at: str


class ImageRepository(Repository):
    async def record(self, worker_id: str, reference: str, digest: str | None) -> None:
        await self.db.execute(
            """INSERT INTO images(worker_id, reference, digest, pulled_at) VALUES (?, ?, ?, ?)
               ON CONFLICT(worker_id, reference) DO UPDATE SET digest=excluded.digest, pulled_at=excluded.pulled_at""",
            (worker_id, reference, digest, utcnow().isoformat()),
        )

    async def list(self, worker_id: str | None = None) -> list[ImageRecord]:
        if worker_id:
            rows = await self.db.fetchall(
                "SELECT worker_id, reference, digest, pulled_at FROM images WHERE worker_id = ? ORDER BY reference",
                (worker_id,),
            )
        else:
            rows = await self.db.fetchall(
                "SELECT worker_id, reference, digest, pulled_at FROM images ORDER BY reference"
            )
        return [
            ImageRecord(r["worker_id"], r["reference"], r["digest"], r["pulled_at"]) for r in rows
        ]

    async def delete_for_worker(self, worker_id: str) -> None:
        await self.db.execute("DELETE FROM images WHERE worker_id = ?", (worker_id,))
