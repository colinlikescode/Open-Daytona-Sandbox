"""SQLite state layer: lookups by short id, housekeeping deletes, permissions."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest

from sandboxpilot.schemas.common import OperationStatus, SandboxState
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.schemas.sandbox import SandboxRecord, SandboxSpec
from sandboxpilot.state.db import Database
from sandboxpilot.utils.clock import utcnow


@pytest.fixture
async def db() -> AsyncIterator[Database]:
    database = Database(":memory:")
    await database.open()
    try:
        yield database
    finally:
        await database.close()


def record(**kw: object) -> SandboxRecord:
    spec = SandboxSpec(image="python:3.12-slim", cpus=1, memory_bytes=1024**3)
    return SandboxRecord(pool_id="pool_x", pool_name="x", spec=spec, **kw)  # type: ignore[arg-type]


async def test_resolve_by_full_id_short_id_and_prefix(db: Database) -> None:
    # Created in the same millisecond: identical UUIDv7 time prefix, distinct tails.
    a = await db.sandboxes.upsert(record())
    b = await db.sandboxes.upsert(record())
    assert a.short_id != b.short_id
    assert (await db.sandboxes.resolve(a.id)) is not None
    assert (await db.sandboxes.resolve(a.short_id)).id == a.id  # type: ignore[union-attr]
    assert (await db.sandboxes.resolve(b.short_id)).id == b.id  # type: ignore[union-attr]
    body = a.id.split("_", 1)[1].replace("-", "")
    assert (await db.sandboxes.resolve(body[-16:])).id == a.id  # type: ignore[union-attr]
    assert await db.sandboxes.resolve("zzz") is None
    assert await db.sandboxes.resolve("") is None
    assert await db.sandboxes.resolve("%") is None  # LIKE wildcards are matched literally
    assert await db.sandboxes.resolve("sbx_%") is None
    # The shared time prefix is ambiguous and must not resolve to either record.
    assert a.id[:12] == b.id[:12]
    assert await db.sandboxes.resolve(a.id[:12]) is None


async def test_housekeeping_deletes_report_counts(db: Database) -> None:
    old = record(state=SandboxState.STOPPED)
    old.updated_at = utcnow() - timedelta(days=30)
    await db.sandboxes.upsert(old)
    await db.sandboxes.upsert(record(state=SandboxState.RUNNING))
    cutoff = (utcnow() - timedelta(days=7)).isoformat()
    assert await db.sandboxes.delete_terminal_older_than(cutoff) == 1
    assert await db.sandboxes.delete_terminal_older_than(cutoff) == 0
    assert len(await db.sandboxes.list()) == 1

    done = Operation(type="x", status=OperationStatus.SUCCEEDED)
    done.created_at = utcnow() - timedelta(days=30)
    await db.operations.upsert(done)
    await db.operations.upsert(Operation(type="y"))
    assert await db.operations.delete_terminal_older_than(cutoff) == 1
    assert len(await db.operations.list()) == 1

    await db.idempotency.put("k", "sbx_1")
    assert await db.idempotency.expire(ttl_seconds=3600) == 0
    assert await db.idempotency.expire(ttl_seconds=-1) == 1


async def test_database_file_is_private(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.db"
    database = Database(path)
    await database.open()
    try:
        assert path.exists()
        assert (path.stat().st_mode & 0o777) == 0o600
        assert (path.parent.stat().st_mode & 0o777) == 0o700
        assert await database.schema_version() >= 1
    finally:
        await database.close()
