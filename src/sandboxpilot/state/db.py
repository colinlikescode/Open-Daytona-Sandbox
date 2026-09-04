"""Async SQLite database with migrations and repositories."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiosqlite

from sandboxpilot.errors import StateError
from sandboxpilot.state.migrations import MIGRATIONS
from sandboxpilot.state.repositories.idempotency import IdempotencyRepository
from sandboxpilot.state.repositories.images import ImageRepository
from sandboxpilot.state.repositories.operations import OperationRepository
from sandboxpilot.state.repositories.pools import PoolRepository
from sandboxpilot.state.repositories.runtime_state import RuntimeStateRepository
from sandboxpilot.state.repositories.sandboxes import SandboxRepository
from sandboxpilot.state.repositories.workers import WorkerRepository
from sandboxpilot.utils.clock import utcnow
from sandboxpilot.utils.paths import ensure_private_dir, ensure_private_file
from sandboxpilot.version import STATE_SCHEMA_VERSION


class Database:
    """Single-connection async SQLite wrapper.

    A single connection guarded by a lock is used: SQLite serializes writers
    anyway, and it keeps transactions simple and correct.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path) if str(path) != ":memory:" else path
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self.pools = PoolRepository(self)
        self.workers = WorkerRepository(self)
        self.sandboxes = SandboxRepository(self)
        self.operations = OperationRepository(self)
        self.images = ImageRepository(self)
        self.runtime_state = RuntimeStateRepository(self)
        self.idempotency = IdempotencyRepository(self)

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise StateError("Database is not open")
        return self._conn

    async def open(self) -> None:
        if self._conn is not None:
            return
        if isinstance(self.path, Path):
            ensure_private_dir(self.path.parent)
            existed = self.path.exists()
            self._conn = await aiosqlite.connect(self.path)
            ensure_private_file(self.path)
            if not existed:
                ensure_private_file(self.path)
        else:
            self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._migrate()
        if isinstance(self.path, Path):
            for suffix in ("-wal", "-shm"):
                side = self.path.with_name(self.path.name + suffix)
                if side.exists():
                    ensure_private_file(side)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> Database:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def _migrate(self) -> None:
        conn = self.conn
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
        )
        await conn.commit()
        cursor = await conn.execute("SELECT version FROM schema_migrations")
        applied = {row[0] for row in await cursor.fetchall()}
        for version, name, sql in MIGRATIONS:
            if version in applied:
                continue
            await conn.executescript("BEGIN;" + sql + "COMMIT;")
            await conn.execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                (version, name, utcnow().isoformat()),
            )
            await conn.commit()
        cursor = await conn.execute("SELECT MAX(version) FROM schema_migrations")
        row = await cursor.fetchone()
        current = row[0] if row and row[0] is not None else 0
        if current > STATE_SCHEMA_VERSION:
            raise StateError(
                f"State database schema version {current} is newer than this SandboxPilot "
                f"build supports ({STATE_SCHEMA_VERSION}). Upgrade SandboxPilot."
            )

    async def schema_version(self) -> int:
        cursor = await self.conn.execute("SELECT MAX(version) FROM schema_migrations")
        row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        async with self._lock:
            await self.conn.execute(sql, params)
            await self.conn.commit()

    async def execute_many(self, statements: list[tuple[str, tuple[Any, ...]]]) -> None:
        """Run several statements in one transaction."""
        async with self._lock:
            try:
                for sql, params in statements:
                    await self.conn.execute(sql, params)
                await self.conn.commit()
            except Exception:
                await self.conn.rollback()
                raise

    async def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> aiosqlite.Row | None:
        async with self._lock:
            cursor = await self.conn.execute(sql, params)
            return await cursor.fetchone()

    async def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        async with self._lock:
            cursor = await self.conn.execute(sql, params)
            return list(await cursor.fetchall())
