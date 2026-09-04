"""Ordered schema migrations.

Each migration is ``(version, name, sql)``. The database records applied
versions in ``schema_migrations`` and applies any newer ones at open time.
"""

from __future__ import annotations

from sandboxpilot.state.migrations.v001_initial import MIGRATION as V001

MIGRATIONS: list[tuple[int, str, str]] = [V001]
