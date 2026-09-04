"""Console output helpers: tables for humans, JSON when asked."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


class Output:
    """Set once by the root command; every subcommand renders through it."""

    json_mode = False


def _plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def emit(value: Any) -> None:
    """Print raw data as JSON (used when ``--json`` is set)."""
    print(json.dumps(_plain(value), indent=2, default=str))


def table(
    rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]], *, title: str | None = None
) -> None:
    rows = list(rows)
    if Output.json_mode:
        emit(rows)
        return
    t = Table(title=title, show_lines=False, header_style="bold")
    for _, header in columns:
        t.add_column(header)
    for row in rows:
        t.add_row(*[fmt(row.get(key)) for key, _ in columns])
    if not rows:
        console.print("(none)" if title is None else f"(no {title.lower()})")
        return
    console.print(t)


def detail(value: Any, *, keys: list[str] | None = None) -> None:
    data = _plain(value)
    if Output.json_mode:
        emit(data)
        return
    if not isinstance(data, dict):
        console.print(data)
        return
    for key in keys or list(data):
        if key in data:
            console.print(f"[bold]{key}[/bold]: {fmt(data[key])}")


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    return str(value)


def human_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024  # type: ignore[assignment]
    return str(n)


def ok(message: str) -> None:
    if not Output.json_mode:
        console.print(f"[green]✓[/green] {message}")


def warn(message: str) -> None:
    err_console.print(f"[yellow]![/yellow] {message}")


def fail(message: str, hint: str | None = None) -> None:
    err_console.print(f"[red]error:[/red] {message}")
    if hint:
        err_console.print(f"  hint: {hint}")


def is_tty() -> bool:
    return sys.stdout.isatty()
