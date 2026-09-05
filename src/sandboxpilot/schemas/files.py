"""File transfer helpers.

File contents are transferred as tar archives (Docker archive API semantics)
so the same code path handles single files and directories, preserves modes
and never requires the worker to touch host filesystem paths.
"""

from __future__ import annotations

MAX_FILE_MODE = 0o7777


def validate_sandbox_path(path: str) -> str:
    if not path or not path.startswith("/"):
        raise ValueError("sandbox paths must be absolute")
    if "\0" in path:
        raise ValueError("invalid path")
    return path
