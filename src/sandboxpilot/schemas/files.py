"""File transfer models.

File contents are transferred as tar archives (Docker archive API semantics)
so the same code path handles single files and directories, preserves modes
and never requires the worker to touch host filesystem paths.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


def validate_sandbox_path(path: str) -> str:
    if not path or not path.startswith("/"):
        raise ValueError("sandbox paths must be absolute")
    if "\0" in path:
        raise ValueError("invalid path")
    return path


class FileWriteQuery(BaseModel):
    """Query parameters for ``PUT /sandboxes/{id}/files``."""

    model_config = ConfigDict(extra="forbid")

    path: str
    mode: int | None = Field(default=None, ge=0, le=0o7777)
    archive: bool = Field(default=False, description="Body is a tar archive extracted at ``path``")

    @field_validator("path")
    @classmethod
    def _path(cls, v: str) -> str:
        return validate_sandbox_path(v)


class FileReadQuery(BaseModel):
    """Query parameters for ``GET /sandboxes/{id}/files``."""

    model_config = ConfigDict(extra="forbid")

    path: str
    archive: bool = Field(default=False, description="Return a tar archive instead of raw bytes")

    @field_validator("path")
    @classmethod
    def _path(cls, v: str) -> str:
        return validate_sandbox_path(v)


class FileStat(BaseModel):
    path: str
    size: int
    mode: int
    is_dir: bool
