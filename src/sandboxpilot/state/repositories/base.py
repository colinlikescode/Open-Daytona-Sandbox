"""Repository base."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from sandboxpilot.state.db import Database

M = TypeVar("M", bound=BaseModel)


class Repository:
    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def dump(model: BaseModel) -> str:
        return model.model_dump_json()

    @staticmethod
    def load(model_cls: type[M], raw: str | bytes) -> M:
        return model_cls.model_validate_json(raw)

    @staticmethod
    def iso(value: Any) -> str | None:
        return value.isoformat() if value is not None else None
