"""Template store: YAML files in the config directory."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError as PydanticValidationError

from sandboxpilot.errors import NotFoundError, ValidationError
from sandboxpilot.schemas.template import Template


class TemplateStore:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def _path(self, name: str) -> Path:
        safe = "".join(ch for ch in name if ch.isalnum() or ch in "-_.")
        if safe != name or not safe:
            raise ValidationError(f"invalid template name {name!r}")
        return self.directory / f"{name}.yaml"

    def add(self, template: Template) -> Template:
        self.directory.mkdir(parents=True, exist_ok=True)
        self._path(template.name).write_text(
            yaml.safe_dump(template.model_dump(mode="json", exclude_none=True), sort_keys=False)
        )
        return template

    def get(self, name: str) -> Template:
        path = self._path(name)
        if not path.exists():
            raise NotFoundError(
                f"template {name!r} not found",
                hint="List templates with: sandboxpilot template list",
            )
        return self.load_file(path)

    def list(self) -> list[Template]:
        if not self.directory.exists():
            return []
        out: list[Template] = []
        for path in sorted(self.directory.glob("*.yaml")):
            try:
                out.append(self.load_file(path))
            except ValidationError:
                continue
        return out

    def remove(self, name: str) -> None:
        path = self._path(name)
        if not path.exists():
            raise NotFoundError(f"template {name!r} not found")
        path.unlink()

    @staticmethod
    def load_file(path: Path) -> Template:
        try:
            data = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise ValidationError(f"could not parse {path}: {exc}") from exc
        if not isinstance(data, dict):
            raise ValidationError(f"{path} must contain a mapping")
        data.setdefault("name", path.stem)
        try:
            return Template.model_validate(data)
        except PydanticValidationError as exc:
            raise ValidationError(f"invalid template {path}: {exc}") from exc
