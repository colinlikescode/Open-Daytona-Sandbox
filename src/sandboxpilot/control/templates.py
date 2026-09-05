"""Template store: YAML files in the config directory.

All filesystem access runs in a worker thread so the control plane's event loop
never blocks on disk, even for these small files.
"""

from __future__ import annotations

import asyncio
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

    async def add(self, template: Template) -> Template:
        path = self._path(template.name)
        text = yaml.safe_dump(template.model_dump(mode="json", exclude_none=True), sort_keys=False)

        def write() -> None:
            self.directory.mkdir(parents=True, exist_ok=True)
            path.write_text(text)

        await asyncio.to_thread(write)
        return template

    async def get(self, name: str) -> Template:
        path = self._path(name)
        if not await asyncio.to_thread(path.exists):
            raise NotFoundError(
                f"template {name!r} not found",
                hint="List templates with: sandboxpilot template list",
            )
        return await asyncio.to_thread(self.load_file, path)

    async def list(self) -> list[Template]:
        def load_all() -> list[Template]:
            if not self.directory.exists():
                return []
            out: list[Template] = []
            for path in sorted(self.directory.glob("*.yaml")):
                try:
                    out.append(self.load_file(path))
                except ValidationError:
                    continue
            return out

        return await asyncio.to_thread(load_all)

    async def remove(self, name: str) -> None:
        path = self._path(name)

        def unlink() -> bool:
            if not path.exists():
                return False
            path.unlink()
            return True

        if not await asyncio.to_thread(unlink):
            raise NotFoundError(f"template {name!r} not found")

    @staticmethod
    def load_file(path: Path) -> Template:
        """Parse one template file (synchronous; call from a thread in async code)."""
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
