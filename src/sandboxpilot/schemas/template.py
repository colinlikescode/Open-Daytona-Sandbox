"""Sandbox templates: reusable creation configuration (not snapshots)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sandboxpilot.schemas.common import NetworkPolicy
from sandboxpilot.utils.sizes import parse_bytes, parse_duration


class TemplateResources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpus: float | None = None
    memory: str | int | None = None
    pids_limit: int | None = None
    tmpfs: str | int | None = None


class Template(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=63, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    image: str | None = None
    workdir: str | None = None
    resources: TemplateResources = Field(default_factory=TemplateResources)
    network: NetworkPolicy | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout: str | int | None = None
    user: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    read_only_root: bool | None = None
    keepalive_command: list[str] | None = None
    description: str | None = None

    @field_validator("env", mode="before")
    @classmethod
    def _stringify_env(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return {str(k): str(val) for k, val in v.items()}
        return v

    @model_validator(mode="after")
    def _validate(self) -> Template:
        if self.resources.memory is not None:
            parse_bytes(self.resources.memory)
        if self.resources.tmpfs is not None:
            parse_bytes(self.resources.tmpfs)
        if self.timeout is not None:
            parse_duration(self.timeout)
        return self

    def as_create_overrides(self) -> dict[str, Any]:
        """Fields to apply beneath explicit create parameters."""
        out: dict[str, Any] = {}
        if self.image:
            out["image"] = self.image
        if self.workdir:
            out["workdir"] = self.workdir
        if self.resources.cpus is not None:
            out["cpus"] = self.resources.cpus
        if self.resources.memory is not None:
            out["memory"] = self.resources.memory
        if self.resources.pids_limit is not None:
            out["pids_limit"] = self.resources.pids_limit
        if self.resources.tmpfs is not None:
            out["tmpfs"] = self.resources.tmpfs
        if self.network is not None:
            out["network"] = self.network
        if self.env:
            out["env"] = dict(self.env)
        if self.timeout is not None:
            out["timeout"] = self.timeout
        if self.user is not None:
            out["user"] = self.user
        if self.labels:
            out["labels"] = dict(self.labels)
        if self.read_only_root is not None:
            out["read_only_root"] = self.read_only_root
        if self.keepalive_command is not None:
            out["keepalive_command"] = list(self.keepalive_command)
        return out
