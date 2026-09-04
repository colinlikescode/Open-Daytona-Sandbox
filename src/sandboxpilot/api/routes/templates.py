"""Sandbox templates (stored as YAML files in the config directory)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from sandboxpilot.api.deps import control, require_auth
from sandboxpilot.control.service import ControlPlane
from sandboxpilot.schemas.template import Template

router = APIRouter(prefix="/templates", tags=["templates"], dependencies=[Depends(require_auth)])


@router.get("", response_model=list[Template])
async def list_templates(cp: ControlPlane = Depends(control)) -> list[Template]:
    return cp.templates.list()


@router.post("", response_model=Template, status_code=201)
async def add_template(body: Template, cp: ControlPlane = Depends(control)) -> Template:
    return cp.templates.add(body)


@router.get("/{name}", response_model=Template)
async def get_template(name: str, cp: ControlPlane = Depends(control)) -> Template:
    return cp.templates.get(name)


@router.delete("/{name}", status_code=204)
async def remove_template(name: str, cp: ControlPlane = Depends(control)) -> None:
    cp.templates.remove(name)
