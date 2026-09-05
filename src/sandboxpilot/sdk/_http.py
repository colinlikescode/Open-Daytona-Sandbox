"""Thin HTTP layer shared by the sync and async SDK clients.

Handles: base URL + token discovery, control plane autostart, error mapping,
SSE parsing. Nothing sandbox-specific lives here.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx

from sandboxpilot.config.loader import api_token_from_env, api_url_from_env, load_config
from sandboxpilot.config.models import Config
from sandboxpilot.errors import SandboxPilotError, error_from_payload
from sandboxpilot.schemas.commands import CommandEvent

DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=None)


def resolve_connection(
    url: str | None = None, token: str | None = None, *, autostart: bool = True
) -> tuple[str, str | None]:
    """Work out where the control plane is, starting a local one if allowed (blocking)."""
    config: Config | None = None
    if url is None:
        config = load_config()
        url = api_url_from_env(config.api.url)
    if token is None:
        token = api_token_from_env()
        if token is None and config is not None:
            token = config.api.token
    if autostart:
        from sandboxpilot.control.daemon import ensure_running, is_healthy

        if not is_healthy(url):
            config = config or load_config()
            if url == config.api.url:
                ensure_running(config)
    return url.rstrip("/"), token


async def resolve_connection_async(
    url: str | None = None, token: str | None = None, *, autostart: bool = True
) -> tuple[str, str | None]:
    """:func:`resolve_connection` without ever blocking the event loop.

    Config parsing runs in a thread; health checks use an async client; the
    daemon is spawned and awaited with ``asyncio`` primitives.
    """
    config: Config | None = None
    if url is None:
        config = await asyncio.to_thread(load_config)
        url = api_url_from_env(config.api.url)
    if token is None:
        token = api_token_from_env()
        if token is None and config is not None:
            token = config.api.token
    if autostart:
        from sandboxpilot.control.daemon import ensure_running_async, is_healthy_async

        if not await is_healthy_async(url):
            if config is None:
                config = await asyncio.to_thread(load_config)
            if url == config.api.url:
                await ensure_running_async(config)
    return url.rstrip("/"), token


def auth_headers(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def raise_for_status(resp: httpx.Response) -> None:
    if resp.status_code < 400:
        return
    try:
        payload: Any = resp.json()
    except ValueError:
        payload = resp.text
    raise error_from_payload(resp.status_code, payload)


def wrap_transport_error(exc: httpx.HTTPError, url: str) -> SandboxPilotError:
    return SandboxPilotError(
        f"Could not reach the SandboxPilot control plane at {url}: {exc}",
        hint="Is it running? Try: sandboxpilot daemon start",
    )


def parse_sse_lines(lines: Iterator[str]) -> Iterator[CommandEvent]:
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif line == "" and data:
            yield CommandEvent.model_validate_json("\n".join(data))
            data = []
    if data:
        yield CommandEvent.model_validate_json("\n".join(data))


async def aparse_sse_lines(lines: AsyncIterator[str]) -> AsyncIterator[CommandEvent]:
    data: list[str] = []
    async for raw in lines:
        line = raw.rstrip("\r\n")
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif line == "" and data:
            yield CommandEvent.model_validate_json("\n".join(data))
            data = []
    if data:
        yield CommandEvent.model_validate_json("\n".join(data))


def dumps(model: Any) -> str:
    if hasattr(model, "model_dump_json"):
        return str(model.model_dump_json(exclude_none=True))
    return json.dumps(model)
