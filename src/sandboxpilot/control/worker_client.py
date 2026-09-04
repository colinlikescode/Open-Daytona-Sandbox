"""HTTP client for the worker private API (used through the SSH tunnel)."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import httpx

from sandboxpilot.errors import (
    SandboxPilotError,
    WorkerAuthenticationError,
    WorkerUnavailableError,
    error_from_payload,
)
from sandboxpilot.schemas.commands import (
    CommandEvent,
    CommandInfo,
    CommandLogs,
    CommandRequest,
    CommandResult,
)
from sandboxpilot.schemas.sandbox import SandboxSpec
from sandboxpilot.schemas.worker import WorkerHealth
from sandboxpilot.worker.runtime.base import ImageInfo
from sandboxpilot.worker.service import WorkerSandboxView


def parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[CommandEvent]:
    return _parse_sse(lines)


async def _parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[CommandEvent]:
    data_lines: list[str] = []
    async for raw in lines:
        line = raw.rstrip("\r\n")
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif line == "" and data_lines:
            payload = "\n".join(data_lines)
            data_lines = []
            yield CommandEvent.model_validate_json(payload)
    if data_lines:
        yield CommandEvent.model_validate_json("\n".join(data_lines))


class WorkerClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._own = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, read=None, connect=5.0)
        )

    async def aclose(self) -> None:
        if self._own:
            await self._client.aclose()

    @property
    def http(self) -> httpx.AsyncClient:
        return self._client

    def headers(self) -> dict[str, str]:
        return dict(self._headers)

    def url(self, path: str) -> str:
        return f"{self.base_url}/v1{path}"

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            resp = await self._client.request(
                method, self.url(path), headers=self._headers, **kwargs
            )
        except httpx.ConnectError as exc:
            raise WorkerUnavailableError(f"worker unreachable: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise WorkerUnavailableError(f"worker request timed out: {exc}") from exc
        except httpx.HTTPError as exc:
            raise WorkerUnavailableError(f"worker request failed: {exc}") from exc
        if resp.status_code >= 400:
            self._raise(resp)
        return resp

    def _raise(self, resp: httpx.Response) -> None:
        try:
            payload = resp.json()
        except ValueError:
            payload = {"error": {"code": "worker_error", "message": resp.text[:500]}}
        if resp.status_code == 401:
            raise WorkerAuthenticationError("worker rejected the control plane token")
        raise error_from_payload(resp.status_code, payload)

    async def health(self, timeout: float | None = None) -> WorkerHealth:
        resp = await self._request("GET", "/health", timeout=timeout)
        return WorkerHealth.model_validate(resp.json())

    async def drain(self, draining: bool) -> None:
        await self._request("POST", "/drain", json={"draining": draining})

    async def list_images(self) -> list[ImageInfo]:
        resp = await self._request("GET", "/images")
        return [ImageInfo.model_validate(i) for i in resp.json()]

    async def pull_image(self, reference: str, policy: str = "if-not-present") -> ImageInfo:
        resp = await self._request(
            "POST", "/images/pull", json={"reference": reference, "policy": policy}, timeout=1800
        )
        return ImageInfo.model_validate(resp.json())

    async def list_sandboxes(self) -> list[WorkerSandboxView]:
        resp = await self._request("GET", "/sandboxes")
        return [WorkerSandboxView.model_validate(s) for s in resp.json()]

    async def create_sandbox(
        self, sandbox_id: str, pool_id: str, spec: SandboxSpec, expires_at: datetime
    ) -> WorkerSandboxView:
        body = {
            "sandbox_id": sandbox_id,
            "pool_id": pool_id,
            "spec": spec.model_dump(mode="json"),
            "expires_at": expires_at.isoformat(),
        }
        resp = await self._request("POST", "/sandboxes", json=body, timeout=600)
        return WorkerSandboxView.model_validate(resp.json())

    async def get_sandbox(self, sandbox_id: str) -> WorkerSandboxView:
        resp = await self._request("GET", f"/sandboxes/{sandbox_id}")
        return WorkerSandboxView.model_validate(resp.json())

    async def delete_sandbox(self, sandbox_id: str) -> None:
        await self._request("DELETE", f"/sandboxes/{sandbox_id}", timeout=60)

    async def set_expiration(self, sandbox_id: str, expires_at: datetime) -> WorkerSandboxView:
        resp = await self._request(
            "PATCH",
            f"/sandboxes/{sandbox_id}/expiration",
            json={"expires_at": expires_at.isoformat()},
        )
        return WorkerSandboxView.model_validate(resp.json())

    async def run_command(self, sandbox_id: str, request: CommandRequest) -> CommandResult:
        body = request.model_copy(update={"background": False}).model_dump(
            mode="json", exclude_none=True
        )
        resp = await self._request("POST", f"/sandboxes/{sandbox_id}/exec", json=body, timeout=None)
        return CommandResult.model_validate(resp.json())

    async def start_command(self, sandbox_id: str, request: CommandRequest) -> CommandInfo:
        body = request.model_dump(mode="json", exclude_none=True)
        resp = await self._request("POST", f"/sandboxes/{sandbox_id}/exec/start", json=body)
        return CommandInfo.model_validate(resp.json())

    async def get_command(self, sandbox_id: str, command_id: str) -> CommandInfo:
        resp = await self._request("GET", f"/sandboxes/{sandbox_id}/exec/{command_id}")
        return CommandInfo.model_validate(resp.json())

    async def command_result(
        self, sandbox_id: str, command_id: str, wait: bool = True
    ) -> CommandResult:
        resp = await self._request(
            "GET",
            f"/sandboxes/{sandbox_id}/exec/{command_id}/result",
            params={"wait": str(wait).lower()},
            timeout=None,
        )
        return CommandResult.model_validate(resp.json())

    async def command_logs(self, sandbox_id: str, command_id: str) -> CommandLogs:
        resp = await self._request("GET", f"/sandboxes/{sandbox_id}/exec/{command_id}/logs")
        return CommandLogs.model_validate(resp.json())

    async def kill_command(self, sandbox_id: str, command_id: str) -> CommandInfo:
        resp = await self._request("DELETE", f"/sandboxes/{sandbox_id}/exec/{command_id}")
        return CommandInfo.model_validate(resp.json())

    async def stream_command(
        self, sandbox_id: str, command_id: str, from_seq: int = 0
    ) -> AsyncIterator[CommandEvent]:
        url = self.url(f"/sandboxes/{sandbox_id}/exec/{command_id}/stream")
        try:
            async with self._client.stream(
                "GET", url, headers=self._headers, params={"from_seq": from_seq}, timeout=None
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    self._raise(resp)
                async for event in _parse_sse(resp.aiter_lines()):
                    yield event
        except httpx.HTTPError as exc:
            raise WorkerUnavailableError(f"worker stream failed: {exc}") from exc

    async def upload(
        self,
        sandbox_id: str,
        path: str,
        content: AsyncIterator[bytes] | bytes,
        *,
        archive: bool,
        mode: int | None = None,
        size: int | None = None,
    ) -> None:
        params: dict[str, Any] = {"path": path, "archive": str(archive).lower()}
        if mode is not None:
            params["mode"] = mode
        headers = dict(self._headers)
        if size is not None:
            headers["Content-Length"] = str(size)
        try:
            resp = await self._client.put(
                self.url(f"/sandboxes/{sandbox_id}/files"),
                params=params,
                headers=headers,
                content=content,
                timeout=None,
            )
        except httpx.HTTPError as exc:
            raise WorkerUnavailableError(f"worker upload failed: {exc}") from exc
        if resp.status_code >= 400:
            self._raise(resp)

    async def download(self, sandbox_id: str, path: str, *, archive: bool) -> AsyncIterator[bytes]:
        url = self.url(f"/sandboxes/{sandbox_id}/files")
        try:
            async with self._client.stream(
                "GET",
                url,
                params={"path": path, "archive": str(archive).lower()},
                headers=self._headers,
                timeout=None,
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    self._raise(resp)
                async for chunk in resp.aiter_bytes():
                    yield chunk
        except httpx.HTTPError as exc:
            raise WorkerUnavailableError(f"worker download failed: {exc}") from exc

    def proxy_base(self, sandbox_id: str, port: int) -> str:
        return self.url(f"/sandboxes/{sandbox_id}/proxy/{port}")


async def safe_close(client: WorkerClient | None) -> None:
    if client is not None:
        with contextlib.suppress(Exception):
            await client.aclose()


__all__ = ["SandboxPilotError", "WorkerClient", "parse_sse", "safe_close"]
