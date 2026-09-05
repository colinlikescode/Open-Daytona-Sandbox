"""Sync SDK: :class:`SandboxPilot` (client) and :class:`Sandbox`.

    with Sandbox.create() as sb:
        print(sb.run("echo hi").stdout)

Same surface as the async SDK, executed on a background event loop.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Any

from sandboxpilot.schemas.commands import CommandEvent, CommandInfo, CommandLogs, CommandResult
from sandboxpilot.schemas.common import NetworkPolicy
from sandboxpilot.schemas.operations import Operation
from sandboxpilot.schemas.pool import PoolCreateRequest, PoolUpdateRequest, WorkerPool
from sandboxpilot.schemas.sandbox import SandboxCreateRequest, SandboxInfo
from sandboxpilot.schemas.template import Template
from sandboxpilot.schemas.worker import WorkerView
from sandboxpilot.sdk._portal import Portal
from sandboxpilot.sdk.asyncio import AsyncCommand, AsyncSandbox, AsyncSandboxPilot


class SandboxPilot:
    """Sync client for the control plane API."""

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        *,
        autostart: bool = True,
        timeout: float | None = None,
    ) -> None:
        self._portal = Portal()
        self._async = self._portal.call(_construct(url, token, autostart, timeout))

    @property
    def url(self) -> str:
        return self._async.url

    def close(self) -> None:
        self._portal.call(self._async.aclose())
        self._portal.close()

    def __enter__(self) -> SandboxPilot:
        return self

    def __exit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    # system
    def health(self) -> dict[str, Any]:
        return self._portal.call(self._async.health())

    def status(self) -> dict[str, Any]:
        return self._portal.call(self._async.status())

    def doctor(self) -> dict[str, Any]:
        return self._portal.call(self._async.doctor())

    def cleanup(self, *, terminate_workers: bool = False) -> dict[str, Any]:
        return self._portal.call(self._async.cleanup(terminate_workers=terminate_workers))

    def metrics(self) -> str:
        return self._portal.call(self._async.metrics())

    # pools
    def list_pools(self) -> list[WorkerPool]:
        return self._portal.call(self._async.list_pools())

    def get_pool(self, ref: str) -> WorkerPool:
        return self._portal.call(self._async.get_pool(ref))

    def create_pool(self, request: PoolCreateRequest) -> WorkerPool:
        return self._portal.call(self._async.create_pool(request))

    def update_pool(self, ref: str, request: PoolUpdateRequest) -> WorkerPool:
        return self._portal.call(self._async.update_pool(ref, request))

    def delete_pool(self, ref: str, *, force: bool = False) -> None:
        self._portal.call(self._async.delete_pool(ref, force=force))

    def pool_up(self, ref: str, *, workers: int | None = None) -> list[Operation]:
        return self._portal.call(self._async.pool_up(ref, workers=workers))

    def pool_down(self, ref: str, *, force: bool = False) -> dict[str, Any]:
        return self._portal.call(self._async.pool_down(ref, force=force))

    # workers
    def list_workers(self, pool: str | None = None) -> list[WorkerView]:
        return self._portal.call(self._async.list_workers(pool))

    def get_worker(self, ref: str) -> WorkerView:
        return self._portal.call(self._async.get_worker(ref))

    def drain_worker(self, ref: str, *, terminate_when_empty: bool = True) -> WorkerView:
        return self._portal.call(
            self._async.drain_worker(ref, terminate_when_empty=terminate_when_empty)
        )

    def remove_worker(self, ref: str, *, force: bool = False) -> WorkerView:
        return self._portal.call(self._async.remove_worker(ref, force=force))

    # operations / images / templates
    def get_operation(self, op_id: str, *, wait: float = 0) -> Operation:
        return self._portal.call(self._async.get_operation(op_id, wait=wait))

    def wait_operation(self, op_id: str, *, timeout: float = 900) -> Operation:
        return self._portal.call(self._async.wait_operation(op_id, timeout=timeout))

    def preload_image(self, reference: str, *, pool: str | None = None) -> Operation:
        return self._portal.call(self._async.preload_image(reference, pool=pool))

    def list_images(self, *, worker: str | None = None) -> list[dict[str, Any]]:
        return self._portal.call(self._async.list_images(worker=worker))

    def list_templates(self) -> list[Template]:
        return self._portal.call(self._async.list_templates())

    def add_template(self, template: Template) -> Template:
        return self._portal.call(self._async.add_template(template))

    def remove_template(self, name: str) -> None:
        self._portal.call(self._async.remove_template(name))

    # sandboxes
    def list_sandboxes(self, *, pool: str | None = None, all: bool = False) -> list[SandboxInfo]:
        return self._portal.call(self._async.list_sandboxes(pool=pool, all=all))

    def get_sandbox_info(self, ref: str) -> SandboxInfo:
        return self._portal.call(self._async.get_sandbox_info(ref))

    def create_sandbox(
        self, request: SandboxCreateRequest, *, idempotency_key: str | None = None
    ) -> SandboxInfo:
        return self._portal.call(
            self._async.create_sandbox(request, idempotency_key=idempotency_key)
        )

    def kill_sandbox(self, ref: str) -> SandboxInfo:
        return self._portal.call(self._async.kill_sandbox(ref))


async def _construct(
    url: str | None, token: str | None, autostart: bool, timeout: float | None
) -> AsyncSandboxPilot:
    # The sync client connects eagerly so configuration/autostart errors surface
    # from the constructor, as callers of a blocking API expect.
    client = AsyncSandboxPilot(url, token, autostart=autostart, timeout=timeout)
    await client.connect()
    return client


class Sandbox:
    """A handle to one sandbox (sync)."""

    def __init__(self, client: SandboxPilot, inner: AsyncSandbox, *, owns_client: bool) -> None:
        self._client = client
        self._inner = inner
        self._owns_client = owns_client

    @classmethod
    def create(
        cls,
        image: str | None = None,
        *,
        pool: str | None = None,
        template: str | None = None,
        cpus: float | None = None,
        memory: str | int | None = None,
        timeout: str | int | None = None,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        network: NetworkPolicy | str | None = None,
        labels: dict[str, str] | None = None,
        metadata: dict[str, str] | None = None,
        create_timeout: float | None = None,
        idempotency_key: str | None = None,
        client: SandboxPilot | None = None,
        **extra: Any,
    ) -> Sandbox:
        owns = client is None
        client = client or SandboxPilot()
        try:
            inner = client._portal.call(
                AsyncSandbox.create(
                    image,
                    pool=pool,
                    template=template,
                    cpus=cpus,
                    memory=memory,
                    timeout=timeout,
                    env=env,
                    workdir=workdir,
                    network=network,
                    labels=labels,
                    metadata=metadata,
                    create_timeout=create_timeout,
                    idempotency_key=idempotency_key,
                    client=client._async,
                    **extra,
                )
            )
        except BaseException:
            if owns:
                client.close()
            raise
        return cls(client, inner, owns_client=owns)

    @classmethod
    def connect(cls, sandbox_id: str, *, client: SandboxPilot | None = None) -> Sandbox:
        owns = client is None
        client = client or SandboxPilot()
        inner = client._portal.call(AsyncSandbox.connect(sandbox_id, client=client._async))
        return cls(client, inner, owns_client=owns)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        try:
            self.kill()
        finally:
            if self._owns_client:
                self._client.close()

    @property
    def id(self) -> str:
        return self._inner.id

    @property
    def short_id(self) -> str:
        return self._inner.short_id

    @property
    def info(self) -> SandboxInfo:
        return self._inner.info

    def refresh(self) -> SandboxInfo:
        return self._client._portal.call(self._inner.refresh())

    def kill(self) -> None:
        self._client._portal.call(self._inner.kill())

    def set_timeout(self, timeout: str | int) -> SandboxInfo:
        return self._client._portal.call(self._inner.set_timeout(timeout))

    def run(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
        check: bool = False,
    ) -> CommandResult:
        return self._client._portal.call(
            self._inner.run(command, env=env, cwd=cwd, timeout=timeout, user=user, check=check)
        )

    def run_background(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
    ) -> Command:
        inner = self._client._portal.call(
            self._inner.run_background(command, env=env, cwd=cwd, timeout=timeout, user=user)
        )
        return Command(self, inner)

    def stream(
        self,
        command: str | list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        user: str | None = None,
    ) -> Iterator[CommandEvent]:
        return self.run_background(command, env=env, cwd=cwd, timeout=timeout, user=user).events()

    def write(self, path: str, content: str | bytes, *, mode: int | None = None) -> None:
        self._client._portal.call(self._inner.write(path, content, mode=mode))

    def read(self, path: str) -> bytes:
        return self._client._portal.call(self._inner.read(path))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self._client._portal.call(self._inner.read_text(path, encoding))

    def upload(
        self, local: str | os.PathLike[str], remote: str, *, mode: int | None = None
    ) -> None:
        self._client._portal.call(self._inner.upload(local, remote, mode=mode))

    def download(self, remote: str, local: str | os.PathLike[str]) -> Path:
        return self._client._portal.call(self._inner.download(remote, local))

    def get_url(self, port: int, *, expires_in: int | None = None) -> str:
        return self._client._portal.call(self._inner.get_url(port, expires_in=expires_in))


class Command:
    def __init__(self, sandbox: Sandbox, inner: AsyncCommand) -> None:
        self.sandbox = sandbox
        self._inner = inner

    @property
    def id(self) -> str:
        return self._inner.id

    @property
    def info(self) -> CommandInfo:
        return self._inner.info

    def refresh(self) -> CommandInfo:
        return self.sandbox._client._portal.call(self._inner.refresh())

    def wait(self) -> CommandResult:
        return self.sandbox._client._portal.call(self._inner.wait())

    def logs(self) -> CommandLogs:
        return self.sandbox._client._portal.call(self._inner.logs())

    def kill(self) -> CommandInfo:
        return self.sandbox._client._portal.call(self._inner.kill())

    def events(self, from_seq: int = 0) -> Iterator[CommandEvent]:
        return self.sandbox._client._portal.iterate(self._inner.events(from_seq))
