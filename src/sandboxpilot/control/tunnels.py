"""SSH tunnels from the control plane to worker daemons.

The worker API listens on ``127.0.0.1:9417`` on the VM and is never exposed
publicly. The control plane keeps one persistent OpenSSH port-forward per
active worker, using the SSH configuration SkyPilot generates for the cluster.
Only argument arrays are used; no shell strings.
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from sandboxpilot.config import defaults as d
from sandboxpilot.errors import WorkerUnavailableError
from sandboxpilot.schemas.worker import WorkerRecord
from sandboxpilot.utils.logging import get_logger

if TYPE_CHECKING:
    from sandboxpilot.providers.fake import FakeFleet

log = get_logger("control.tunnels")


class TunnelManager(ABC):
    @abstractmethod
    async def ensure(self, worker: WorkerRecord) -> str:
        """Return a base URL through which the worker API is reachable, (re)connecting if needed."""

    @abstractmethod
    async def is_alive(self, worker_id: str) -> bool: ...

    @abstractmethod
    async def close(self, worker_id: str) -> None: ...

    @abstractmethod
    async def close_all(self) -> None: ...

    def endpoint(self, worker_id: str) -> str | None:
        return None


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    except (OSError, TimeoutError):
        return False
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    return True


@dataclass
class SSHTunnel:
    worker_id: str
    alias: str
    local_port: int
    process: asyncio.subprocess.Process
    stderr: list[str] = field(default_factory=list)
    drain_task: asyncio.Task[None] | None = None

    @property
    def alive(self) -> bool:
        return self.process.returncode is None


class SSHTunnelManager(TunnelManager):
    def __init__(
        self,
        *,
        remote_port: int = d.WORKER_PORT,
        ssh_config_dir: Path | None = None,
        connect_timeout: float = 30.0,
        ssh_binary: str | None = None,
    ) -> None:
        self.remote_port = remote_port
        self.ssh_config_dir = ssh_config_dir or Path.home() / ".sky" / "generated" / "ssh"
        self.connect_timeout = connect_timeout
        self.ssh_binary = ssh_binary or shutil.which("ssh") or "ssh"
        self._tunnels: dict[str, SSHTunnel] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, worker_id: str) -> asyncio.Lock:
        return self._locks.setdefault(worker_id, asyncio.Lock())

    def endpoint(self, worker_id: str) -> str | None:
        t = self._tunnels.get(worker_id)
        return f"http://127.0.0.1:{t.local_port}" if t and t.alive else None

    def ssh_command(
        self, alias: str, local_port: int, *, config_file: Path | None = None
    ) -> list[str]:
        argv = [
            self.ssh_binary,
            "-N",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{self.remote_port}",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=20",
        ]
        if config_file is not None:
            argv += ["-F", str(config_file)]
        argv.append(alias)
        return argv

    async def _config_file(self, alias: str) -> Path | None:
        config = self.ssh_config_dir / alias
        return config if await asyncio.to_thread(config.exists) else None

    async def ensure(self, worker: WorkerRecord) -> str:
        async with self._lock(worker.id):
            existing = self._tunnels.get(worker.id)
            if existing and existing.alive and await port_open(existing.local_port):
                return f"http://127.0.0.1:{existing.local_port}"
            if existing:
                await self._terminate(existing)
            alias = worker.provider_metadata.get("ssh_alias") or worker.provider_cluster
            port = (
                worker.tunnel_port
                if worker.tunnel_port and not await port_open(worker.tunnel_port)
                else free_port()
            )
            argv = self.ssh_command(alias, port, config_file=await self._config_file(alias))
            log.info(
                "opening SSH tunnel to %s on local port %d",
                alias,
                port,
                extra={"worker_id": worker.id},
            )
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            tunnel = SSHTunnel(worker_id=worker.id, alias=alias, local_port=port, process=process)
            tunnel.drain_task = asyncio.create_task(self._drain_stderr(tunnel))
            self._tunnels[worker.id] = tunnel
            deadline = asyncio.get_running_loop().time() + self.connect_timeout
            while asyncio.get_running_loop().time() < deadline:
                if not tunnel.alive:
                    break
                if await port_open(port):
                    worker.tunnel_port = port
                    return f"http://127.0.0.1:{port}"
                await asyncio.sleep(0.2)
            await self._terminate(tunnel)
            self._tunnels.pop(worker.id, None)
            detail = " ".join(tunnel.stderr)[-500:]
            raise WorkerUnavailableError(
                f"Could not establish SSH tunnel to worker {alias}.",
                hint=f"Check `ssh {alias}` works (SkyPilot manages this alias). ssh said: {detail or 'nothing'}",
                details={"worker_id": worker.id, "alias": alias},
            )

    async def _drain_stderr(self, tunnel: SSHTunnel) -> None:
        assert tunnel.process.stderr is not None
        try:
            while True:
                line = await tunnel.process.stderr.readline()
                if not line:
                    break
                tunnel.stderr.append(line.decode(errors="replace").strip())
                del tunnel.stderr[:-20]
        except Exception:
            pass

    async def is_alive(self, worker_id: str) -> bool:
        t = self._tunnels.get(worker_id)
        return bool(t and t.alive and await port_open(t.local_port))

    async def _terminate(self, tunnel: SSHTunnel) -> None:
        if tunnel.alive:
            with contextlib.suppress(ProcessLookupError):
                tunnel.process.terminate()
            try:
                await asyncio.wait_for(tunnel.process.wait(), 5)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    tunnel.process.kill()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(tunnel.process.wait(), 5)

    async def close(self, worker_id: str) -> None:
        tunnel = self._tunnels.pop(worker_id, None)
        if tunnel:
            await self._terminate(tunnel)

    async def close_all(self) -> None:
        for worker_id in list(self._tunnels):
            await self.close(worker_id)


class FakeTunnelManager(TunnelManager):
    """Resolves fake worker endpoints directly; supports simulated tunnel death."""

    def __init__(self, fleet: FakeFleet) -> None:
        self.fleet = fleet
        self.connected: dict[str, str] = {}
        self.killed: set[str] = set()
        self.connect_count: dict[str, int] = {}

    def endpoint(self, worker_id: str) -> str | None:
        return None if worker_id in self.killed else self.connected.get(worker_id)

    async def ensure(self, worker: WorkerRecord) -> str:
        vm = self.fleet.get(worker.provider_cluster)
        if vm is None or vm.status != "UP" or vm.port is None:
            raise WorkerUnavailableError(
                f"Could not establish SSH tunnel to worker {worker.provider_cluster}: VM not reachable",
                details={"worker_id": worker.id},
            )
        if worker.id not in self.connected or worker.id in self.killed:
            self.connect_count[worker.id] = self.connect_count.get(worker.id, 0) + 1
        self.killed.discard(worker.id)
        self.connected[worker.id] = vm.endpoint
        worker.tunnel_port = vm.port
        return vm.endpoint

    async def is_alive(self, worker_id: str) -> bool:
        return worker_id in self.connected and worker_id not in self.killed

    def kill(self, worker_id: str) -> None:
        self.killed.add(worker_id)

    async def close(self, worker_id: str) -> None:
        self.connected.pop(worker_id, None)
        self.killed.discard(worker_id)

    async def close_all(self) -> None:
        self.connected.clear()
        self.killed.clear()
