"""Streaming tar helpers so single files can be transferred through archive APIs without buffering."""

from __future__ import annotations

import io
import posixpath
import tarfile
import time
from collections.abc import AsyncIterator

from sandboxpilot.errors import FileTransferError

BLOCK = 512


def tar_header(name: str, size: int, mode: int = 0o644, is_dir: bool = False) -> bytes:
    info = tarfile.TarInfo(name)
    info.size = 0 if is_dir else size
    info.mode = mode
    info.mtime = int(time.time())
    info.type = tarfile.DIRTYPE if is_dir else tarfile.REGTYPE
    return info.tobuf(format=tarfile.PAX_FORMAT)


def _padding(size: int) -> bytes:
    rem = size % BLOCK
    return b"\0" * (BLOCK - rem) if rem else b""


async def single_file_tar(
    name: str, content: AsyncIterator[bytes], size: int, mode: int = 0o644
) -> AsyncIterator[bytes]:
    """Wrap a byte stream of known ``size`` into a tar archive with one regular file."""
    yield tar_header(name, size, mode)
    sent = 0
    async for chunk in content:
        sent += len(chunk)
        if sent > size:
            raise FileTransferError("upload body exceeded declared size")
        yield chunk
    if sent != size:
        raise FileTransferError(f"upload body was {sent} bytes but {size} were declared")
    yield _padding(size)
    yield b"\0" * (BLOCK * 2)


def single_file_tar_bytes(name: str, data: bytes, mode: int = 0o644) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mode = mode
        info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class _Buffer:
    def __init__(self, source: AsyncIterator[bytes]) -> None:
        self.source = source
        self.buf = bytearray()
        self.eof = False

    async def read(self, n: int) -> bytes:
        while len(self.buf) < n and not self.eof:
            try:
                chunk = await self.source.__anext__()
            except StopAsyncIteration:
                self.eof = True
                break
            self.buf.extend(chunk)
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    async def skip(self, n: int) -> None:
        while n > 0:
            chunk = await self.read(min(n, 1 << 16))
            if not chunk:
                return
            n -= len(chunk)


def _parse_header(block: bytes) -> tarfile.TarInfo | None:
    if len(block) < BLOCK or block == b"\0" * BLOCK:
        return None
    try:
        return tarfile.TarInfo.frombuf(block, tarfile.ENCODING, "surrogateescape")
    except tarfile.TarError as exc:
        raise FileTransferError(f"invalid archive: {exc}") from exc


async def first_regular_file(archive: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Yield the content of the first regular file in a streamed tar archive.

    Pax/GNU long-name extension headers are skipped. Raises if the archive
    contains no regular file (for example when ``path`` is a directory).
    """
    buf = _Buffer(archive)
    while True:
        header = await buf.read(BLOCK)
        info = _parse_header(header)
        if info is None:
            raise FileTransferError(
                "path is not a regular file", details={"reason": "no_regular_file"}
            )
        padded = info.size + (-info.size % BLOCK)
        if info.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.CONTTYPE):
            remaining = info.size
            while remaining > 0:
                chunk = await buf.read(min(remaining, 1 << 16))
                if not chunk:
                    raise FileTransferError("archive ended prematurely")
                remaining -= len(chunk)
                yield chunk
            await buf.skip(padded - info.size)
            return
        if info.type == tarfile.DIRTYPE:
            raise FileTransferError(
                "path is a directory; request archive=true to download directories",
                details={"reason": "is_directory"},
            )
        # Extended headers (pax/GNU long name) precede the real entry: skip their payload.
        await buf.skip(padded)


def basename_and_parent(path: str) -> tuple[str, str]:
    norm = posixpath.normpath(path)
    return posixpath.basename(norm), posixpath.dirname(norm) or "/"
