"""Serialized single-process JSON document with an exclusive sibling lock file."""

from __future__ import annotations

import asyncio
import copy
import datetime
import json
import os
import stat
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from .._util import is_safe_int, json_dumps
from ..errors import ConfigurationError, PersistenceError
from .model import empty_document, parse_document

T = TypeVar("T")


def _bounded_read(path: str, max_bytes: int) -> bytes:
    with open(path, "rb") as handle:
        if os.fstat(handle.fileno()).st_size > max_bytes:
            raise PersistenceError("Persistence file exceeds maxBytes")
        chunks: list[bytes] = []
        total = 0
        # Read through the same descriptor, checking every chunk if the file grows.
        while True:
            chunk = handle.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise PersistenceError("Persistence file exceeds maxBytes")
            chunks.append(chunk)
        return b"".join(chunks)


class FileDocument:
    """Internal document engine; the lock is held for the store's full lifetime.

    Intended for a local filesystem, not a distributed lease. Contents are fsynced before
    an atomic ``os.replace``; power-loss durability of the directory entry is not guaranteed.
    """

    def __init__(self, path: str, lock_fd: int, owner: str, max_bytes: int, document: dict[str, Any]) -> None:
        self._path = path
        self._lock_fd: int | None = lock_fd
        self._owner = owner
        self._max_bytes = max_bytes
        self._document = document
        self._mutex = asyncio.Lock()
        self._closing = False
        self._close_task: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return "FileDocument()"

    @classmethod
    async def open(cls, path: str | os.PathLike[str], kind: str, max_bytes: int | None = None) -> FileDocument:
        if isinstance(path, os.PathLike):
            path = os.fspath(path)
        if not isinstance(path, str) or not path.strip() or "\0" in path:
            raise ConfigurationError("A persistence file path is required")
        limit = 64 * 1024 * 1024 if max_bytes is None else max_bytes
        if not is_safe_int(limit) or limit < 1024 or limit > 1024 * 1024 * 1024:
            raise ConfigurationError("Persistence max_bytes must be an integer between 1024 and 1073741824")
        return await asyncio.to_thread(cls._open_sync, path, kind, int(limit))

    @classmethod
    def _open_sync(cls, path: str, kind: str, max_bytes: int) -> FileDocument:
        try:
            absolute = os.path.abspath(path)
            directory = os.path.dirname(absolute)
            os.makedirs(directory, mode=0o700, exist_ok=True)
            canonical = os.path.join(os.path.realpath(directory), os.path.basename(absolute))
            try:
                if stat.S_ISLNK(os.lstat(canonical).st_mode):
                    raise PersistenceError("Persistence files must not be symbolic links")
            except FileNotFoundError:
                pass
        except Exception:
            raise PersistenceError("Could not prepare the persistence file location") from None
        try:
            lock_fd = os.open(canonical + ".lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        except FileExistsError:
            raise PersistenceError("Persistence store is locked; close its owner or manually inspect a stale lock") from None
        except OSError:
            raise PersistenceError("Could not acquire the persistence file lock") from None
        owner = str(uuid.uuid4())
        try:
            marker = json_dumps(
                {
                    "schema": 1,
                    "owner": owner,
                    "pid": os.getpid(),
                    "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                }
            ).encode("utf-8")
            os.write(lock_fd, marker)
            os.fsync(lock_fd)
            document = empty_document(kind)
            try:
                raw = _bounded_read(canonical, max_bytes)
            except FileNotFoundError:
                raw = None
            if raw is not None:
                try:
                    parsed = json.loads(raw.decode("utf-8", "strict"))
                except (ValueError, UnicodeDecodeError, RecursionError):
                    raise PersistenceError("Could not open a valid persistence file") from None
                document = parse_document(parsed, kind)
            return cls(canonical, lock_fd, owner, max_bytes, document)
        except Exception as error:
            try:
                os.close(lock_fd)
            except OSError:
                pass
            try:
                os.unlink(canonical + ".lock")
            except OSError:
                pass
            if isinstance(error, PersistenceError):
                raise
            raise PersistenceError("Could not open a valid persistence file") from None

    async def read(self, operation: Callable[[dict[str, Any]], T]) -> T:
        if self._closing:
            raise PersistenceError("Persistence store is closed")
        async with self._mutex:
            return operation(self._document)

    async def update(self, operation: Callable[[dict[str, Any]], T]) -> T:
        if self._closing:
            raise PersistenceError("Persistence store is closed")
        async with self._mutex:
            following = copy.deepcopy(self._document)
            result = operation(following)
            # Read-only repeats/CAS failures do not require another disk write.
            if json_dumps(following) != json_dumps(self._document):
                await asyncio.to_thread(self._write, following)
                self._document = following
            return result

    async def close(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.ensure_future(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        async with self._mutex:
            try:
                await asyncio.to_thread(self._release)
            except PersistenceError:
                raise
            except Exception:
                raise PersistenceError("Could not close the persistence store") from None

    def _release(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None
        try:
            with open(self._path + ".lock", encoding="utf-8") as handle:
                marker = json.load(handle)
            if not isinstance(marker, dict) or marker.get("owner") != self._owner:
                raise ValueError("different owner")
            os.unlink(self._path + ".lock")
        except Exception:
            raise PersistenceError("Persistence lock ownership changed; manual inspection is required") from None

    def _write(self, document: dict[str, Any]) -> None:
        contents = json_dumps(document).encode("utf-8", "surrogatepass")
        if len(contents) > self._max_bytes:
            raise PersistenceError("Persistence write exceeds maxBytes")
        temporary = f"{self._path}.tmp-{uuid.uuid4()}"
        fd: int | None = None
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
            view = memoryview(contents)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
            os.close(fd)
            fd = None
            # os.replace is atomic on supported local filesystems, including Windows.
            os.replace(temporary, self._path)
        except Exception:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise PersistenceError("Persistence write failed; the in-memory transaction was not committed") from None
