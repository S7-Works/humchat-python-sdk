from __future__ import annotations

import os
from typing import Any

from ..errors import PersistenceError
from .file import FileDocument
from .model import cursor_value, empty_document, scope_key, scope_value, scope_wire
from .types import PersistenceScope


class MemoryCheckpointStore:
    """Process-local checkpoints. Restarting loses all progress."""

    def __init__(self) -> None:
        self._document = empty_document("checkpoints")
        self._closed = False

    async def load(self, scope: PersistenceScope) -> str | None:
        if self._closed:
            raise PersistenceError("Persistence store is closed")
        record = self._document["records"].get(scope_key(scope_value(scope)))
        return None if record is None else record["cursor"]

    async def compare_and_set(self, scope: PersistenceScope, expected_cursor: str | None, next_cursor: str) -> bool:
        if self._closed:
            raise PersistenceError("Persistence store is closed")
        normalized = scope_value(scope)
        if expected_cursor is not None:
            cursor_value(expected_cursor)
        cursor_value(next_cursor)
        return _compare_and_set(self._document, normalized, expected_cursor, next_cursor)

    async def close(self) -> None:
        self._closed = True


def _compare_and_set(document: dict[str, Any], scope: PersistenceScope, expected: str | None, following: str) -> bool:
    key = scope_key(scope)
    record = document["records"].get(key)
    if (None if record is None else record["cursor"]) != expected:
        return False
    document["records"][key] = {"scope": scope_wire(scope), "cursor": following}
    return True


class FileCheckpointStore:
    """Single-process file store. An abandoned ``.lock`` requires manual inspection/removal."""

    def __init__(self, document: FileDocument) -> None:
        self._document = document

    @classmethod
    async def open(cls, path: str | os.PathLike[str], *, max_bytes: int | None = None) -> FileCheckpointStore:
        """``max_bytes`` bounds serialized state (default 64 MiB) for reads and writes."""
        return cls(await FileDocument.open(path, "checkpoints", max_bytes))

    async def load(self, scope: PersistenceScope) -> str | None:
        normalized = scope_value(scope)

        def read(document: dict[str, Any]) -> str | None:
            record = document["records"].get(scope_key(normalized))
            return None if record is None else record["cursor"]

        return await self._document.read(read)

    async def compare_and_set(self, scope: PersistenceScope, expected_cursor: str | None, next_cursor: str) -> bool:
        normalized = scope_value(scope)
        if expected_cursor is not None:
            cursor_value(expected_cursor)
        cursor_value(next_cursor)
        return await self._document.update(lambda document: _compare_and_set(document, normalized, expected_cursor, next_cursor))

    async def close(self) -> None:
        await self._document.close()
