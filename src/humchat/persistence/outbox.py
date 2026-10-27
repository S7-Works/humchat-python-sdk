from __future__ import annotations

import copy
import os
from collections.abc import Mapping
from typing import Any

from ..errors import PersistenceError
from ..types import SendReceipt
from .file import FileDocument
from .model import empty_document, key_value, mark_entry_sent, normalized_request, pending_entries, prepare_entry, scope_value
from .types import OutboxEntry, PersistenceScope


class MemoryOutboxStore:
    """Process-local outbox. Use a durable adapter when sends must survive a restart."""

    def __init__(self) -> None:
        self._document = empty_document("outbox")
        self._closed = False

    def _check_open(self) -> None:
        if self._closed:
            raise PersistenceError("Persistence store is closed")

    async def prepare(
        self, scope: PersistenceScope, key: str, message: Mapping[str, Any], *, idempotency_key: str | None = None
    ) -> OutboxEntry:
        self._check_open()
        normalized = scope_value(scope)
        checked = key_value(key)
        return prepare_entry(self._document, normalized, checked, normalized_request(normalized, message, idempotency_key))

    async def mark_sent(self, scope: PersistenceScope, key: str, receipt: SendReceipt) -> OutboxEntry:
        self._check_open()
        return mark_entry_sent(self._document, scope_value(scope), key_value(key), receipt)

    async def list_pending(self, scope: PersistenceScope) -> list[OutboxEntry]:
        self._check_open()
        return pending_entries(self._document, scope_value(scope))

    async def close(self) -> None:
        self._closed = True


class FileOutboxStore:
    """Durable intent and receipt file; no network send happens inside this adapter."""

    def __init__(self, document: FileDocument) -> None:
        self._document = document

    @classmethod
    async def open(cls, path: str | os.PathLike[str], *, max_bytes: int | None = None) -> FileOutboxStore:
        return cls(await FileDocument.open(path, "outbox", max_bytes))

    async def prepare(
        self, scope: PersistenceScope, key: str, message: Mapping[str, Any], *, idempotency_key: str | None = None
    ) -> OutboxEntry:
        normalized = scope_value(scope)
        checked = key_value(key)
        request = normalized_request(normalized, message, idempotency_key)
        return await self._document.update(lambda document: prepare_entry(document, normalized, checked, request))

    async def mark_sent(self, scope: PersistenceScope, key: str, receipt: SendReceipt) -> OutboxEntry:
        normalized = scope_value(scope)
        checked = key_value(key)
        copied = copy.deepcopy(receipt)
        return await self._document.update(lambda document: mark_entry_sent(document, normalized, checked, copied))

    async def list_pending(self, scope: PersistenceScope) -> list[OutboxEntry]:
        normalized = scope_value(scope)
        return await self._document.read(lambda document: pending_entries(document, normalized))

    async def close(self) -> None:
        await self._document.close()
