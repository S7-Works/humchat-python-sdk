from __future__ import annotations

import copy
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

from .._signal import AbortSignal
from ..errors import ValidationError
from ..persistence.types import OutboxStore, PersistenceScope
from ..types import Identity, SendReceipt

if TYPE_CHECKING:
    from .messages import MessagesResource

_CONSUMER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class OutboxResource:
    """Persist exact send intent before network work; application checkpoints follow confirmed delivery."""

    def __init__(
        self,
        origin: str,
        connect: Callable[[AbortSignal | None, int | None], Awaitable[Identity]],
        messages: MessagesResource,
    ) -> None:
        self._origin = origin
        self._connect = connect
        self._messages = messages

    async def _scope(self, consumer_id: Any, channel_id: Any, signal: AbortSignal | None, timeout_ms: int | None) -> PersistenceScope:
        if not isinstance(consumer_id, str) or not _CONSUMER.fullmatch(consumer_id):
            raise ValidationError("Invalid consumerId")
        identity = await self._connect(signal, timeout_ms)
        return PersistenceScope(
            server_origin=self._origin, org_id=identity.org_id, actor_id=identity.actor_id, consumer_id=consumer_id, channel_id=channel_id
        )

    async def send(
        self,
        *,
        consumer_id: str,
        key: str,
        store: OutboxStore,
        message: Mapping[str, Any],
        idempotency_key: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> SendReceipt:
        """Bind ``key`` to ``message`` durably, then send it (or return the recorded receipt)."""
        channel_id = message.get("channel_id") if isinstance(message, Mapping) else None
        scope = await self._scope(consumer_id, channel_id, signal, timeout_ms)
        entry = await store.prepare(scope, key, message, idempotency_key=idempotency_key)
        if entry.status == "sent" and entry.receipt is not None:
            return copy.deepcopy(entry.receipt)
        receipt = await self._send(entry, signal, timeout_ms)
        await store.mark_sent(scope, key, receipt)
        return receipt

    async def flush(
        self,
        *,
        consumer_id: str,
        channel_id: str,
        store: OutboxStore,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> list[SendReceipt]:
        """Send every pending intent for this consumer/channel in stored order."""
        scope = await self._scope(consumer_id, channel_id, signal, timeout_ms)
        results: list[SendReceipt] = []
        for entry in await store.list_pending(scope):
            receipt = await self._send(entry, signal, timeout_ms)
            await store.mark_sent(scope, entry.key, receipt)
            results.append(receipt)
        return results

    async def _send(self, entry: Any, signal: AbortSignal | None, timeout_ms: int | None) -> SendReceipt:
        intent = entry.intent
        return await self._messages.send(
            channel_id=intent.channel_id,
            content=intent.content,
            id=intent.id,
            thread_root_id=intent.thread_root_id,
            priority=intent.priority,
            idempotency_key=entry.idempotency_key,
            signal=signal,
            timeout_ms=timeout_ms,
        )
