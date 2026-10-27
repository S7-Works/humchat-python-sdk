from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypedDict, runtime_checkable

from ..errors import PersistenceError

if TYPE_CHECKING:
    from ..types import MessagePriority, SendReceipt


@dataclass(frozen=True)
class PersistenceScope:
    """All components participate in storage identity. Never reuse a consumer ID for unrelated work."""

    server_origin: str
    org_id: str
    actor_id: str
    consumer_id: str
    channel_id: str


class OutboxMessage(TypedDict, total=False):
    """The send intent an outbox binds to a key (``messages.send`` arguments)."""

    channel_id: str
    content: str
    id: str
    thread_root_id: str | None
    priority: MessagePriority


@dataclass
class OutboxIntent:
    id: str
    channel_id: str
    content: str
    thread_root_id: str | None
    priority: MessagePriority


@dataclass
class OutboxEntry:
    key: str
    intent: OutboxIntent
    idempotency_key: str
    status: Literal["pending", "sent"]
    receipt: SendReceipt | None = None


@runtime_checkable
class CheckpointStore(Protocol):
    async def load(self, scope: PersistenceScope) -> str | None: ...

    async def compare_and_set(self, scope: PersistenceScope, expected_cursor: str | None, next_cursor: str) -> bool:
        """False means another worker changed this checkpoint. Do not acknowledge its work."""
        ...

    async def close(self) -> None: ...


@runtime_checkable
class OutboxStore(Protocol):
    async def prepare(
        self, scope: PersistenceScope, key: str, message: Mapping[str, Any], *, idempotency_key: str | None = None
    ) -> OutboxEntry:
        """A key binds one normalized caller intent; omitted ID/key are allocated and persisted once."""
        ...

    async def mark_sent(self, scope: PersistenceScope, key: str, receipt: SendReceipt) -> OutboxEntry: ...

    async def list_pending(self, scope: PersistenceScope) -> list[OutboxEntry]: ...

    async def close(self) -> None: ...


__all__ = [
    "CheckpointStore",
    "OutboxEntry",
    "OutboxIntent",
    "OutboxMessage",
    "OutboxStore",
    "PersistenceError",
    "PersistenceScope",
]
