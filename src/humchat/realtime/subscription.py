"""Observer handles. Several handles can share one channel's physical socket."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Protocol

from .._signal import AbortController, AbortSignal, budget, maybe_await, race_signal, spawn
from ..errors import AbortError, HumChatError, ProtocolError, UnsupportedFeatureError, with_intent
from ..types import AuthRequiredEvent, ConnectionStatus, HandlerErrorEvent

if TYPE_CHECKING:
    from ..persistence.types import CheckpointStore
    from ..types import DurableConsumer

EventListener = Callable[[Any], Any]

#: Every event name a subscription can emit.
EVENT_NAMES = frozenset(
    {
        "event",
        "checkpoint",
        "message",
        "message_updated",
        "message_deleted",
        "reaction_updated",
        "state_reconciled",
        "typing",
        "presence",
        "receipt",
        "call_invite",
        "connection",
        "resync",
        "recovery_incomplete",
        "recovery_complete",
        "session_revoked",
        "auth_required",
        "error",
        "handler_error",
        "unknown_event",
    }
)


class SubscriptionOwner(Protocol):
    status: ConnectionStatus

    def detach(self, handle: ChannelSubscription) -> None: ...

    async def resync(self) -> None: ...

    async def send_signal(self, kind: str, through: int | None = None) -> None: ...


def _copy_payload(event: str, payload: Any) -> Any:
    if isinstance(payload, HumChatError):
        return with_intent(payload)
    if isinstance(payload, (HandlerErrorEvent, AuthRequiredEvent)):
        return replace(payload, error=with_intent(payload.error))
    return copy.deepcopy(payload)


class ChannelSubscription:
    """One observer handle for a canonical channel."""

    def __init__(self, channel_id: str, owner: SubscriptionOwner, handler_timeout_ms: int) -> None:
        self.channel_id = channel_id
        self._owner = owner
        self._handler_timeout_ms = handler_timeout_ms
        self._closed = AbortController()
        self._listeners: dict[str, list[EventListener]] = {}
        self._notifications: dict[str, Any] = {}
        self._notifying: set[str] = set()
        loop = asyncio.get_running_loop()
        self._ready: asyncio.Future[None] = loop.create_future()
        # A rejected readiness future is also observable through error events.
        self._ready.add_done_callback(lambda future: future.cancelled() or future.exception())

    def __repr__(self) -> str:
        return f"ChannelSubscription(channel_id={self.channel_id!r}, status={self.status!r})"

    @property
    def ready(self) -> asyncio.Future[None]:
        """Resolves when the channel is live; raises on close or a terminal error.

        Cancelling one awaiter never cancels readiness for other awaiters.
        """
        return asyncio.shield(self._ready)

    @property
    def status(self) -> ConnectionStatus:
        return "closed" if self._closed.signal.aborted else self._owner.status

    def on(self, event: str, listener: EventListener) -> ChannelSubscription:
        """Register a sync or async listener. Handlers for one channel run serially."""
        self._check_open()
        if event not in EVENT_NAMES:
            from ..errors import ValidationError

            raise ValidationError(f"Unknown subscription event: {event}")
        self._listeners.setdefault(event, []).append(listener)
        return self

    def off(self, event: str, listener: EventListener) -> ChannelSubscription:
        listeners = self._listeners.get(event)
        if listeners and listener in listeners:
            listeners.remove(listener)
        return self

    async def typing(self) -> None:
        self._check_open()
        await self._owner.send_signal("typing")

    def consume(self, handler: DurableConsumer, *, consumer_id: str, checkpoint_store: CheckpointStore) -> ChannelSubscription:
        """Register the channel's acknowledged durable consumer (before startup)."""
        self._check_open()
        consume = getattr(self._owner, "consume", None)
        if consume is None:
            raise UnsupportedFeatureError("Acknowledged consumption requires durable recovery")
        consume(self, handler, consumer_id, checkpoint_store)
        return self

    async def mark_read(self, *, through: int) -> None:
        self._check_open()
        await self._owner.send_signal("read", through)

    async def resync(self) -> None:
        self._check_open()
        await self._owner.resync()

    async def close(self) -> None:
        if self._closed.signal.aborted:
            return
        error = AbortError("Subscription is closed")
        self._closed.abort(error)
        if not self._ready.done():
            self._ready.set_exception(error)
        self._owner.detach(self)
        self._listeners.clear()
        self._notifications.clear()

    def _check_open(self) -> None:
        if self._closed.signal.aborted or self._owner.status == "closed":
            raise AbortError("Subscription is closed")

    # --- internal ---------------------------------------------------------------------------

    def resolve_ready(self) -> None:
        if not self._closed.signal.aborted and not self._ready.done():
            self._ready.set_result(None)

    def reject_ready(self, error: HumChatError) -> None:
        if not self._ready.done():
            self._ready.set_exception(with_intent(error, channel_id=self.channel_id))

    def notify(self, event: str, payload: Any) -> None:
        """Control notifications retain at most one active and one latest value per kind."""
        if self._closed.signal.aborted:
            return
        self._notifications[event] = payload
        if event in self._notifying:
            return
        self._notifying.add(event)

        async def run() -> None:
            try:
                while not self._closed.signal.aborted and event in self._notifications:
                    latest = self._notifications.pop(event)
                    await self.dispatch(event, latest, self._closed.signal, False)
            finally:
                self._notifying.discard(event)
                self._notifications.pop(event, None)

        spawn(run())

    async def dispatch(self, event: str, payload: Any, signal: AbortSignal, fail_on_handler: bool = True) -> None:
        """Application delivery is serialized and awaited by the channel worker."""
        for listener in list(self._listeners.get(event, [])):
            if self._closed.signal.aborted or signal.aborted:
                return
            lifetime = budget(self._handler_timeout_ms, [signal, self._closed.signal])
            try:
                value = _copy_payload(event, payload)

                async def invoke(listener: EventListener = listener, value: Any = value, lifetime_signal: AbortSignal = lifetime.signal) -> Any:
                    if self._closed.signal.aborted or lifetime_signal.aborted:
                        return None
                    return await maybe_await(listener(value))

                await race_signal(invoke(), lifetime.signal)
            except Exception:
                if self._closed.signal.aborted or signal.aborted:
                    return
                error = ProtocolError("Event handler failed or exceeded its deadline", channel_id=self.channel_id)
                if event not in ("handler_error", "error"):
                    self.notify("handler_error", HandlerErrorEvent(event=event, error=error))
                if fail_on_handler:
                    raise error from None
            finally:
                lifetime.dispose()
