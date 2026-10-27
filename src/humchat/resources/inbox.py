from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .. import _clock
from .._signal import AbortController, any_signal, maybe_await, spawn
from .._util import is_int
from ..errors import AbortError, BackpressureError, HumChatError, ProtocolError, UnsupportedFeatureError, ValidationError, with_intent
from ..types import ConnectionStatus, ConsumerContext, DurableEvent, HandlerErrorEvent

if TYPE_CHECKING:
    from .._context import ResourceContext
    from ..persistence.types import CheckpointStore
    from ..realtime.manager import RealtimeClient
    from ..realtime.subscription import ChannelSubscription
    from ..types import DurableConsumer
    from .conversations import ConversationsResource

InboxStatus = Literal["starting", "watching", "blocked", "closed"]
_CONSUMER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass
class InboxChannelStats:
    channel_id: str
    status: ConnectionStatus


@dataclass
class InboxStats:
    conversations: int
    channels: list[InboxChannelStats]


def _settle_future(loop: asyncio.AbstractEventLoop) -> asyncio.Future[None]:
    future: asyncio.Future[None] = loop.create_future()
    future.add_done_callback(lambda done: done.cancelled() or done.exception())
    return future


class InboxWatch:
    """One managed consumer over the authenticated identity's conversations, including groups."""

    def __init__(
        self,
        context: ResourceContext,
        conversations: ConversationsResource,
        realtime: RealtimeClient,
        handler: DurableConsumer,
        consumer_id: str,
        checkpoint_store: CheckpointStore,
        poll_interval_ms: int,
        max_conversations: int,
    ) -> None:
        self._context = context
        self._conversations = conversations
        self._realtime = realtime
        self._handler = handler
        self._consumer_id = consumer_id
        self._checkpoint_store = checkpoint_store
        self._poll_interval_ms = poll_interval_ms
        self._max_conversations = max_conversations
        self._status: InboxStatus = "starting"
        self._error: HumChatError | None = None
        self._last_channel_error: HumChatError | None = None
        self._cancel = AbortController()
        self._handles: dict[str, ChannelSubscription] = {}
        self._timer: Any = None
        self._refresh: asyncio.Future[None] | None = None
        loop = asyncio.get_running_loop()
        self._ready = _settle_future(loop)
        self._done = _settle_future(loop)
        loop.call_soon(self._first_refresh)

    def __repr__(self) -> str:
        return f"InboxWatch(status={self._status!r}, conversations={len(self._handles)})"

    def _first_refresh(self) -> None:
        if not self._cancel.signal.aborted:
            spawn(self.refresh())

    @property
    def ready(self) -> asyncio.Future[None]:
        """Initial discovery and consumer registration completed; channel replay can still be running."""
        return asyncio.shield(self._ready)

    @property
    def done(self) -> asyncio.Future[None]:
        """Raises on a terminal discovery/channel/handler error; resolves when explicitly closed."""
        return asyncio.shield(self._done)

    @property
    def status(self) -> InboxStatus:
        return self._status

    @property
    def error(self) -> HumChatError | None:
        return with_intent(self._error) if self._error is not None else None

    @property
    def last_channel_error(self) -> HumChatError | None:
        """Most recent channel error, including transient errors handled by the reconnect policy."""
        return with_intent(self._last_channel_error) if self._last_channel_error is not None else None

    def get_stats(self) -> InboxStats:
        return InboxStats(
            conversations=len(self._handles),
            channels=[InboxChannelStats(channel_id=channel_id, status=handle.status) for channel_id, handle in self._handles.items()],
        )

    def refresh(self) -> asyncio.Future[None]:
        """Coalesces concurrent refreshes. Periodic requests never overlap."""
        loop = asyncio.get_running_loop()
        if self._status == "closed":
            return _rejected(loop, AbortError("Inbox watcher is closed"))
        if self._status == "blocked":
            assert self._error is not None
            return _rejected(loop, self._error)
        if self._refresh is not None:
            # Coalesced callers receive the identical awaitable, like the Node promise.
            return self._refresh
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        async def run() -> None:
            try:
                await self._discover()
                if self._cancel.signal.aborted:
                    raise AbortError("Inbox watcher is closed")
                self._status = "watching"
            except Exception as error:
                self._fail(error)
                raise (self._error or AbortError("Inbox watcher is closed")) from None
            finally:
                self._refresh = None
                if self._status == "watching" and not self._ready.done():
                    self._ready.set_result(None)
                if not self._cancel.signal.aborted:
                    self._timer = _clock.call_later(self._poll_interval_ms, self._periodic)

        task = spawn(run())
        # Shield once so one awaiter's cancellation cannot cancel the shared discovery.
        outer = asyncio.shield(task)
        outer.add_done_callback(lambda done: done.cancelled() or done.exception())
        self._refresh = outer
        return outer

    def _periodic(self) -> None:
        self._timer = None
        spawn(self.refresh())

    async def close(self) -> None:
        if self._status == "closed":
            return
        self._status = "closed"
        error = AbortError("Inbox watcher is closed")
        self._cancel.abort(error)
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._ready.done():
            self._ready.set_exception(error)
        await self._close_handles()
        if not self._done.done():
            self._done.set_result(None)

    async def _close_handles(self) -> None:
        handles = list(self._handles.values())
        self._handles.clear()
        await asyncio.gather(*(handle.close() for handle in handles))

    def _fail(self, error: BaseException) -> None:
        if self._cancel.signal.aborted:
            return
        self._error = with_intent(error) if isinstance(error, HumChatError) else ProtocolError("Inbox operation failed")
        self._status = "blocked"
        self._cancel.abort(self._error)
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._ready.done():
            self._ready.set_exception(self._error)
        if not self._done.done():
            self._done.set_exception(self._error)
        spawn(self._close_handles())

    async def _discover(self) -> None:
        signal = self._cancel.signal
        capabilities = await self._context.capabilities(signal, None)
        if not capabilities.messaging.durable_replay or 1 not in capabilities.messaging.event_protocol_versions:
            raise UnsupportedFeatureError("Inbox watching requires durable event replay")
        me = await self._context.connect(signal, None)
        conversations = await self._conversations.list(signal=signal)
        if signal.aborted:
            raise AbortError("Inbox watcher is closed")
        if len(conversations) > self._max_conversations:
            raise BackpressureError("Inbox conversation limit reached")
        for conversation in conversations:
            ids = [member.actor_id for member in conversation.members]
            if me.actor_id not in ids or conversation.member_count != len(conversation.members) or len(set(ids)) != len(ids):
                raise ProtocolError("Inbox conversation membership mismatch", channel_id=conversation.channel_id)
        present = {conversation.channel_id for conversation in conversations}
        retiring = []
        for channel_id, handle in list(self._handles.items()):
            if channel_id not in present:
                del self._handles[channel_id]
                retiring.append(self._realtime.retire(handle, signal))
        await asyncio.gather(*retiring)
        for conversation in conversations:
            if signal.aborted:
                raise AbortError("Inbox watcher is closed")
            if conversation.channel_id in self._handles:
                continue
            self._watch(conversation.channel_id)

    def _watch(self, channel_id: str) -> None:
        handle = self._realtime.subscribe(channel_id, start="earliest")
        self._handles[channel_id] = handle

        def owned() -> bool:
            return not self._cancel.signal.aborted and self._handles.get(channel_id) is handle

        def on_error(error: HumChatError) -> None:
            if not owned():
                return
            self._last_channel_error = with_intent(error)
            if handle.status == "blocked":
                self._fail(error)

        def on_handler_error(event: HandlerErrorEvent) -> None:
            if owned():
                self._fail(event.error)

        handle.on("error", on_error)
        handle.on("handler_error", on_handler_error)

        async def consume(event: DurableEvent, context: ConsumerContext) -> Any:
            if not owned():
                raise AbortError("Inbox conversation is no longer watched")
            signal, detach = any_signal([context.signal, self._cancel.signal])
            try:
                return await maybe_await(self._handler(event, ConsumerContext(signal=signal, source=context.source)))
            finally:
                detach()

        handle.consume(consume, consumer_id=self._consumer_id, checkpoint_store=self._checkpoint_store)

        async def watch_ready() -> None:
            try:
                await handle.ready
            except Exception as error:
                if owned():
                    self._fail(error)

        spawn(watch_ready())


def _rejected(loop: asyncio.AbstractEventLoop, error: BaseException) -> asyncio.Future[None]:
    future: asyncio.Future[None] = loop.create_future()
    future.set_exception(error)
    return future


class InboxResource:
    def __init__(self, context: ResourceContext, conversations: ConversationsResource, realtime: RealtimeClient) -> None:
        self._context = context
        self._conversations = conversations
        self._realtime = realtime
        self._watch: InboxWatch | None = None
        self._closed = False

    def watch(
        self,
        handler: DurableConsumer,
        *,
        consumer_id: str,
        checkpoint_store: CheckpointStore,
        poll_interval_ms: int | None = None,
        max_conversations: int | None = None,
    ) -> InboxWatch:
        """Consume every conversation the identity belongs to with acknowledged checkpoints.

        ``checkpoint_store`` is caller-owned; the watcher never closes it. ``poll_interval_ms``
        (default 5000, minimum 100) is the delay after each completed discovery request.
        ``max_conversations`` defaults to ``realtime.max_channels``.
        """
        if self._closed:
            raise AbortError("Client is closed")
        config = self._realtime.config
        if config.recovery["mode"] != "durable":
            raise UnsupportedFeatureError("Inbox watching requires durable recovery mode")
        if self._watch is not None and self._watch.status != "closed":
            raise ValidationError("Close the existing inbox watcher before starting another")
        if (
            not callable(handler)
            or not callable(getattr(checkpoint_store, "load", None))
            or not callable(getattr(checkpoint_store, "compare_and_set", None))
        ):
            raise ValidationError("Invalid inbox consumer")
        maximum = config.realtime["max_channels"]
        if (
            not isinstance(consumer_id, str)
            or not _CONSUMER_ID.fullmatch(consumer_id)
            or (poll_interval_ms is not None and (not is_int(poll_interval_ms) or not 100 <= poll_interval_ms <= 2_147_483_647))
            or (max_conversations is not None and (not is_int(max_conversations) or not 1 <= max_conversations <= maximum))
        ):
            raise ValidationError("Invalid inbox options")
        self._watch = InboxWatch(
            self._context,
            self._conversations,
            self._realtime,
            handler,
            consumer_id,
            checkpoint_store,
            int(poll_interval_ms) if poll_interval_ms is not None else 5000,
            int(max_conversations) if max_conversations is not None else maximum,
        )
        return self._watch

    async def close(self) -> None:
        self._closed = True
        if self._watch is not None:
            await self._watch.close()
