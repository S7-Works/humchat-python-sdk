"""Realtime registry, shared admission budget, and the best-effort channel worker."""

from __future__ import annotations

import asyncio
import math
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .. import _clock
from .._signal import AbortController, AbortSignal, aborted, budget, race_signal, spawn
from .._util import is_safe_int, json_dumps, json_loads, utf8_len
from .._wire import js_less, validate_channel
from ..errors import (
    AbortError,
    AuthenticationError,
    BackpressureError,
    HumChatError,
    NetworkError,
    NotFoundError,
    ProtocolError,
    SessionRevokedError,
    TimeoutError,
    UnsupportedFeatureError,
    ValidationError,
    with_intent,
)
from ..types import (
    AuthRequiredEvent,
    ConnectionEvent,
    ConnectionStatus,
    EventCursor,
    MessageEvent,
    RecoveryCoverage,
    ResyncEvent,
    SessionRevokedEvent,
    StateEvent,
    UnknownEvent,
)
from . import _ws
from ._upgrade import upgrade_error, ws_url
from .durable_worker import DurableChannelWorker
from .multiplex import MultiplexTransport
from .protocol import ParsedFrame, message_from_state, parse_frame
from .state_cache import StateCache
from .subscription import ChannelSubscription

if TYPE_CHECKING:
    from .._session import SessionManager, VerifiedSession
    from ..config import ResolvedConfig
    from ..resources.events import EventsResource
    from ..resources.messages import MessagesResource


@dataclass
class RealtimeStats:
    active_sockets: int
    channels: int
    subscriptions: int
    queued_events: int
    queued_bytes: int
    reconnect_count: int
    recovery_count: int
    auth_refresh_failures: int
    dropped_ephemeral_count: int


@dataclass
class SubscriptionResult:
    channel_id: str
    subscription: ChannelSubscription | None = None
    error: HumChatError | None = None


@dataclass
class Counters:
    queued_events: int = 0
    queued_bytes: int = 0
    reconnect_count: int = 0
    recovery_count: int = 0
    auth_refresh_failures: int = 0
    dropped_ephemeral_count: int = 0


def _newer(a_created: int, a_id: str, b_created: int, b_id: str) -> bool:
    return a_created > b_created or (a_created == b_created and js_less(b_id, a_id))


_EPHEMERAL = ("typing", "presence", "receipt", "call_invite", "unknown")


def _start_key(start: Any) -> str:
    if isinstance(start, EventCursor):
        return json_dumps({"cursor": start.cursor})
    return json_dumps(start)


class RealtimeClient:
    """Registry, shared admission budget, and one socket worker per canonical channel."""

    def __init__(self, config: ResolvedConfig, sessions: SessionManager, messages: MessagesResource, events: EventsResource) -> None:
        self.config = config
        self.sessions = sessions
        self.messages = messages
        self.events = events
        self.counters = Counters()
        self._channels: dict[str, ChannelWorker | DurableChannelWorker] = {}
        self._starts: dict[str, str] = {}
        self._slots = 0
        self._waiters: list[Callable[[], None]] = []
        self._ended = False
        self._revoked = False
        #: Internal: shared multiplex transport when ``realtime.transport == 'multiplex'``.
        self.multiplex = MultiplexTransport(self) if config.realtime["transport"] == "multiplex" else None

        def rotate(session: VerifiedSession) -> None:
            if self.multiplex is not None:
                self.multiplex.rotate(session)
            for worker in list(self._channels.values()):
                worker.rotate(session)

        self._remove_session_listener = sessions.on_change(rotate)

    def __repr__(self) -> str:
        return f"RealtimeClient(channels={len(self._channels)})"

    def subscribe(self, channel_id: str, *, start: Any = None) -> ChannelSubscription:
        """Open (or share) the channel stream. ``start`` is 'now', 'earliest' or ``EventCursor``."""
        if self._ended:
            raise AbortError("Client is closed")
        if self._revoked:
            raise SessionRevokedError("Session ended; create a new client")
        explicit = start is not None
        position = "now" if start is None else start
        if position not in ("now", "earliest") and not (isinstance(position, EventCursor) and isinstance(position.cursor, str)):
            raise ValidationError("Invalid subscription start")
        if self.config.recovery["mode"] == "best_effort" and explicit:
            raise UnsupportedFeatureError("Explicit event positions require durable recovery")
        key = _start_key(position)
        validate_channel(channel_id, self.config.org_id)
        worker = self._channels.get(channel_id)
        if worker is not None and self._starts.get(channel_id) != key:
            raise ValidationError("Shared channel handles must use the same start option")
        if worker is None:
            if len(self._channels) >= self.config.realtime["max_channels"]:
                raise BackpressureError("Channel subscription limit reached", channel_id=channel_id)
            if self.config.recovery["mode"] == "best_effort":
                worker = ChannelWorker(channel_id, self)
            else:
                worker = DurableChannelWorker(channel_id, self, EventCursor(position.cursor) if isinstance(position, EventCursor) else position)
            self._channels[channel_id] = worker
            self._starts[channel_id] = key
            asyncio.get_running_loop().call_soon(worker.start)
        return worker.attach()

    def subscribe_many(self, channel_ids: Sequence[str]) -> list[SubscriptionResult]:
        results = []
        for channel_id in channel_ids:
            try:
                results.append(SubscriptionResult(channel_id=channel_id, subscription=self.subscribe(channel_id)))
            except Exception as error:
                results.append(SubscriptionResult(channel_id=channel_id, error=with_intent(error, channel_id=channel_id)))
        return results

    def get_stats(self) -> RealtimeStats:
        workers = list(self._channels.values())
        active = (1 if self.multiplex.socket_open else 0) if self.multiplex is not None else sum(1 for worker in workers if worker.socket_open)
        return RealtimeStats(
            active_sockets=active,
            channels=len(workers),
            subscriptions=sum(len(worker.handles) for worker in workers),
            **asdict(self.counters),
        )

    async def close(self) -> None:
        if self._ended:
            return
        self._ended = True
        self._remove_session_listener()
        for worker in list(self._channels.values()):
            worker.stop()
        if self.multiplex is not None:
            self.multiplex.close()
        self._channels.clear()
        self._starts.clear()

    # --- internal ---------------------------------------------------------------------------

    def remove(self, channel_id: str, worker: Any) -> None:
        if self._channels.get(channel_id) is worker:
            del self._channels[channel_id]
            self._starts.pop(channel_id, None)

    async def retire(self, handle: ChannelSubscription, signal: AbortSignal) -> None:
        worker = self._channels.get(handle.channel_id)
        if isinstance(worker, DurableChannelWorker) and handle in worker.handles:
            await worker.retire(handle, signal)
        else:
            await handle.close()

    def revoke(self, reason: str) -> None:
        self._revoked = True
        self.sessions.revoke()
        for worker in list(self._channels.values()):
            worker.notify("session_revoked", SessionRevokedEvent(channel_id=worker.channel_id, reason=reason))
            worker.block(SessionRevokedError("Session ended; automatic reconnect is disabled", channel_id=worker.channel_id))
        if self.multiplex is not None:
            self.multiplex.block(SessionRevokedError("Session ended; automatic reconnect is disabled"))

    def end_session(self, frame: ParsedFrame, session: VerifiedSession) -> bool:
        """True only when an explicitly negotiated expiry permits reconnect."""
        if frame.kind == "session_expired" and "TOKEN_EXPIRED" in session.capabilities.authentication.socket_session_end_codes:
            self.sessions.expire(session)
            return True
        self.revoke(frame.reason or "session revoked" if frame.kind == "session_revoked" else "session no longer valid")
        return False

    async def acquire(self, signal: AbortSignal) -> Callable[[], None]:
        if signal.aborted:
            raise aborted(signal)
        if self._slots >= self.config.realtime["connect_concurrency"]:
            loop = asyncio.get_running_loop()
            waiter: asyncio.Future[None] = loop.create_future()

            def wake() -> None:
                if not waiter.done():
                    waiter.set_result(None)
                else:  # A cancelled waiter must hand its slot to the next one.
                    self._release()

            def cancel(_: Any = None) -> None:
                if wake in self._waiters:
                    self._waiters.remove(wake)
                if not waiter.done():
                    waiter.set_exception(aborted(signal))

            self._waiters.append(wake)
            signal.add_listener(cancel)
            try:
                await waiter
            finally:
                signal.remove_listener(cancel)
        else:
            self._slots += 1
        if signal.aborted:
            self._release()
            raise aborted(signal)
        released = False

        def release() -> None:
            nonlocal released
            if not released:
                released = True
                self._release()

        return release

    def _release(self) -> None:
        if self._waiters:
            self._waiters.pop(0)()
        else:
            self._slots -= 1


class ChannelWorker:
    """Best-effort channel: history snapshots plus live frames; never claims complete recovery."""

    def __init__(self, channel_id: str, manager: RealtimeClient) -> None:
        self.channel_id = channel_id
        self._manager = manager
        self.handles: list[ChannelSubscription] = []
        self.status: ConnectionStatus = "idle"
        options = manager.config.realtime
        self._cache = StateCache(options["max_cached_messages"], options["max_cached_bytes"])
        self._threads: OrderedDict[str, bool] = OrderedDict()
        self._watermarks: dict[str, tuple[int, str] | None] = {}
        self._socket: _ws.Socket | None = None
        self._session: VerifiedSession | None = None
        self._cycle = AbortController()
        self._generation = 0
        self._attempt = 0
        self._baseline = False
        self._ready = False
        self._queue: list[tuple[ParsedFrame, int]] = []
        self._draining: asyncio.Future[None] | None = None
        self._recovering: asyncio.Future[None] | None = None
        self._resync_again = False
        self._retry_timer: Any = None
        self._ping_timer: Any = None
        self._pong_timer: Any = None
        self._expiry_timer: Any = None
        self._stable_timer: Any = None

    @property
    def socket_open(self) -> bool:
        return self._socket is not None and self._socket.ready_state == _ws.OPEN

    def attach(self) -> ChannelSubscription:
        handle = ChannelSubscription(self.channel_id, self, self._manager.config.realtime["handler_timeout_ms"])
        self.handles.append(handle)
        loop = asyncio.get_running_loop()
        if self.status == "live" and self._ready:
            loop.call_soon(lambda: handle.resolve_ready() if self.status == "live" and self._ready else None)
        if self.status == "blocked":
            loop.call_soon(handle.reject_ready, AuthenticationError("Channel is blocked; close its handles before subscribing again"))
        return handle

    def detach(self, handle: ChannelSubscription) -> None:
        if handle in self.handles:
            self.handles.remove(handle)
        if not self.handles:
            self.stop()
            self._manager.remove(self.channel_id, self)

    def start(self) -> None:
        if self.status != "idle":
            return
        spawn(self._connect())

    def stop(self) -> None:
        if self.status == "closed":
            return
        self._invalidate()
        self._set_status("closed")
        error = AbortError("Subscription is closed", channel_id=self.channel_id)
        for handle in list(self.handles):
            handle.reject_ready(error)
            spawn(handle.close())
        self._cache.clear()
        self._threads.clear()
        self._watermarks.clear()

    def block(self, error: HumChatError) -> None:
        if self.status in ("closed", "blocked"):
            return
        self._invalidate()
        self._set_status("blocked")
        self.notify("error", error)
        if isinstance(error, (AuthenticationError, SessionRevokedError)):
            self.notify("auth_required", AuthRequiredEvent(channel_id=self.channel_id, error=error))
        for handle in list(self.handles):
            handle.reject_ready(error)

    def notify(self, event: str, payload: Any) -> None:
        for handle in list(self.handles):
            handle.notify(event, payload)

    async def _emit(self, event: str, payload: Any, generation: int, signal: AbortSignal | None = None) -> None:
        signal = signal or self._cycle.signal
        for handle in list(self.handles):
            if not self._current(generation):
                return
            if signal.aborted:
                raise aborted(signal)
            await handle.dispatch(event, payload, signal)
            if signal.aborted:
                raise aborted(signal)

    def _set_status(self, state: ConnectionStatus, next_retry_at: float | None = None) -> None:
        self.status = state
        self.notify("connection", ConnectionEvent(channel_id=self.channel_id, state=state, attempt=self._attempt, next_retry_at=next_retry_at))

    def _current(self, generation: int) -> bool:
        return generation == self._generation and not self._cycle.signal.aborted and self.status not in ("closed", "blocked")

    def _invalidate(self) -> None:
        self._ready = False
        self._generation += 1
        self._cycle.abort(AbortError("Connection generation ended"))
        for name in ("_retry_timer", "_ping_timer", "_pong_timer", "_expiry_timer", "_stable_timer"):
            timer = getattr(self, name)
            if timer is not None:
                timer.cancel()
            setattr(self, name, None)
        socket, self._socket = self._socket, None
        if socket is not None:
            socket.on_error = lambda error: None
            socket.terminate()
        for _, size in self._queue:
            self._release(size)
        self._queue = []

    async def _connect(self) -> None:
        if not self.handles or self.status in ("closed", "blocked"):
            return
        self._cycle = AbortController()
        self._generation += 1
        generation = self._generation
        self._set_status("connecting")
        release: Callable[[], None] | None = None
        config = self._manager.config
        try:
            release = await self._manager.acquire(self._cycle.signal)
            session_budget = budget(config.retry["total_timeout_ms"], [self._cycle.signal])
            try:
                session = await self._manager.sessions.get(session_budget.signal)
            finally:
                session_budget.dispose()
            if not self._current(generation):
                return
            self._session = session
            try:
                await self._open(session, generation)
            except HumChatError as error:
                if not isinstance(error, AuthenticationError) or error.status != 401 or not self._current(generation):
                    raise
                refresh = budget(config.retry["total_timeout_ms"], [self._cycle.signal])
                try:
                    session = await self._manager.sessions.unauthorized(session, refresh.signal)
                except Exception:
                    self._manager.counters.auth_refresh_failures += 1
                    raise
                finally:
                    refresh.dispose()
                self._session = session
                await self._open(session, generation)
            if not self._current(generation):
                return
            # Refresh may have completed through HTTP while this handshake was pending.
            check = budget(config.retry["total_timeout_ms"], [self._cycle.signal])
            try:
                latest = await self._manager.sessions.get(check.signal)
            finally:
                check.dispose()
            if not self._current(generation):
                return
            if latest.token != session.token:
                raise NetworkError("Credential changed during handshake; reconnecting channel", retryable=True)
            self._session = latest
            self._start_liveness(generation)
            await self._recover("reconnect" if self._baseline else "initial", generation)
            if not self._current(generation):
                return
            self._baseline = True
            self._set_status("live")
            await asyncio.shield(self._drain(generation))
            if not self._current(generation):
                return
            self._ready = True
            for handle in list(self.handles):
                handle.resolve_ready()

            def stable() -> None:
                if self._current(generation):
                    self._attempt = 0

            self._stable_timer = _clock.call_later(30_000, stable)
            if self._resync_again:
                self._resync_again = False
                spawn(self.resync())
        except Exception as error:
            if self._current(generation):
                self._fail(with_intent(error, channel_id=self.channel_id))
        finally:
            if release is not None:
                release()

    def _open(self, session: VerifiedSession, generation: int) -> asyncio.Future[None]:
        config = self._manager.config
        url = ws_url(config.base_url, "/ws/chat?channel_id=" + quote(self.channel_id, safe=""))
        loop = asyncio.get_running_loop()
        result: asyncio.Future[None] = loop.create_future()
        result.add_done_callback(lambda future: future.cancelled() or future.exception())
        socket = _ws.Socket(
            url,
            {"Authorization": f"Bearer {session.token}"},
            max_payload=config.realtime["max_payload_bytes"],
            handshake_timeout_ms=config.timeout_ms,
        )
        self._socket = socket
        state = {"opened": False, "rejected": False}

        def fail(error: HumChatError) -> None:
            if state["rejected"]:
                return
            state["rejected"] = True
            if not result.done():
                result.set_exception(error)
            socket.terminate()

        def unexpected(status: int, headers: Any) -> None:
            fail(
                upgrade_error(
                    status,
                    headers,
                    {
                        "401": "WebSocket authentication rejected",
                        "403": "WebSocket channel permission denied",
                        "404": "WebSocket channel not found",
                        "429": "WebSocket rate limit exceeded",
                        "408": "WebSocket upgrade timed out",
                        "5xx": "WebSocket service temporarily unavailable",
                        "other": "WebSocket upgrade rejected",
                    },
                )
            )

        def opened() -> None:
            if not self._current(generation):
                socket.terminate()
                return
            state["opened"] = True
            if not result.done():
                result.set_result(None)

        def message(data: str | bytes, binary: bool) -> None:
            if not self._current(generation) or socket is not self._socket:
                return
            if binary:
                self.block(ProtocolError("Unexpected binary chat frame", channel_id=self.channel_id))
                return
            try:
                text = data if isinstance(data, str) else data.decode("utf-8", "strict")
                frame = parse_frame(json_loads(text), config.org_id, self.channel_id)
                if frame.kind in ("session_revoked", "session_expired"):
                    if self._manager.end_session(frame, session):
                        self._fail(NetworkError("Socket token expired; reconnecting with a fresh session", retryable=True))
                    return
                if frame.kind == "resync":
                    spawn(self.resync())
                    return
                self._enqueue(frame, utf8_len(text), generation)
            except HumChatError as error:
                self.block(error)
            except Exception:
                self.block(ProtocolError("Invalid JSON chat frame", channel_id=self.channel_id))

        def pong() -> None:
            if self._current(generation) and self._pong_timer is not None:
                self._pong_timer.cancel()
                self._pong_timer = None

        def error(_: Any) -> None:
            if not state["opened"]:
                fail(NetworkError("WebSocket handshake failed", retryable=True))
            elif self._current(generation) and socket is self._socket:
                self._fail(NetworkError("WebSocket transport failed", retryable=True))

        def closed() -> None:
            if not state["opened"]:
                fail(NetworkError("WebSocket closed during handshake", retryable=True))
            elif self._current(generation) and socket is self._socket:
                self._fail(NetworkError("WebSocket connection closed", retryable=True))

        socket.on_unexpected_response = unexpected
        socket.on_open = opened
        socket.on_message = message
        socket.on_pong = pong
        socket.on_error = error
        socket.on_close = closed
        return result

    def _fail(self, error: HumChatError) -> None:
        if not error.retryable:
            self.block(error)
            return
        self.notify("error", error)
        self._invalidate()
        options = self._manager.config.realtime
        ceiling = min(options["reconnect_max_delay_ms"], options["reconnect_initial_delay_ms"] * 2 ** min(self._attempt, 20))
        wait = max(1, math.floor(_clock.random() * ceiling), error.retry_after_ms or 0)
        self._attempt += 1
        self._manager.counters.reconnect_count += 1
        self._set_status("backoff", _clock.now_ms() + wait)

        def retry() -> None:
            self._retry_timer = None
            spawn(self._connect())

        self._retry_timer = _clock.call_later(wait, retry)

    def rotate(self, session: VerifiedSession) -> None:
        if self.status not in ("live", "syncing"):
            return
        if self._session is None or self._session.token == session.token:
            return
        self._fail(NetworkError("Credential refreshed; reconnecting channel", retryable=True))

    def _start_liveness(self, generation: int) -> None:
        options = self._manager.config.realtime

        def heartbeat() -> None:
            if not self._current(generation) or not self.socket_open or self._pong_timer is not None:
                return
            socket = self._socket
            assert socket is not None

            def ping_failed(_: Any) -> None:
                if self._current(generation):
                    self._fail(NetworkError("WebSocket ping failed", retryable=True))

            socket.ping(ping_failed)

            def deadline() -> None:
                if self._current(generation):
                    self._fail(TimeoutError("WebSocket pong deadline exceeded", retryable=True))

            self._pong_timer = _clock.call_later(options["pong_timeout_ms"], deadline)

        self._ping_timer = _clock.call_every(options["ping_interval_ms"], heartbeat)
        self._schedule_refresh(generation)

    def _schedule_refresh(self, generation: int) -> None:
        if not self._current(generation) or self._session is None or self._session.refresh_at is None:
            return
        wait = min(60_000, max(1, self._session.refresh_at * 1000 - _clock.now_ms()))

        async def refresh() -> None:
            try:
                lifetime = budget(self._manager.config.retry["total_timeout_ms"], [self._cycle.signal])
                try:
                    session = await self._manager.sessions.get(lifetime.signal)
                finally:
                    lifetime.dispose()
                if not self._current(generation):
                    return
                if self._session is None or session.token != self._session.token:
                    self.rotate(session)
                    return
                self._session = session
                self._schedule_refresh(generation)
            except Exception as error:
                if self._current(generation):
                    self._manager.counters.auth_refresh_failures += 1
                    self._fail(with_intent(error, channel_id=self.channel_id))

        self._expiry_timer = _clock.call_later(wait, lambda: spawn(refresh()))

    def _enqueue(self, frame: ParsedFrame, size: int, generation: int) -> None:
        total, limit = self._manager.counters, self._manager.config.realtime
        if total.queued_events + 1 > limit["max_buffered_events"] or total.queued_bytes + size > limit["max_buffered_bytes"]:
            if frame.kind in _EPHEMERAL:
                total.dropped_ephemeral_count += 1
                return
            self.notify(
                "recovery_incomplete",
                RecoveryCoverage(
                    channel_id=self.channel_id,
                    mode="best_effort",
                    reason="overflow",
                    pages=0,
                    states=0,
                    complete=False,
                    limitations=["Incoming event buffer exceeded its budget; delivery is blocked"],
                ),
            )
            self.block(BackpressureError("Incoming event buffer exceeded its budget", channel_id=self.channel_id))
            return
        total.queued_events += 1
        total.queued_bytes += size
        self._queue.append((frame, size))
        if self.status == "live":
            spawn(self._drain(generation))

    def _release(self, size: int) -> None:
        self._manager.counters.queued_events -= 1
        self._manager.counters.queued_bytes -= size

    def _drain(self, generation: int) -> asyncio.Future[None]:
        # A finished task counts as absent: its clearing done-callback runs a loop turn later
        # (JavaScript runs it as a microtask), and a frame enqueued in between must still drain.
        if self._draining is not None and not self._draining.done():
            return self._draining

        async def work() -> None:
            while self._current(generation) and self.status == "live" and self._queue:
                frame, size = self._queue.pop(0)
                try:
                    await self._deliver(frame, generation)
                except Exception as error:
                    if self._current(generation):
                        self.block(with_intent(error, channel_id=self.channel_id))
                finally:
                    self._release(size)

        task = spawn(work())
        self._draining = task

        def clear(_: Any) -> None:
            if self._draining is task:
                self._draining = None

        task.add_done_callback(clear)
        return task

    def _remember(self, created_at: int, message_id: str, thread_root_id: str | None) -> None:
        stream = thread_root_id or ""
        old = self._watermarks.get(stream)
        if old is None or _newer(created_at, message_id, old[0], old[1]):
            self._watermarks[stream] = (created_at, message_id)
        root = thread_root_id if thread_root_id is not None else message_id
        self._threads.pop(root, None)
        self._threads[root] = True
        while len(self._threads) > self._manager.config.recovery["max_threads"]:
            oldest, _ = self._threads.popitem(last=False)
            self._watermarks.pop(oldest, None)

    async def _deliver(self, frame: ParsedFrame, generation: int) -> None:
        kind = frame.kind
        if kind == "message":
            message = frame.message
            assert message is not None
            self._remember(message.created_at, message.id, message.thread_root_id)
            if self._cache.observe_message(message):
                await self._emit("message", MessageEvent(message=message, source="live", replayed=False), generation)
        elif kind == "state":
            state = frame.state
            assert state is not None and frame.event is not None
            self._remember(state.created_at, state.id, state.thread_root_id)
            merged = self._cache.merge(state)
            if (frame.event == "reaction_updated" and merged.reactions_changed) or (frame.event != "reaction_updated" and merged.content_changed):
                await self._emit(frame.event, StateEvent(state=merged.state, source="live"), generation)
        elif kind == "legacy_mutation":
            assert frame.message_id is not None
            state = await self._manager.messages.get_state(channel_id=self.channel_id, message_id=frame.message_id, signal=self._cycle.signal)
            if not self._current(generation):
                return
            merged = self._cache.merge(state)
            # A hydrated current snapshot cannot certify which historical mutation it represents.
            if merged.changed:
                await self._emit("state_reconciled", StateEvent(state=merged.state, source="recovery"), generation)
        elif kind in ("typing", "presence", "receipt", "call_invite"):
            await self._emit(kind, frame.payload, generation)
        elif kind == "unknown":
            await self._emit("unknown_event", UnknownEvent(channel_id=self.channel_id, type=frame.type or "unknown"), generation)

    async def send_signal(self, kind: str, through: int | None = None) -> None:
        if self.status != "live" or not self.socket_open:
            raise NetworkError("Channel is not live", channel_id=self.channel_id)
        if kind == "read" and (not is_safe_int(through) or through is None or through < 0):
            raise ValidationError("through must be non-negative Unix seconds")
        socket = self._socket
        assert socket is not None
        if socket.buffered_amount > self._manager.config.realtime["max_buffered_bytes"]:
            raise BackpressureError("Outgoing signal buffer exceeded its budget", channel_id=self.channel_id)
        frame: dict[str, Any] = {"type": kind, "channel_id": self.channel_id}
        if kind == "read":
            frame["ts"] = int(through)  # type: ignore[arg-type]
        lifetime = budget(self._manager.config.timeout_ms, [self._cycle.signal])
        try:
            try:
                await race_signal(socket.send(json_dumps(frame)), lifetime.signal)
            except _ws.SocketError:
                raise NetworkError("WebSocket signal failed") from None
            except HumChatError:
                raise
            except Exception:
                raise NetworkError("WebSocket signal failed") from None
        finally:
            lifetime.dispose()

    async def resync(self) -> None:
        """Schedule recovery. Observe ``recovery_complete`` for completion, including from handlers."""
        if self.status == "closed":
            raise AbortError("Subscription is closed")
        if self.status == "blocked":
            raise ProtocolError("Channel is blocked; create a new subscription")
        self.notify("resync", ResyncEvent(channel_id=self.channel_id, reason="requested"))
        if self.status != "live" or (self._recovering is not None and not self._recovering.done()):
            self._resync_again = True
            return
        generation = self._generation
        self._ready = False
        self._set_status("syncing")

        async def work() -> None:
            if self._draining is not None:
                await asyncio.shield(self._draining)
            if not self._current(generation):
                return
            while True:
                self._resync_again = False
                await self._recover("resync", generation)
                if not (self._current(generation) and self._resync_again):
                    break
            if self._current(generation):
                self._set_status("live")
                await asyncio.shield(self._drain(generation))
                if self._current(generation):
                    self._ready = True
                    for handle in list(self.handles):
                        handle.resolve_ready()

        task = spawn(work())
        self._recovering = task

        def settled(future: asyncio.Future[None]) -> None:
            if not future.cancelled() and future.exception() is not None and self._current(generation):
                self._fail(with_intent(future.exception() or ProtocolError("Recovery failed"), channel_id=self.channel_id))
            if self._recovering is task:
                self._recovering = None
            # A resync can arrive from a buffered handler after the scan loop ended.
            if self._current(generation) and self._resync_again:
                self._resync_again = False
                spawn(self.resync())

        task.add_done_callback(settled)

    async def _recover(self, reason: str, generation: int) -> None:
        if not self._current(generation):
            return
        lifetime = budget(self._manager.config.recovery["timeout_ms"], [self._cycle.signal])
        try:
            await self._recover_within(reason, generation, lifetime.signal)
        finally:
            lifetime.dispose()

    async def _recover_within(self, reason: str, generation: int, signal: AbortSignal) -> None:
        if not self._current(generation):
            return
        if signal.aborted:
            raise aborted(signal)
        self._set_status("syncing")
        self._manager.counters.recovery_count += 1
        recovery = self._manager.config.recovery
        coverage = RecoveryCoverage(channel_id=self.channel_id, mode="best_effort", reason=reason, pages=0, states=0, complete=False)
        limitations: list[str] = [
            "Current server history is not a durable event log",
            "Replies in unknown or unscanned threads and older mutations may be missing",
        ]

        def limit(text: str) -> None:
            if text not in limitations:
                limitations.append(text)

        seen: set[str] = set()
        spent = 0

        async def scan(root: str | None) -> None:
            nonlocal spent
            stream = root or ""
            has_boundary = stream in self._watermarks
            boundary = self._watermarks.get(stream)
            pages: list[list[Any]] = []
            cursor: str | None = None
            for count in range(recovery["max_pages"]):
                page = await self._manager.messages.history(channel_id=self.channel_id, thread_root_id=root, cursor=cursor, signal=signal)
                if not self._current(generation):
                    return
                coverage.pages += 1
                spent += utf8_len(json_dumps(asdict(page)))
                if spent > recovery["max_bytes"]:
                    limit("Recovery byte budget reached")
                    break
                pages.append(page.messages)
                if page.next_cursor is None:
                    break
                oldest = page.messages[0] if page.messages else None
                if boundary is not None and oldest is not None and not _newer(oldest.created_at, oldest.id, boundary[0], boundary[1]):
                    break
                cursor = page.next_cursor
                if count + 1 == recovery["max_pages"]:
                    limit("Recovery page budget reached")
            if stream not in self._watermarks:
                self._watermarks[stream] = None
            for state in [item for messages in reversed(pages) for item in messages]:
                if not self._current(generation):
                    return
                seen.add(state.id)
                coverage.states += 1
                prior = self._cache.get(state.id)
                recover_creation = (
                    self._baseline
                    and has_boundary
                    and prior is None
                    and not state.deleted
                    and state.revision == 1
                    and (boundary is None or _newer(state.created_at, state.id, boundary[0], boundary[1]))
                )
                if recover_creation:
                    message = message_from_state(state)
                    if self._cache.observe_message(message):
                        await self._emit("message", MessageEvent(message=message, source="recovery", replayed=True), generation, signal)
                merged = self._cache.merge(state)
                self._remember(state.created_at, state.id, state.thread_root_id)
                if merged.changed:
                    await self._emit("state_reconciled", StateEvent(state=merged.state, source="recovery"), generation, signal)

        await scan(None)
        for root in reversed(list(self._threads.keys())):
            if not self._current(generation) or spent > recovery["max_bytes"]:
                break
            await scan(root)
        if self._baseline and self._current(generation):
            targets = [message_id for message_id in self._cache.ids() if message_id not in seen]
            targets.reverse()
            if len(targets) > recovery["max_state_refresh"]:
                limit("Known-state refresh budget reached")
            for message_id in targets[: recovery["max_state_refresh"]]:
                if not self._current(generation) or spent > recovery["max_bytes"]:
                    break
                try:
                    state = await self._manager.messages.get_state(channel_id=self.channel_id, message_id=message_id, signal=signal)
                    if not self._current(generation):
                        return
                    spent += utf8_len(json_dumps(asdict(state)))
                    if spent > recovery["max_bytes"]:
                        limit("Recovery byte budget reached")
                        break
                    coverage.states += 1
                    merged = self._cache.merge(state)
                    if merged.changed:
                        await self._emit("state_reconciled", StateEvent(state=merged.state, source="recovery"), generation, signal)
                except NotFoundError:
                    limit("A previously observed message is no longer readable")
        if not self._current(generation):
            return
        incomplete = RecoveryCoverage(**{**asdict(coverage), "limitations": list(limitations)})
        await self._emit("recovery_incomplete", incomplete, generation, signal)
        await self._emit("recovery_complete", coverage, generation, signal)


__all__ = ["RealtimeClient", "RealtimeStats", "SubscriptionResult"]
