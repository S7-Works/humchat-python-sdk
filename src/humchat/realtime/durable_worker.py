"""Ordered durable log consumption.

WebSocket watermarks wake HTTP replay; frames never advance an acknowledgement.
"""

from __future__ import annotations

import asyncio
import copy
import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .. import _clock
from .._signal import AbortController, AbortSignal, aborted, any_signal, budget, maybe_await, race_signal, spawn
from .._util import is_safe_int, json_dumps, json_loads, utf8_len
from ..errors import (
    AbortError,
    AuthenticationError,
    BackpressureError,
    ConflictError,
    HumChatError,
    NetworkError,
    PersistenceError,
    ProtocolError,
    RecoveryGapError,
    SessionRevokedError,
    TimeoutError,
    UnsupportedFeatureError,
    ValidationError,
    with_intent,
)
from ..persistence.types import PersistenceScope
from ..types import (
    AuthRequiredEvent,
    CheckpointEvent,
    ConnectionEvent,
    ConnectionStatus,
    ConsumerContext,
    DurableEvent,
    EventCursor,
    HandlerErrorEvent,
    MessageEvent,
    RecoveryCoverage,
    ResyncEvent,
    StateEvent,
    UnknownEvent,
)
from . import _ws
from ._upgrade import upgrade_error, ws_url
from .durable_protocol import ReadyFrame, cursor_position, parse_event, parse_gap, parse_ready, same_stream
from .protocol import message_from_state, parse_frame
from .subscription import ChannelSubscription

if TYPE_CHECKING:
    from .._session import VerifiedSession
    from ..persistence.types import CheckpointStore
    from ..types import DurableConsumer
    from .manager import RealtimeClient
    from .multiplex import MultiplexLease

_CONSUMER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass
class _Consumer:
    handle: ChannelSubscription
    handler: DurableConsumer
    consumer_id: str
    checkpoint_store: CheckpointStore
    scope: PersistenceScope | None = None


def _settled(future: asyncio.Future[Any] | None) -> Awaitable[Any] | None:
    """Wait for settlement without propagating cancellation into shared work."""
    return None if future is None else asyncio.shield(future)


async def _quiet(future: asyncio.Future[Any] | None) -> None:
    if future is None:
        return
    try:
        await asyncio.shield(future)
    except Exception:
        pass


class DurableChannelWorker:
    def __init__(self, channel_id: str, manager: RealtimeClient, start: Any) -> None:
        self.channel_id = channel_id
        self._manager = manager
        self._start = start
        self.handles: list[ChannelSubscription] = []
        self.status: ConnectionStatus = "idle"
        self._socket: _ws.Socket | None = None
        self._lease: MultiplexLease | None = manager.multiplex.create(channel_id) if manager.multiplex is not None else None
        self._session: VerifiedSession | None = None
        self._cycle = AbortController()
        self._generation = 0
        self._attempt = 0
        self._ready = False
        self._cursor: str | None = None
        self._target: str | None = None
        self._pump: asyncio.Future[None] | None = None
        self._store_work: asyncio.Future[Any] | None = None
        self._consumer_work: asyncio.Future[None] | None = None
        self._delivery_ended = AbortController()
        self._retiring = False
        self._refresh_requested = False
        self._resync_requested = False
        self._consumer: _Consumer | None = None
        self._retry_timer: Any = None
        self._heartbeat: Any = None
        self._pong: Any = None
        self._refresh: Any = None
        self._stable: Any = None
        self._poll: Any = None

    @property
    def socket_open(self) -> bool:
        if self._lease is not None:
            return self._lease.socket_open
        return self._socket is not None and self._socket.ready_state == _ws.OPEN

    def attach(self) -> ChannelSubscription:
        handle = ChannelSubscription(self.channel_id, self, self._manager.config.realtime["handler_timeout_ms"])
        self.handles.append(handle)
        loop = asyncio.get_running_loop()
        if self._ready and self.status == "live":
            loop.call_soon(lambda: handle.resolve_ready() if self._ready else None)
        if self.status == "blocked":
            loop.call_soon(handle.reject_ready, ProtocolError("Channel is blocked; close its handles before subscribing again"))
        return handle

    def consume(self, handle: ChannelSubscription, handler: DurableConsumer, consumer_id: str, checkpoint_store: CheckpointStore) -> None:
        if self.status != "idle" or self._consumer is not None:
            raise ValidationError("Register one durable consumer per channel before startup")
        if (
            not callable(handler)
            or not isinstance(consumer_id, str)
            or not _CONSUMER_ID.fullmatch(consumer_id)
            or not callable(getattr(checkpoint_store, "load", None))
            or not callable(getattr(checkpoint_store, "compare_and_set", None))
        ):
            raise ValidationError("Invalid durable consumer options")
        self._consumer = _Consumer(handle=handle, handler=handler, consumer_id=consumer_id, checkpoint_store=checkpoint_store)

    def detach(self, handle: ChannelSubscription) -> None:
        if handle in self.handles:
            self.handles.remove(handle)
        if not self.handles:
            self.stop()
            self._manager.remove(self.channel_id, self)
        elif self._consumer is not None and self._consumer.handle is handle and self.status != "closed":
            self.block(AbortError("Acknowledged consumer closed; replace the channel subscription"))

    async def retire(self, handle: ChannelSubscription, signal: AbortSignal) -> None:
        """Drain acknowledged work before an inbox can automatically replace this worker."""
        consumer = self._consumer_work
        store = self._store_work
        pump = self._pump
        # Closing cancels delivery, but an application's coroutine may still be cleaning
        # up. Keep its original deadline alive while waiting for actual settlement.
        self._retiring = True
        storage_budget = budget(self._manager.config.retry["total_timeout_ms"], [signal])

        async def storage() -> None:
            if store is None:
                return
            try:
                result = await race_signal(asyncio.shield(store), storage_budget.signal)
            except Exception:
                if signal.aborted:
                    raise aborted(signal) from None
                raise PersistenceError("Checkpoint storage did not settle; inbox replacement stopped") from None
            if result is False:
                raise ConflictError("Checkpoint changed concurrently; channel ownership was lost")

        async def settled() -> None:
            # Promise.all semantics: settle when every barrier fulfils, or fail with the first
            # failure while the others keep running.
            barriers = [spawn(_settled(consumer) or _none()), spawn(storage()), spawn(_quiet(pump))]
            done, _ = await asyncio.wait(barriers, return_when=asyncio.FIRST_EXCEPTION)
            for barrier in barriers:
                if barrier in done and not barrier.cancelled() and barrier.exception() is not None:
                    raise barrier.exception()  # type: ignore[misc]

        work = spawn(settled())
        try:
            await handle.close()
            await race_signal(work, signal)
        finally:
            storage_budget.dispose()
            self._delivery_ended.abort(AbortError("Subscription is closed"))

    def start(self) -> None:
        if self.status == "idle":
            spawn(self._connect())

    def stop(self) -> None:
        if self.status == "closed":
            return
        self._invalidate()
        self._set_status("closed")
        if self._lease is not None:
            self._lease.close()
        if not self._retiring:
            self._delivery_ended.abort(AbortError("Subscription is closed"))
        for handle in list(self.handles):
            handle.reject_ready(AbortError("Subscription is closed"))
            spawn(handle.close())

    def block(self, error: HumChatError) -> None:
        if self.status in ("closed", "blocked"):
            return
        self._invalidate()
        self._set_status("blocked")
        if not self._retiring:
            self._delivery_ended.abort(error)
        self.notify("error", error)
        if self._lease is not None:
            self._lease.close()
        if isinstance(error, (AuthenticationError, SessionRevokedError)):
            self.notify("auth_required", AuthRequiredEvent(channel_id=self.channel_id, error=error))
        for handle in list(self.handles):
            handle.reject_ready(error)

    def notify(self, event: str, value: Any) -> None:
        for handle in list(self.handles):
            handle.notify(event, value)

    async def _emit(self, event: str, value: Any, generation: int) -> None:
        for handle in list(self.handles):
            if not self._current(generation):
                raise aborted(self._cycle.signal)
            await handle.dispatch(event, value, self._cycle.signal)
        if not self._current(generation):
            raise AbortError("Event delivery generation ended")

    def _set_status(self, state: ConnectionStatus, next_retry_at: float | None = None) -> None:
        self.status = state
        self.notify("connection", ConnectionEvent(channel_id=self.channel_id, state=state, attempt=self._attempt, next_retry_at=next_retry_at))

    def _current(self, generation: int) -> bool:
        return generation == self._generation and not self._cycle.signal.aborted and self.status not in ("blocked", "closed")

    def _invalidate(self) -> None:
        self._generation += 1
        self._ready = False
        self._cycle.abort(AbortError("Connection generation ended"))
        for name in ("_retry_timer", "_heartbeat", "_pong", "_refresh", "_stable", "_poll"):
            timer = getattr(self, name)
            if timer is not None:
                timer.cancel()
            setattr(self, name, None)
        if self._lease is not None:
            self._lease.cancel_attempt()
        socket, self._socket = self._socket, None
        if socket is not None:
            socket.on_error = lambda error: None
            socket.terminate()

    def _fail(self, error: HumChatError) -> None:
        if not error.retryable:
            self.block(error)
            return
        self.notify("error", error)
        self._invalidate()
        config = self._manager.config.realtime
        ceiling = min(config["reconnect_max_delay_ms"], config["reconnect_initial_delay_ms"] * 2 ** min(self._attempt, 20))
        wait = max(1, math.floor(_clock.random() * ceiling), error.retry_after_ms or 0)
        self._attempt += 1
        if self._lease is None:
            self._manager.counters.reconnect_count += 1
        self._set_status("backoff", _clock.now_ms() + wait)
        self._retry_timer = _clock.call_later(wait, lambda: spawn(self._connect()))

    def rotate(self, session: VerifiedSession) -> None:
        if self._lease is not None:
            return  # The shared transport rotates every logical lease together.
        if self.status in ("live", "syncing") and (self._session is None or self._session.token != session.token):
            self._fail(NetworkError("Credential changed; reconnecting event stream", retryable=True))

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
            # Cancellation cannot stop an application coroutine. Never overlap it with replay,
            # and let an in-flight checkpoint write settle before another generation loads it.
            # A prior generation's replay is expected to fail when its socket disappears; wait
            # for settlement without turning that into a permanent error here. Consumer/store
            # barriers below retain their failures.
            await _quiet(self._pump)
            if self._consumer_work is not None:
                await asyncio.shield(self._consumer_work)
            if self._store_work is not None:
                await asyncio.shield(self._store_work)
            if not self._current(generation):
                return
            release = await self._manager.acquire(self._cycle.signal)
            lifetime = budget(config.retry["total_timeout_ms"], [self._cycle.signal])
            try:
                session = await self._manager.sessions.get(lifetime.signal)
            finally:
                lifetime.dispose()
            if not self._current(generation):
                return
            messaging = session.capabilities.messaging
            if not messaging.durable_replay or 1 not in messaging.event_protocol_versions:
                raise UnsupportedFeatureError("Server lacks durable event protocol v1; choose best_effort explicitly for an older server")
            self._session = session
            try:
                ready = await self._open(session, generation)
            except HumChatError as error:
                if self._lease is not None:
                    raise  # The shared transport already performed its single 401 refresh.
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
                ready = await self._open(session, generation)
            if not self._current(generation):
                return
            latest_budget = budget(config.retry["total_timeout_ms"], [self._cycle.signal])
            try:
                latest = await self._manager.sessions.get(latest_budget.signal)
            finally:
                latest_budget.dispose()
            if latest.token != session.token and self._lease is None:
                raise NetworkError("Credential changed during handshake", retryable=True)
            # The hub may refresh a rejected upgrade once for all waiting channels. Its generation
            # guards already invalidate leases if that new credential rotates.
            if self._lease is not None:
                session = latest
                self._session = latest
            if not self._current(generation):
                return
            # Admission bounds handshakes, not application work or an arbitrarily large durable
            # backlog. Shared delivery budgets still bound replay across channels.
            release()
            release = None
            self._set_status("syncing")
            self._start_timers(generation)
            resumed = self._cursor is not None or self._consumer is not None
            self._target = self._higher(self._target, ready.cursor)
            consumer = self._consumer
            scope = PersistenceScope(
                server_origin=config.base_url,
                org_id=session.identity.org_id,
                actor_id=session.identity.actor_id,
                consumer_id=consumer.consumer_id if consumer is not None else "",
                channel_id=self.channel_id,
            )
            if consumer is not None:
                consumer.scope = scope
                self._cursor = await self._checkpoint(lambda: consumer.checkpoint_store.load(scope))
            if not self._current(generation):
                return
            if self._cursor is None:
                start = self._start
                self._cursor = start.cursor if isinstance(start, EventCursor) else ready.earliest_cursor if start == "earliest" else ready.cursor
                self._check_boundary(self._cursor, ready)
                if consumer is not None:
                    first = self._cursor
                    if not await self._checkpoint(lambda: consumer.checkpoint_store.compare_and_set(scope, None, first)):
                        raise ConflictError("Checkpoint changed concurrently; only one owner may consume this stream")
            else:
                self._check_boundary(self._cursor, ready)
            if not self._current(generation):
                return
            self._manager.counters.recovery_count += 1
            await asyncio.shield(self._start_pump(generation, "reconnect" if resumed else "initial", ready.cursor))
            if not self._current(generation):
                return
            self._ready = True
            self._set_status("live")
            for handle in list(self.handles):
                handle.resolve_ready()
            if self._lease is None:

                def stable() -> None:
                    if self._current(generation):
                        self._attempt = 0

                self._stable = _clock.call_later(30_000, stable)
            self._wake(generation)
        except Exception as error:
            if self._current(generation):
                self._fail(with_intent(error, channel_id=self.channel_id))
        finally:
            if release is not None:
                release()

    def _check_boundary(self, cursor: str, ready: ReadyFrame) -> None:
        org = self._manager.config.org_id
        position = cursor_position(cursor, org, self.channel_id)
        floor = cursor_position(ready.earliest_cursor, org, self.channel_id)
        head = cursor_position(ready.cursor, org, self.channel_id)
        if position.epoch != head.epoch or position.sequence < floor.sequence:
            raise RecoveryGapError(
                "Checkpoint has expired or its stream was replaced",
                channel_id=self.channel_id,
                earliest_cursor=ready.earliest_cursor,
                earliest_sequence=str(floor.sequence),
            )
        if position.sequence > head.sequence:
            raise ProtocolError("Checkpoint is ahead of the committed stream")

    async def _checkpoint(self, operation: Callable[[], Awaitable[Any]]) -> Any:
        async def run() -> Any:
            return await maybe_await(operation())

        work = spawn(run())
        self._store_work = work
        try:
            return await asyncio.shield(work)
        except Exception:
            raise PersistenceError("Checkpoint storage failed; event acknowledgement stopped") from None
        finally:
            if self._store_work is work:
                self._store_work = None

    def _higher(self, current: str | None, candidate: str) -> str:
        if not current:
            return candidate
        org = self._manager.config.org_id
        a, b = cursor_position(current, org, self.channel_id), cursor_position(candidate, org, self.channel_id)
        same_stream(a, b)
        return candidate if b.sequence > a.sequence else current

    def _open(self, session: VerifiedSession, generation: int) -> Awaitable[ReadyFrame]:
        if self._lease is not None:
            self._target = None

            def frame(raw: Any) -> None:
                if not self._current(generation):
                    return
                try:
                    self._receive(raw, generation)
                except HumChatError as error:
                    self._fail(error)
                except Exception:
                    self._fail(ProtocolError("Invalid multiplex channel frame"))

            def interrupt(error: HumChatError) -> None:
                if not self._current(generation):
                    return
                if not error.retryable:
                    self.block(error)
                    return
                self.notify("error", error)
                self._invalidate()
                self._set_status("backoff")
                # Physical reconnect and delay belong to the shared transport. Logical workers
                # only wait for it, retaining their application/store settlement barriers.
                asyncio.get_running_loop().call_soon(lambda: spawn(self._connect()))

            return self._lease.open(self._cycle.signal, frame, interrupt)
        config = self._manager.config
        url = ws_url(config.base_url, "/ws/chat?channel_id=" + quote(self.channel_id, safe=""))
        self._target = None
        loop = asyncio.get_running_loop()
        result: asyncio.Future[ReadyFrame] = loop.create_future()
        result.add_done_callback(lambda future: future.cancelled() or future.exception())
        state = {"got_ready": False, "settled": False, "failed": False}
        socket = _ws.Socket(
            url,
            {"Authorization": f"Bearer {session.token}", "X-HumChat-Event-Protocol": "v1"},
            max_payload=config.realtime["max_payload_bytes"],
            handshake_timeout_ms=config.timeout_ms,
        )
        self._socket = socket
        cycle = self._cycle

        def fail(error: HumChatError) -> None:
            # terminate() can emit error/close after an HTTP rejection. Preserve the first
            # handshake error so a 403 cannot become a retryable network failure.
            if state["failed"]:
                return
            state["failed"] = True
            timer.cancel()
            if not state["settled"]:
                state["settled"] = True
                if not result.done():
                    result.set_exception(error)
            elif state["got_ready"] and self._current(generation) and socket is self._socket:
                self._fail(error)
            socket.terminate()

        timer = _clock.call_later(config.timeout_ms, lambda: fail(TimeoutError("Durable ready handshake timed out", retryable=True)))

        def cancel(_: Any = None) -> None:
            fail(AbortError("Durable handshake cancelled"))

        cycle.signal.add_listener(cancel)

        def cleanup() -> None:
            timer.cancel()
            cycle.signal.remove_listener(cancel)

        def unexpected(status: int, headers: Any) -> None:
            fail(
                upgrade_error(
                    status,
                    headers,
                    {
                        "401": "WebSocket authentication rejected",
                        "403": "Channel permission denied",
                        "404": "Channel not found",
                        "429": "WebSocket rate limit exceeded",
                        "408": "WebSocket upgrade timed out",
                        "5xx": "WebSocket temporarily unavailable",
                        "other": "Durable WebSocket negotiation rejected",
                    },
                )
            )

        def message(data: str | bytes, binary: bool) -> None:
            if not self._current(generation) or socket is not self._socket:
                return
            try:
                if binary:
                    raise ProtocolError("Unexpected binary event frame")
                raw = json_loads(data)
                kind = raw.get("type") if isinstance(raw, dict) else None
                if kind == "session_revoked":
                    parsed = parse_frame(raw, config.org_id, self.channel_id)
                    if parsed.kind in ("session_revoked", "session_expired") and self._manager.end_session(parsed, session):
                        fail(NetworkError("Socket token expired; reconnecting with a fresh session", retryable=True))
                    return
                if not state["got_ready"]:
                    ready = parse_ready(raw, config.org_id, self.channel_id)
                    state["got_ready"] = True
                    state["settled"] = True
                    self._target = ready.cursor
                    cleanup()
                    if not result.done():
                        result.set_result(ready)
                    return
                self._receive(raw, generation)
            except HumChatError as error:
                fail(error)
            except Exception:
                fail(ProtocolError("Invalid event JSON"))

        def pong() -> None:
            if self._current(generation) and self._pong is not None:
                self._pong.cancel()
                self._pong = None

        def closed() -> None:
            cleanup()
            if self._current(generation) and socket is self._socket:
                fail(NetworkError("Durable WebSocket closed", retryable=True))

        socket.on_unexpected_response = unexpected
        socket.on_message = message
        socket.on_pong = pong
        socket.on_error = lambda error: fail(NetworkError("Durable WebSocket transport failed", retryable=True))
        socket.on_close = closed
        return result

    def _receive(self, raw: Any, generation: int) -> None:
        kind = raw.get("type") if isinstance(raw, dict) else None
        org = self._manager.config.org_id
        if kind == "event":
            event = parse_event(raw, org, self.channel_id)
            self._target = self._higher(self._target, event.cursor)
            self._wake(generation)
            return
        if kind == "watermark" and self._lease is not None:
            self._target = self._higher(self._target, raw["cursor"])
            self._wake(generation)
            return
        if kind == "ready":
            raise ProtocolError("Duplicate ready frame")
        if kind == "event_gap":
            raise parse_gap(raw.get("gap"), org, self.channel_id)
        if kind == "gap" or (isinstance(raw, dict) and raw.get("code") == "EVENT_GAP"):
            raise parse_gap(raw, org, self.channel_id)
        frame = parse_frame(raw, org, self.channel_id)
        if frame.kind == "resync":
            self._refresh_requested = True
            self._wake(generation)
        elif frame.kind in ("typing", "presence", "receipt", "call_invite"):
            self.notify(frame.kind, frame.payload)
        elif frame.kind == "unknown":
            self.notify("unknown_event", UnknownEvent(channel_id=self.channel_id, type=frame.type or "unknown"))
        else:
            raise ProtocolError("Unversioned durable message frame")

    def _start_timers(self, generation: int) -> None:
        if self._lease is not None:
            return  # Heartbeat, expiry and missed-hint polling are shared.
        options = self._manager.config.realtime

        def heartbeat() -> None:
            if not self._current(generation) or not self.socket_open or self._pong is not None:
                return
            socket = self._socket
            assert socket is not None

            def failed(_: Any) -> None:
                if self._current(generation):
                    self._fail(NetworkError("Ping failed", retryable=True))

            socket.ping(failed)

            def deadline() -> None:
                if self._current(generation):
                    self._fail(TimeoutError("Pong deadline exceeded", retryable=True))

            self._pong = _clock.call_later(options["pong_timeout_ms"], deadline)

        self._heartbeat = _clock.call_every(options["ping_interval_ms"], heartbeat)

        # A second source of progress detection protects against a missed final live hint.
        def poll() -> None:
            if self._current(generation):
                self._refresh_requested = True
                self._wake(generation)

        self._poll = _clock.call_every(5000, poll)
        self._schedule_refresh(generation)

    def _schedule_refresh(self, generation: int) -> None:
        if not self._current(generation) or self._session is None or self._session.refresh_at is None:
            return

        async def refresh() -> None:
            lifetime = budget(self._manager.config.retry["total_timeout_ms"], [self._cycle.signal])
            try:
                session = await self._manager.sessions.get(lifetime.signal)
                if not self._current(generation):
                    return
                if self._session is None or session.token != self._session.token:
                    self.rotate(session)
                else:
                    self._session = session
                    self._schedule_refresh(generation)
            except Exception as error:
                if self._current(generation):
                    self._manager.counters.auth_refresh_failures += 1
                    self._fail(with_intent(error, channel_id=self.channel_id))
            finally:
                lifetime.dispose()

        wait = min(60_000, max(1, self._session.refresh_at * 1000 - _clock.now_ms()))
        self._refresh = _clock.call_later(wait, lambda: spawn(refresh()))

    def _wake(self, generation: int) -> None:
        # A finished pump counts as absent even before its done-callback clears it (that
        # callback runs a loop turn later, unlike a JavaScript microtask), so no hint is lost.
        if self.status != "live" or (self._pump is not None and not self._pump.done()) or not self._current(generation):
            return
        reason = "resync" if self._resync_requested else "live"
        self._resync_requested = False
        if reason == "resync":
            self._manager.counters.recovery_count += 1
        pump = self._start_pump(generation, reason)

        def failed(future: asyncio.Future[None]) -> None:
            if not future.cancelled() and future.exception() is not None and self._current(generation):
                self._fail(with_intent(future.exception() or ProtocolError("Replay failed"), channel_id=self.channel_id))

        pump.add_done_callback(failed)

    def _start_pump(self, generation: int, reason: str, through: str | None = None) -> asyncio.Future[None]:
        if self._pump is not None and not self._pump.done():
            return self._pump
        work = spawn(self._run_pump(generation, reason, through))
        self._pump = work

        def done(future: asyncio.Future[None]) -> None:
            if self._pump is work:
                self._pump = None
            if future.cancelled() or future.exception() is not None:
                return
            if (
                self._current(generation)
                and self.status == "live"
                and (self._refresh_requested or (self._target is not None and self._cursor != self._target))
            ):
                self._wake(generation)

        work.add_done_callback(done)
        return work

    async def _run_pump(self, generation: int, reason: str, through: str | None) -> None:
        pages = states = 0
        # The attempt's captured watermark is fixed; later live hints wait for the next pass.
        target = through if through is not None else (None if self._refresh_requested else self._target)
        self._refresh_requested = False
        config = self._manager.config
        while self._current(generation) and self._cursor is not None and (target is None or self._cursor != target):
            page = await self._manager.events.sync(
                channel_id=self.channel_id,
                after=self._cursor,
                through=target,
                limit=min(200, config.realtime["max_buffered_events"]),
                signal=self._cycle.signal,
            )
            if not self._current(generation):
                return
            pages += 1
            target = page.through_cursor
            self._target = self._higher(self._target, target)
            size = utf8_len(json_dumps(asdict(page)))
            counters, options = self._manager.counters, config.realtime
            if size > config.recovery["max_bytes"] or size > options["max_buffered_bytes"] or len(page.events) > options["max_buffered_events"]:
                raise BackpressureError(
                    "Durable replay page exceeds available delivery budget; checkpoint was not advanced", channel_id=self.channel_id
                )
            if counters.queued_bytes + size > options["max_buffered_bytes"] or counters.queued_events + len(page.events) > options["max_buffered_events"]:
                raise BackpressureError(
                    "Shared delivery budget is busy; replay will resume from the checkpoint", channel_id=self.channel_id, retryable=True
                )
            counters.queued_bytes += size
            counters.queued_events += len(page.events)
            try:
                for event in page.events:
                    if not self._current(generation):
                        return
                    await self._deliver(event, "live" if reason == "live" else "recovery", generation)
                    states += 1
            finally:
                counters.queued_bytes -= size
                counters.queued_events -= len(page.events)
            if not page.next_cursor:
                break
        if self._current(generation) and reason != "live" and self._cursor is not None:
            await self._emit(
                "recovery_complete",
                RecoveryCoverage(
                    channel_id=self.channel_id, mode="durable", reason=reason, pages=pages, states=states, complete=True, cursor=self._cursor
                ),
                generation,
            )

    async def _deliver(self, event: DurableEvent, source: str, generation: int) -> None:
        prior = self._cursor
        # Replay remains in committed channel order. No snapshot cache suppresses a mutation.
        await self._emit("event", event, generation)
        if event.kind == "message":
            await self._emit("message", MessageEvent(message=message_from_state(event.state), source=source, replayed=source == "recovery"), generation)  # type: ignore[arg-type]
        else:
            name = "message_updated" if event.kind == "edit" else "message_deleted" if event.kind == "delete" else "reaction_updated"
            await self._emit(name, StateEvent(state=event.state, source=source), generation)  # type: ignore[arg-type]
        consumer = self._consumer
        if consumer is None:
            self._cursor = event.cursor
            return
        # Keep the original deadline alive after a disconnect: user work may ignore its
        # cancellation signal. A new generation waits for settlement or blocks at that
        # deadline instead of running the same consumer concurrently.
        lifetime = budget(self._manager.config.realtime["handler_timeout_ms"], [self._delivery_ended.signal])
        signal, detach = any_signal([lifetime.signal, self._cycle.signal])
        context = ConsumerContext(signal=signal, source=source)  # type: ignore[arg-type]

        async def run() -> None:
            if signal.aborted:
                raise aborted(signal)
            await maybe_await(consumer.handler(copy.deepcopy(event), context))

        work = spawn(run())

        async def barrier_work() -> None:
            try:
                await race_signal(_swallow(work), lifetime.signal)
            except Exception:
                failure = ProtocolError(
                    "Acknowledged consumer did not settle before its deadline; checkpoint not advanced", channel_id=self.channel_id
                )
                if self.status not in ("blocked", "closed"):
                    self.notify("handler_error", HandlerErrorEvent(event="consume", error=failure))
                    self.block(failure)
                raise failure from None

        barrier = spawn(barrier_work())
        self._consumer_work = barrier

        def settled(_: Any) -> None:
            lifetime.dispose()
            detach()
            if self._consumer_work is barrier:
                self._consumer_work = None

        barrier.add_done_callback(settled)
        try:
            await race_signal(work, signal)
        except Exception:
            if not self._current(generation):
                raise
            failure = ProtocolError("Acknowledged consumer failed or exceeded its deadline; checkpoint not advanced", channel_id=self.channel_id)
            self.notify("handler_error", HandlerErrorEvent(event="consume", error=failure))
            raise failure from None
        if not self._current(generation):
            return
        # Await the durable write even if the socket disconnects; the next generation reloads the store.
        scope = consumer.scope
        assert scope is not None
        if not await self._checkpoint(lambda: consumer.checkpoint_store.compare_and_set(scope, prior, event.cursor)):
            raise ConflictError("Checkpoint changed concurrently; channel ownership was lost")
        self._cursor = event.cursor
        if self._current(generation):
            checkpoint = CheckpointEvent(channel_id=self.channel_id, consumer_id=consumer.consumer_id, cursor=event.cursor, sequence=event.sequence)
            self.notify("checkpoint", checkpoint)

    async def resync(self) -> None:
        if self.status == "closed":
            raise AbortError("Subscription is closed")
        if self.status == "blocked":
            raise ProtocolError("Channel is blocked; create a replacement subscription")
        self.notify("resync", ResyncEvent(channel_id=self.channel_id, reason="requested"))
        self._refresh_requested = True
        self._resync_requested = True
        self._wake(self._generation)

    async def send_signal(self, kind: str, through: int | None = None) -> None:
        if self.status != "live" or not self.socket_open:
            raise NetworkError("Channel is not live")
        if kind == "read" and (not is_safe_int(through) or through is None or through < 0):
            raise ValidationError("through must be non-negative Unix seconds")
        config = self._manager.config
        if self._lease is not None:
            lifetime = budget(config.timeout_ms, [self._cycle.signal])
            try:
                await self._lease.send(kind, through, lifetime.signal)
            finally:
                lifetime.dispose()
            return
        socket = self._socket
        assert socket is not None
        if socket.buffered_amount > config.realtime["max_buffered_bytes"]:
            raise BackpressureError("Outgoing signal buffer exceeded")
        frame: dict[str, Any] = {"type": kind, "channel_id": self.channel_id}
        if kind == "read":
            frame["ts"] = int(through)  # type: ignore[arg-type]
        lifetime = budget(config.timeout_ms, [self._cycle.signal])
        try:
            try:
                await race_signal(socket.send(json_dumps(frame)), lifetime.signal)
            except HumChatError:
                raise
            except Exception:
                raise NetworkError("Signal send failed") from None
        finally:
            lifetime.dispose()


async def _none() -> None:
    return None


async def _swallow(work: asyncio.Future[Any]) -> None:
    try:
        await asyncio.shield(work)
    except Exception:
        pass
