"""One physical connection, retry loop, refresh lifecycle and heartbeat for all durable channels."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .. import _clock
from .._signal import AbortController, AbortSignal, aborted, budget, race_signal, spawn
from .._util import is_safe_int, json_dumps, json_loads, utf8_len
from ..errors import (
    AbortError,
    AuthenticationError,
    BackpressureError,
    HumChatError,
    NetworkError,
    NotFoundError,
    PermissionError,
    ProtocolError,
    ServerError,
    TimeoutError,
    UnsupportedFeatureError,
    ValidationError,
)
from . import _ws
from ._upgrade import upgrade_error, ws_url
from .durable_protocol import ReadyFrame, parse_ready
from .protocol import parse_frame

if TYPE_CHECKING:
    from .._session import VerifiedSession
    from .manager import RealtimeClient

Frame = dict[str, Any]
_CORRELATION = re.compile(r"^[\x21-\x7e]{1,128}$")


class MultiplexLease:
    """A logical subscription. Cancellation removes its correlation ID before sending unsubscribe."""

    def __init__(self, channel_id: str, owner: MultiplexTransport) -> None:
        self.channel_id = channel_id
        self._owner = owner
        self.id: str | None = None
        self.deadline: float | None = None
        self.ready = False
        self._waiting: tuple[asyncio.Future[ReadyFrame], Callable[[], None]] | None = None
        self._frame: Callable[[Frame], None] | None = None
        self._interrupt: Callable[[HumChatError], None] | None = None
        self._closed = False

    @property
    def socket_open(self) -> bool:
        return self.ready and self._owner.socket_open

    @property
    def pending(self) -> bool:
        return self._waiting is not None

    def open(self, signal: AbortSignal, frame: Callable[[Frame], None], interrupt: Callable[[HumChatError], None]) -> asyncio.Future[ReadyFrame]:
        self.cancel_attempt()
        loop = asyncio.get_running_loop()
        result: asyncio.Future[ReadyFrame] = loop.create_future()
        result.add_done_callback(lambda future: future.cancelled() or future.exception())
        if self._closed:
            result.set_exception(AbortError("Multiplex channel is closed"))
            return result
        if signal.aborted:
            result.set_exception(aborted(signal))
            return result
        self._frame = frame
        self._interrupt = interrupt

        def cancel(_: Any = None) -> None:
            self.cancel_attempt(aborted(signal))

        self._waiting = (result, lambda: signal.remove_listener(cancel))
        signal.add_listener(cancel)
        self._owner.request(self)
        return result

    def accept(self, raw: Frame) -> None:
        if self._waiting is None or self.ready:
            raise ProtocolError("Unexpected multiplex ready frame")
        ready = parse_ready(raw, self._owner.org_id, self.channel_id)
        self.ready = True
        self.deadline = None
        (result, cleanup), self._waiting = self._waiting, None
        cleanup()
        if not result.done():
            result.set_result(ready)

    def frame(self, raw: Frame) -> None:
        if self.ready and self._frame is not None:
            self._frame(raw)

    def poll(self) -> None:
        if self.ready:
            self.frame({"type": "resync", "channel_id": self.channel_id})

    def disconnect(self, error: HumChatError) -> None:
        self.id = None
        self.deadline = None
        if not self.ready:
            return  # Pending opens await the single shared reconnect loop.
        self.ready = False
        if self._interrupt is not None:
            self._interrupt(error)

    def reject(self, error: HumChatError) -> None:
        waiting, interrupt = self._waiting, self._interrupt
        self.cancel_attempt(error)
        if waiting is None and interrupt is not None:
            interrupt(error)

    def cancel_attempt(self, error: HumChatError | None = None) -> None:
        self._owner.forget(self)
        self.ready = False
        self.deadline = None
        waiting, self._waiting = self._waiting, None
        self._frame = None
        self._interrupt = None
        if waiting is not None:
            result, cleanup = waiting
            cleanup()
            if not result.done():
                result.set_exception(error or AbortError("Logical subscription attempt ended"))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.cancel_attempt()
        self._owner.remove(self)

    async def send(self, kind: str, through: int | None, signal: AbortSignal) -> None:
        if not self.socket_open or not self.id:
            raise NetworkError("Channel is not live")
        frame: Frame = {"type": kind, "subscription_id": self.id, "channel_id": self.channel_id}
        if kind == "read":
            frame["ts"] = through
        await self._owner.send(frame, signal)


class MultiplexTransport:
    def __init__(self, manager: RealtimeClient) -> None:
        self._manager = manager
        self.org_id = manager.config.org_id
        self._leases: list[MultiplexLease] = []
        self._ids: dict[str, MultiplexLease] = {}
        self._queue: list[MultiplexLease] = []
        self._socket: _ws.Socket | None = None
        self._session: VerifiedSession | None = None
        self._ready = False
        self._connecting = False
        self._closed = False
        self._blocked: HumChatError | None = None
        self._generation = 0
        self._serial = 0
        self._attempt = 0
        self._limit = 100
        self._cycle = AbortController()
        self._retry: Any = None
        self._heartbeat: Any = None
        self._pong: Any = None
        self._refresh: Any = None
        self._stable: Any = None
        self._poll: Any = None
        self._supervisor: Any = None

    @property
    def socket_open(self) -> bool:
        return self._socket is not None and self._socket.ready_state == _ws.OPEN

    def create(self, channel_id: str) -> MultiplexLease:
        if self._closed:
            raise AbortError("Client is closed")
        if self._blocked is not None:
            raise self._blocked
        lease = MultiplexLease(channel_id, self)
        self._leases.append(lease)
        return lease

    def request(self, lease: MultiplexLease) -> None:
        if self._blocked is not None or self._closed:
            lease.reject(self._blocked or AbortError("Client is closed"))
            return
        if lease not in self._queue:
            self._queue.append(lease)
        if self._ready:
            self._flush()
        elif not self._connecting and self._retry is None:
            spawn(self._connect())

    def forget(self, lease: MultiplexLease) -> None:
        if lease in self._queue:
            self._queue.remove(lease)
        identifier, lease.id = lease.id, None
        if not identifier:
            return
        self._ids.pop(identifier, None)
        if self._ready and self.socket_open:
            self._write({"type": "unsubscribe", "subscription_id": identifier})

    def remove(self, lease: MultiplexLease) -> None:
        if lease in self._leases:
            self._leases.remove(lease)
        if not self._leases:
            self._reset()
            self._attempt = 0

    def rotate(self, session: VerifiedSession) -> None:
        if self._session is not None and self._session.token != session.token and self._ready:
            self._fail(NetworkError("Credential changed; reconnecting multiplex stream", retryable=True))

    def close(self) -> None:
        self._closed = True
        self._reset()
        for lease in list(self._leases):
            lease.close()

    def block(self, error: HumChatError) -> None:
        if self._closed or self._blocked is not None:
            return
        self._blocked = error
        self._reset()
        for lease in list(self._leases):
            lease.reject(error)

    def _reset(self) -> None:
        self._generation += 1
        self._ready = False
        self._connecting = False
        self._cycle.abort(AbortError("Multiplex connection generation ended"))
        for name in ("_retry", "_heartbeat", "_pong", "_refresh", "_stable", "_poll", "_supervisor"):
            timer = getattr(self, name)
            if timer is not None:
                timer.cancel()
            setattr(self, name, None)
        socket, self._socket = self._socket, None
        if socket is not None:
            socket.on_error = lambda error: None
            socket.terminate()
        self._ids.clear()
        self._queue.clear()

    def _current(self, generation: int) -> bool:
        return generation == self._generation and not self._closed and self._blocked is None and not self._cycle.signal.aborted

    def _fail(self, error: HumChatError) -> None:
        if self._closed or self._blocked is not None:
            return
        if not error.retryable:
            self.block(error)
            return
        self._reset()
        if not self._leases:
            return
        config = self._manager.config.realtime
        ceiling = min(config["reconnect_max_delay_ms"], config["reconnect_initial_delay_ms"] * 2 ** min(self._attempt, 20))
        wait = max(1, math.floor(_clock.random() * ceiling), error.retry_after_ms or 0)
        self._attempt += 1
        self._manager.counters.reconnect_count += 1

        def retry() -> None:
            self._retry = None
            spawn(self._connect())

        # Install the shared timer before interrupting workers that immediately request new leases.
        self._retry = _clock.call_later(wait, retry)
        for lease in list(self._leases):
            lease.disconnect(error)
            if lease.pending and lease not in self._queue:
                self._queue.append(lease)

    async def _connect(self) -> None:
        if self._closed or self._blocked is not None or self._connecting or not self._leases:
            return
        self._cycle = AbortController()
        self._generation += 1
        generation = self._generation
        self._connecting = True
        lifetime = budget(self._manager.config.retry["total_timeout_ms"], [self._cycle.signal])
        try:
            session = await self._manager.sessions.get(lifetime.signal)
            self._validate_capabilities(session)
            self._session = session
            try:
                await self._open(session, generation)
            except HumChatError as error:
                if not isinstance(error, AuthenticationError) or error.status != 401 or not self._current(generation):
                    raise
                try:
                    session = await self._manager.sessions.unauthorized(session, lifetime.signal)
                except Exception:
                    self._manager.counters.auth_refresh_failures += 1
                    raise
                self._validate_capabilities(session)
                self._session = session
                await self._open(session, generation)
            if not self._current(generation):
                return
            latest = await self._manager.sessions.get(lifetime.signal)
            if latest.token != session.token:
                raise NetworkError("Credential changed during multiplex handshake", retryable=True)
            if not self._current(generation):
                return
            self._connecting = False
            self._ready = True
            self._start_timers(generation)
            self._flush()
        except Exception as error:
            if self._current(generation):
                self._fail(error if isinstance(error, HumChatError) else ProtocolError("Multiplex connection failed"))
        finally:
            lifetime.dispose()

    def _validate_capabilities(self, session: VerifiedSession) -> None:
        cap = session.capabilities.messaging
        multiplexed = cap.multiplexed_events
        if not cap.durable_replay or 1 not in cap.event_protocol_versions or multiplexed is None or 1 not in multiplexed.protocol_versions:
            raise UnsupportedFeatureError("Server does not advertise multiplex event protocol v1")
        self._limit = multiplexed.max_channels

    def _open(self, session: VerifiedSession, generation: int) -> asyncio.Future[None]:
        config = self._manager.config
        loop = asyncio.get_running_loop()
        result: asyncio.Future[None] = loop.create_future()
        result.add_done_callback(lambda future: future.cancelled() or future.exception())
        state = {"ready": False, "failed": False}
        socket = _ws.Socket(
            ws_url(config.base_url, "/ws/events"),
            {"Authorization": f"Bearer {session.token}", "X-HumChat-Stream-Protocol": "v1"},
            max_payload=config.realtime["max_payload_bytes"],
            handshake_timeout_ms=config.timeout_ms,
        )
        self._socket = socket
        cycle = self._cycle

        def cleanup() -> None:
            timer.cancel()
            cycle.signal.remove_listener(cancel)

        def fail(error: HumChatError) -> None:
            if state["failed"]:
                return
            state["failed"] = True
            cleanup()
            if not state["ready"]:
                if not result.done():
                    result.set_exception(error)
            elif self._current(generation) and self._socket is socket:
                self._fail(error)
            socket.terminate()

        timer = _clock.call_later(config.timeout_ms, lambda: fail(TimeoutError("Multiplex ready handshake timed out", retryable=True)))

        def cancel(_: Any = None) -> None:
            fail(AbortError("Multiplex handshake cancelled"))

        cycle.signal.add_listener(cancel)

        def unexpected(status: int, headers: Any) -> None:
            fail(
                upgrade_error(
                    status,
                    headers,
                    {
                        "401": "Multiplex authentication rejected",
                        "403": "Multiplex permission denied",
                        "404": "Multiplex endpoint is unavailable",
                        "429": "Multiplex rate limit exceeded",
                        "408": "Multiplex upgrade timed out",
                        "5xx": "Multiplex service unavailable",
                        "other": "Multiplex negotiation rejected",
                    },
                    not_found_unsupported=True,
                )
            )

        def message(data: str | bytes, binary: bool) -> None:
            if not self._current(generation) or self._socket is not socket:
                return
            try:
                if binary:
                    raise ProtocolError("Unexpected binary multiplex frame")
                raw = json_loads(data)
                if not isinstance(raw, dict):
                    raise ProtocolError("Invalid multiplex frame")
                if raw.get("type") == "session_revoked":
                    parsed = parse_frame(raw, self.org_id, "")
                    if parsed.kind in ("session_revoked", "session_expired") and self._manager.end_session(parsed, session):
                        fail(NetworkError("Socket token expired; reconnecting with a fresh session", retryable=True))
                    return
                if raw.get("type") == "stream_error":
                    raise ProtocolError("Multiplex stream rejected by server")
                if not state["ready"]:
                    limit: Any = raw.get("max_channels")
                    if (
                        raw.get("type") != "stream_ready"
                        or raw.get("schema") != 1
                        or isinstance(raw.get("schema"), bool)
                        or raw.get("org_uuid") != self.org_id
                        or not is_safe_int(limit)
                        or limit < 1
                        or limit > self._limit
                    ):
                        raise ProtocolError("Invalid multiplex stream ready")
                    self._limit = int(limit)
                    state["ready"] = True
                    cleanup()
                    if not result.done():
                        result.set_result(None)
                    return
                self._frame(raw)
            except HumChatError as error:
                fail(error)
            except Exception:
                fail(ProtocolError("Invalid multiplex JSON"))

        def pong() -> None:
            if self._current(generation) and socket is self._socket and self._pong is not None:
                self._pong.cancel()
                self._pong = None

        def error(failure: _ws.SocketError) -> None:
            if failure.code == _ws.PAYLOAD_TOO_BIG:
                fail(BackpressureError("Multiplex frame exceeds the configured payload limit"))
            else:
                fail(NetworkError("Multiplex transport failed", retryable=True))

        def closed() -> None:
            cleanup()
            if self._current(generation) and socket is self._socket:
                fail(NetworkError("Multiplex socket closed", retryable=True))

        socket.on_unexpected_response = unexpected
        socket.on_message = message
        socket.on_pong = pong
        socket.on_error = error
        socket.on_close = closed
        return result

    def _frame(self, raw: Frame) -> None:
        if raw.get("type") == "stream_ready":
            raise ProtocolError("Duplicate multiplex stream ready")
        identifier = raw.get("subscription_id")
        if not isinstance(identifier, str) or not _CORRELATION.fullmatch(identifier):
            raise ProtocolError("Missing multiplex correlation ID")
        lease = self._ids.get(identifier)
        if lease is None:
            return  # Late frames cannot reach a replacement lease for the same channel.
        if raw.get("type") == "unsubscribed":
            raise ProtocolError("Unexpected unsubscribe acknowledgement for an active lease")
        if raw.get("channel_id") != lease.channel_id or ("org_uuid" in raw and raw["org_uuid"] != self.org_id):
            raise ProtocolError("Multiplex channel scope mismatch")
        if raw.get("type") == "channel_error":
            status: Any = raw.get("status")
            schema = raw.get("schema")
            if schema != 1 or isinstance(schema, bool) or raw.get("retryable") is not False or not is_safe_int(status) or not 400 <= status <= 599:
                raise ProtocolError("Invalid multiplex channel error")
            options: dict[str, Any] = {"status": int(status), "channel_id": lease.channel_id}
            code = raw.get("code")
            failure: HumChatError | None = (
                PermissionError("Channel permission denied", **options)
                if code == "FORBIDDEN"
                else NotFoundError("Channel not found", **options)
                if code == "NOT_FOUND"
                else BackpressureError("Multiplex channel limit reached", **options)
                if code == "LIMIT_EXCEEDED"
                else ValidationError("Multiplex channel request rejected", **options)
                if code == "INVALID_REQUEST"
                else ServerError("Multiplex channel rejected", **options)
                if code == "SERVER_ERROR"
                else None
            )
            if failure is None:
                raise ProtocolError("Unknown multiplex channel error")
            lease.reject(failure)
            return
        if raw.get("type") == "ready":
            lease.accept(raw)
            return
        if not lease.ready:
            raise ProtocolError("Multiplex channel data arrived before ready")
        if raw.get("type") == "watermark":
            watermark = parse_ready({**raw, "type": "ready"}, self.org_id, lease.channel_id)
            lease.frame({"type": "watermark", "cursor": watermark.cursor})
            return
        lease.frame(raw)

    def _flush(self) -> None:
        if not self._ready or not self.socket_open:
            return
        for lease in list(self._queue):
            if lease in self._queue:
                self._queue.remove(lease)
            if not lease.pending or lease.id:
                continue
            if len(self._ids) >= self._limit:
                lease.reject(BackpressureError("Multiplex server channel limit reached", channel_id=lease.channel_id))
                continue
            self._serial += 1
            identifier = f"s{self._serial}"
            lease.id = identifier
            lease.deadline = _clock.now_ms() + self._manager.config.timeout_ms
            self._ids[identifier] = lease
            if not self._write({"type": "subscribe", "subscription_id": identifier, "channel_id": lease.channel_id}):
                return

    def _write(self, frame: Frame) -> bool:
        if not self._ready or not self.socket_open:
            return False
        data = json_dumps(frame)
        size = utf8_len(data)
        limit = self._manager.config.realtime["max_buffered_bytes"]
        if size > limit:
            self.block(BackpressureError("Multiplex command exceeds the configured buffer limit"))
            return False
        socket = self._socket
        assert socket is not None
        if socket.buffered_amount + size > limit:
            self._fail(BackpressureError("Multiplex signal buffer exceeded", retryable=True))
            return False
        sent = socket.send(data)

        def done(future: asyncio.Future[None]) -> None:
            if (future.cancelled() or future.exception() is not None) and socket is self._socket:
                self._fail(NetworkError("Multiplex send failed", retryable=True))

        sent.add_done_callback(done)
        return True

    async def send(self, frame: Frame, signal: AbortSignal) -> None:
        if not self._ready or not self.socket_open:
            raise NetworkError("Multiplex stream is not live")
        socket = self._socket
        assert socket is not None
        data = json_dumps(frame)
        if socket.buffered_amount + utf8_len(data) > self._manager.config.realtime["max_buffered_bytes"]:
            raise BackpressureError("Multiplex signal buffer exceeded")
        try:
            await race_signal(socket.send(data), signal)
        except HumChatError:
            raise
        except Exception:
            raise NetworkError("Multiplex signal send failed") from None

    def _start_timers(self, generation: int) -> None:
        config = self._manager.config.realtime

        def heartbeat() -> None:
            if not self._current(generation) or not self.socket_open or self._pong is not None:
                return
            socket = self._socket
            assert socket is not None

            def failed(_: Any) -> None:
                if self._current(generation):
                    self._fail(NetworkError("Multiplex ping failed", retryable=True))

            socket.ping(failed)

            def deadline() -> None:
                if self._current(generation):
                    self._fail(TimeoutError("Multiplex pong deadline exceeded", retryable=True))

            self._pong = _clock.call_later(config["pong_timeout_ms"], deadline)

        self._heartbeat = _clock.call_every(config["ping_interval_ms"], heartbeat)

        def poll() -> None:
            if self._current(generation):
                for lease in list(self._leases):
                    lease.poll()

        self._poll = _clock.call_every(5000, poll)

        def supervise() -> None:
            if not self._current(generation):
                return
            for lease in list(self._ids.values()):
                if lease.deadline is not None and lease.deadline <= _clock.now_ms():
                    lease.reject(TimeoutError("Multiplex channel ready deadline exceeded"))

        self._supervisor = _clock.call_every(min(1000, self._manager.config.timeout_ms), supervise)

        def stable() -> None:
            if self._current(generation):
                self._attempt = 0

        self._stable = _clock.call_later(30_000, stable)
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
                    self._fail(error if isinstance(error, HumChatError) else AuthenticationError("Multiplex credential refresh failed"))
            finally:
                lifetime.dispose()

        wait = min(60_000, max(1, self._session.refresh_at * 1000 - _clock.now_ms()))
        self._refresh = _clock.call_later(wait, lambda: spawn(refresh()))
