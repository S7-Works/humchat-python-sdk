"""Event-driven adapter over the ``websockets`` asyncio client.

The realtime workers were designed around callback sockets (open / message / error /
close / pong). This adapter gives them that shape while never following redirects and
never using environment proxies, so a credential-bearing upgrade only reaches the
configured origin.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from .._signal import spawn

CONNECTING, OPEN, CLOSING, CLOSED = 0, 1, 2, 3
PAYLOAD_TOO_BIG = "payload_too_big"


class _NoRedirectConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


class SocketError(Exception):
    def __init__(self, code: str | None = None) -> None:
        super().__init__(code or "socket error")
        self.code = code


class Socket:
    """One client WebSocket. Handlers are plain callables set by the owner."""

    def __init__(self, url: str, headers: Mapping[str, str], *, max_payload: int, handshake_timeout_ms: int) -> None:
        self.ready_state = CONNECTING
        self.on_open: Callable[[], None] = lambda: None
        self.on_message: Callable[[str | bytes, bool], None] = lambda data, binary: None
        self.on_unexpected_response: Callable[[int, Mapping[str, str]], None] = lambda status, headers: None
        self.on_error: Callable[[SocketError], None] = lambda error: None
        self.on_close: Callable[[], None] = lambda: None
        self.on_pong: Callable[[], None] = lambda: None
        self._conn: ClientConnection | None = None
        self._terminated = False
        self._closed_emitted = False
        self._connector = _NoRedirectConnect(
            url,
            additional_headers=dict(headers),
            max_size=max_payload,
            compression=None,
            open_timeout=handshake_timeout_ms / 1000,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=1,
            proxy=None,
        )
        self._task = spawn(self._run())

    def __repr__(self) -> str:
        return f"Socket(state={self.ready_state})"

    @property
    def buffered_amount(self) -> int:
        conn = self._conn
        if conn is None:
            return 0
        try:
            return int(conn.transport.get_write_buffer_size())
        except Exception:
            return 0

    def _emit_close(self) -> None:
        self.ready_state = CLOSED
        if not self._closed_emitted:
            self._closed_emitted = True
            self.on_close()

    async def _run(self) -> None:
        try:
            conn = await self._connector
        except InvalidStatus as error:
            if self._terminated:
                return
            self.ready_state = CLOSED
            response = error.response
            self.on_unexpected_response(response.status_code, {key.lower(): value for key, value in response.headers.raw_items()})
            self._emit_close()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._terminated:
                return
            self.ready_state = CLOSED
            self.on_error(SocketError())
            self._emit_close()
            return
        if self._terminated:
            conn.transport.abort()
            return
        self._conn = conn
        self.ready_state = OPEN
        self.on_open()
        failure: SocketError | None = None
        try:
            while True:
                data = await conn.recv()
                if self._terminated:
                    return
                self.on_message(data, isinstance(data, bytes))
        except ConnectionClosed as closed:
            sent = closed.sent
            if sent is not None and sent.code == 1009:
                failure = SocketError(PAYLOAD_TOO_BIG)
            elif closed.rcvd is None or (closed.rcvd.code not in (1000, 1001)):
                failure = SocketError()
        except asyncio.CancelledError:
            raise
        except Exception:
            failure = SocketError()
        if self._terminated:
            return
        if failure is not None:
            self.on_error(failure)
        self._emit_close()

    def send(self, text: str) -> asyncio.Future[None]:
        """Queue a text frame; the returned future fails if the socket cannot take it."""
        conn = self._conn
        if conn is None or self.ready_state != OPEN:
            future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            future.set_exception(SocketError("not open"))
            future.exception()
            return future
        return spawn(conn.send(text))

    def ping(self, on_error: Callable[[Exception], None]) -> None:
        conn = self._conn
        if conn is None or self.ready_state != OPEN:
            on_error(SocketError("not open"))
            return

        async def run() -> None:
            try:
                waiter = await conn.ping()
            except Exception as error:
                on_error(error)
                return
            try:
                await waiter
            except Exception:
                return
            if not self._terminated:
                self.on_pong()

        spawn(run())

    def terminate(self) -> None:
        if self._terminated:
            return
        self._terminated = True
        conn = self._conn
        if conn is not None:
            try:
                conn.transport.abort()
            except Exception:
                pass
        else:
            self._task.cancel()
        loop = asyncio.get_running_loop()
        loop.call_soon(self._emit_close)
