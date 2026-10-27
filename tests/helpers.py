"""Shared test helpers. Every credential here is a fixed synthetic string."""

from __future__ import annotations

import asyncio
import heapq
import inspect
import itertools
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from aiohttp import WSMsgType, web

from humchat import HumChatError, _clock

ROOT = Path(__file__).resolve().parent.parent
CONTRACT = json.loads((ROOT / "contract" / "fixtures" / "http-ws.json").read_text(encoding="utf-8"))
SAMPLES: dict[str, Any] = CONTRACT["samples"]


def json_response(value: Any, status: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, content=json.dumps(value).encode("utf-8"), headers={"content-type": "application/json", **(headers or {})})


def text_response(body: str | bytes, status: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, content=body if isinstance(body, bytes) else body.encode("utf-8"), headers=headers or {})


@dataclass
class Recorded:
    """One request seen by a mock transport (``init`` equivalent of the Node tests)."""

    method: str
    url: httpx.URL
    path: str
    headers: httpx.Headers
    body: bytes | None
    request: httpx.Request
    cancelled: bool = False

    def json(self) -> Any:
        return json.loads(self.body or b"null")


Handler = Callable[[Recorded], "httpx.Response | Awaitable[httpx.Response]"]


class MockServer:
    """``fetch`` replacement: an httpx MockTransport recording every request."""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.requests: list[Recorded] = []
        self.transport = httpx.MockTransport(self._handle)

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        entry = Recorded(request.method, request.url, request.url.path, request.headers, body or None, request)
        self.requests.append(entry)
        try:
            result = self.handler(entry)
            if inspect.isawaitable(result):
                result = await result
            return result
        except asyncio.CancelledError:
            entry.cancelled = True
            raise

    def paths(self) -> list[str]:
        return [request.path for request in self.requests]

    def count(self, path: str) -> int:
        return sum(1 for request in self.requests if request.path == path)


def code(expected: str, status: int | None = None) -> Callable[[BaseException], bool]:
    def check(error: BaseException) -> bool:
        assert isinstance(error, HumChatError), repr(error)
        assert error.code == expected, f"{error.code} != {expected}: {error}"
        if status is not None:
            assert error.status == status
        return True

    return check


async def rejects(awaitable: Awaitable[Any], expected: Any = HumChatError) -> BaseException:
    """``assert.rejects``: ``expected`` is an exception class, a predicate, or an error code string."""
    try:
        await awaitable
    except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
            raise
        if isinstance(expected, str):
            code(expected)(error)
        elif isinstance(expected, type):
            assert isinstance(error, expected), f"{type(error).__name__}: {error}"
        else:
            assert expected(error)
        return error
    raise AssertionError("Expected the operation to fail")


async def until(predicate: Callable[[], bool], label: str, timeout: float = 2.5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("Timed out: " + label)


class Deferred:
    def __init__(self) -> None:
        self.future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    def resolve(self, value: Any = None) -> None:
        if not self.future.done():
            self.future.set_result(value)

    def __await__(self) -> Any:
        return asyncio.shield(self.future).__await__()


class Observed:
    """``observe(promise)`` from the Node tests: inspect settlement without awaiting."""

    def __init__(self, awaitable: Awaitable[Any]) -> None:
        self.state = "pending"
        self.value: Any = None
        self.error: BaseException | None = None
        self.task = asyncio.ensure_future(self._run(awaitable))

    async def _run(self, awaitable: Awaitable[Any]) -> None:
        try:
            self.value = await awaitable
            self.state = "fulfilled"
        except BaseException as error:
            self.error = error
            self.state = "rejected"


# --- virtual time ------------------------------------------------------------------------------


@dataclass(order=True)
class _Timer:
    due: float
    serial: int
    callback: Callable[[], Any] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)

    def cancel(self) -> None:
        self.cancelled = True


class FakeClock:
    """Virtual ``Date.now``/``setTimeout``/``Math.random`` (``t.mock.timers`` equivalent)."""

    def __init__(self, monkeypatch: Any, now: float = 1_800_000_000_000.0, random: float | None = None, timers: bool = True) -> None:
        self.now = now
        self._timers: list[_Timer] = []
        self._serial = itertools.count()
        monkeypatch.setattr(_clock, "now_ms", lambda: self.now)
        if timers:
            monkeypatch.setattr(_clock, "call_later", self.call_later)
        if random is not None:
            self.random = random
            monkeypatch.setattr(_clock, "random", lambda: self.random)

    def call_later(self, milliseconds: float, callback: Callable[[], Any]) -> _Timer:
        timer = _Timer(self.now + max(0.0, milliseconds), next(self._serial), callback)
        heapq.heappush(self._timers, timer)
        return timer

    def tick(self, milliseconds: float = 0) -> None:
        target = self.now + milliseconds
        while self._timers and self._timers[0].due <= target:
            timer = heapq.heappop(self._timers)
            if timer.cancelled:
                continue
            self.now = max(self.now, timer.due)
            timer.callback()
        self.now = target

    async def drain(self, turns: int = 12) -> None:
        for _ in range(turns):
            self.tick(0)
            for _ in range(5):
                await asyncio.sleep(0)

    async def advance(self, milliseconds: float) -> None:
        self.tick(milliseconds)
        await self.drain()


# --- loopback HTTP + WebSocket peer ------------------------------------------------------------


@dataclass
class PeerSocket:
    ws: web.WebSocketResponse
    path: str
    query: dict[str, str]
    headers: dict[str, str]
    at: float
    frames: list[Any] = field(default_factory=list)
    pings: int = 0

    @property
    def open(self) -> bool:
        return not self.ws.closed

    async def send(self, frame: Any) -> None:
        await self.ws.send_str(frame if isinstance(frame, str) else json.dumps(frame))

    async def terminate(self) -> None:
        transport = self.ws._req.transport if self.ws._req is not None else None
        if transport is not None:
            transport.abort()


@dataclass
class Upgrade:
    path: str
    query: dict[str, str]
    headers: dict[str, str]
    at: float
    number: int


class LoopbackPeer:
    """A controlled HTTP and WebSocket protocol peer on 127.0.0.1 (``createServer`` + ``ws``).

    ``http(request) -> (status, json)`` handles plain requests. ``upgrade(upgrade)`` may return
    a status (int) or ``(status, headers)`` to reject a WebSocket upgrade. ``on_socket`` is
    called after a socket opens; ``on_frame(socket, frame)`` for each client text frame.
    """

    def __init__(
        self,
        http: Callable[[web.Request], Awaitable[tuple[int, Any] | web.StreamResponse]] | None = None,
        *,
        upgrade: Callable[[Upgrade], Awaitable[Any]] | None = None,
        on_socket: Callable[[PeerSocket], Awaitable[None] | None] | None = None,
        on_frame: Callable[[PeerSocket, Any], Awaitable[None] | None] | None = None,
        auto_pong: bool = True,
        ws_paths: tuple[str, ...] = ("/ws/chat", "/ws/events"),
    ) -> None:
        self.http_handler = http
        self.upgrade_handler = upgrade
        self.on_socket = on_socket
        self.on_frame = on_frame
        self.auto_pong = auto_pong
        self.ws_paths = ws_paths
        self.requests: list[web.Request] = []
        self.request_log: list[dict[str, Any]] = []
        self.upgrades: list[Upgrade] = []
        self.sockets: list[PeerSocket] = []
        self.frames: list[tuple[PeerSocket, Any]] = []
        self.fixture_errors: list[BaseException] = []
        self.ping_count = 0
        self._runner: web.AppRunner | None = None
        self.base_url = ""

    async def start(self) -> LoopbackPeer:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app, handle_signals=False, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        port = server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self

    async def close(self) -> None:
        for socket in list(self.sockets):
            await socket.terminate()
        if self._runner is not None:
            await self._runner.cleanup()
        assert self.fixture_errors == [], f"Controlled peer must not hide fixture errors: {self.fixture_errors!r}"

    def latest(self, path: str | None = None, **query: str) -> PeerSocket | None:
        for socket in reversed(self.sockets):
            if not socket.open:
                continue
            if path is not None and socket.path != path:
                continue
            if any(socket.query.get(key) != value for key, value in query.items()):
                continue
            return socket
        return None

    async def _dispatch(self, request: web.Request) -> web.StreamResponse:
        is_upgrade = request.headers.get("upgrade", "").lower() == "websocket"
        if is_upgrade and request.path in self.ws_paths:
            return await self._upgrade(request)
        self.requests.append(request)
        body = await request.read()
        self.request_log.append(
            {"path": request.path, "query": dict(request.query), "headers": dict(request.headers), "body": body, "at": time.time() * 1000}
        )
        try:
            if self.http_handler is None:
                return web.json_response({}, status=404)
            result = await self.http_handler(request)
            if isinstance(result, web.StreamResponse):
                return result
            status, value = result
            return web.json_response(value, status=status)
        except Exception as error:
            self.fixture_errors.append(error)
            return web.json_response({}, status=500)

    async def _upgrade(self, request: web.Request) -> web.StreamResponse:
        upgrade = Upgrade(request.path, dict(request.query), {k.lower(): v for k, v in request.headers.items()}, time.time() * 1000, len(self.upgrades) + 1)
        self.upgrades.append(upgrade)
        try:
            rejected = await self.upgrade_handler(upgrade) if self.upgrade_handler is not None else None
        except Exception as error:
            self.fixture_errors.append(error)
            return web.Response(status=500)
        if rejected:
            status, headers = rejected if isinstance(rejected, tuple) else (rejected, {})
            return web.Response(status=status, headers=headers)
        ws = web.WebSocketResponse(autoping=False, max_msg_size=0, compress=False)
        await ws.prepare(request)
        socket = PeerSocket(ws, request.path, dict(request.query), upgrade.headers, time.time() * 1000)
        self.sockets.append(socket)
        try:
            if self.on_socket is not None:
                result = self.on_socket(socket)
                if inspect.isawaitable(result):
                    await result
            async for message in ws:
                if message.type == WSMsgType.TEXT:
                    frame = json.loads(message.data)
                    socket.frames.append(frame)
                    self.frames.append((socket, frame))
                    if self.on_frame is not None:
                        result = self.on_frame(socket, frame)
                        if inspect.isawaitable(result):
                            await result
                elif message.type == WSMsgType.PING:
                    self.ping_count += 1
                    socket.pings += 1
                    if self.auto_pong:
                        await ws.pong(message.data)
                elif message.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                    break
        except Exception as error:
            if not ws.closed:
                self.fixture_errors.append(error)
        return ws
