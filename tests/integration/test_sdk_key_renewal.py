"""Port of test/integration/sdk-key-renewal.test.mjs.

This loopback protocol peer exercises the real SDK-key HTTP mint and real sockets
together. All identities and credentials are synthetic.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import math
import re
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from aiohttp import WSMsgType, web

from helpers import Deferred, rejects
from humchat import HumChat, MemoryCheckpointStore, PersistenceScope, SessionRevokedError

ORG = "00000000-0000-4000-8000-000000000041"
ACTOR = "synthetic-renewal-member"
EMAIL = "synthetic.renewal@example.invalid"
SDK_KEY = "hsk_synthetic_renewal_fixture"
CHANNELS = [f"{ORG}:team:{name}" for name in ("first", "second")]
EPOCH = "synthetic-renewal-epoch"
BEARER = re.compile(r"^Bearer synthetic-renewal-token-\d+$")


def token(number: int) -> str:
    return f"synthetic-renewal-token-{number}"


def cursor(channel_id: str, sequence: int | str = 0) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def position(value: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))["sequence"])


def now_ms() -> float:
    return time.time() * 1000


async def until(condition: Callable[[], Any], label: str, timeout: float = 4.5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"Timed out: {label}")


async def call(hook: Callable[..., Any] | None, item: dict[str, Any]) -> Any:
    if hook is None:
        return None
    value = hook(item)
    return await value if inspect.isawaitable(value) else value


class IgnoreCancellationTransport(httpx.AsyncBaseTransport):
    """A custom transport that keeps requests running after the SDK cancels them."""

    def __init__(self) -> None:
        self.inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        task = asyncio.ensure_future(self.inner.handle_async_request(request))
        return await asyncio.shield(task)

    async def aclose(self) -> None:
        await self.inner.aclose()


class Peer:
    def __init__(self, mode: str, **options: Any) -> None:
        self.mode = mode
        self.options = options
        self.requests: list[dict[str, Any]] = []
        self.mints: list[dict[str, Any]] = []
        self.upgrades: list[dict[str, Any]] = []
        self.sockets: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self.clients: list[HumChat] = []
        self.streams: dict[str, list[dict[str, Any]]] = {}
        self.base_url = ""
        self._runner: web.AppRunner | None = None
        authentication: dict[str, Any] = {
            "user_sessions": True, "service_identities": False, "api_key_exchange": False, "app_binding_required": False,
            "mock_authentication": True,
            "sdk_key_user_tokens": {"endpoint": "/api/sdk/auth/user-token", "key_header": "X-HumChat-Sdk-Key", "configured": True},
        }  # fmt: skip
        if options.get("advertise_expiry", True):
            authentication["socket_session_end_codes"] = ["TOKEN_EXPIRED"]
        self.capabilities = {
            "sdk_protocol_version": 1,
            "authentication": authentication,
            "messaging": {
                "send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
                "durable_replay": mode != "best_effort", "event_protocol_versions": [1], "websocket_scope": "channel",
                "multiplexed_events": {"protocol_versions": [1], "max_channels": 100},
            },
        }  # fmt: skip

    def stream(self, channel_id: str) -> list[dict[str, Any]]:
        return self.streams.setdefault(channel_id, [])

    async def start(self) -> Peer:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app, handle_signals=False, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        self.base_url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        return self

    async def close(self) -> None:
        await asyncio.gather(*(client.close() for client in self.clients), return_exceptions=True)
        self.drop()
        if self._runner is not None:
            await self._runner.cleanup()
        assert self.errors == [], f"Synthetic peer must not hide fixture failures: {self.errors!r}"
        for item in self.mints:
            assert "authorization" not in item["headers"]
            assert item["headers"].get("x-humchat-sdk-key") == SDK_KEY
            assert item["body"] == {"org_uuid": ORG, "email": EMAIL, "expires_in": 900}

    def scope(self, consumer_id: str, channel_id: str) -> PersistenceScope:
        return PersistenceScope(server_origin=self.base_url, org_id=ORG, actor_id=ACTOR, consumer_id=consumer_id, channel_id=channel_id)

    def append(self, channel_id: str, message_id: str) -> dict[str, Any]:
        rows = self.stream(channel_id)
        sequence, timestamp = str(len(rows) + 1), 1_792_370_346
        event = {
            "type": "event", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": sequence,
            "event_id": f"{EPOCH}:{sequence}", "cursor": cursor(channel_id, sequence), "actor_id": ACTOR, "kind": "message",
            "state": {"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "message_id": message_id, "sender_id": ACTOR,
                      "created_at": timestamp, "thread_root_id": None, "priority": "standard", "original_body": message_id,
                      "body": message_id, "revision": 1, "updated_at": timestamp, "deleted": False, "reaction_revision": 0,
                      "reactions": {}, "attachments": []},
        }  # fmt: skip
        rows.append(event)
        return event

    def client(self, **extra: Any) -> HumChat:
        realtime = {
            "transport": "multiplex" if self.mode == "multiplex" else "channel", "max_channels": 5, "connect_concurrency": 2,
            "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20, "ping_interval_ms": 1000, "pong_timeout_ms": 500,
            **extra.pop("realtime", {}),
        }  # fmt: skip
        options: dict[str, Any] = {
            "org_id": ORG, "base_url": self.base_url, "auth": {"sdk_key": SDK_KEY, "act_as": EMAIL}, "timeout_ms": 1500,
            # Generous total budget: these tests assert sharing, not timing, and CI runners are slow.
            "retry": {"max_retries": 0, "total_timeout_ms": 10000},
            "recovery": {"mode": "best_effort" if self.mode == "best_effort" else "durable"}, **extra, "realtime": realtime,
        }  # fmt: skip
        client = HumChat(**options)
        self.clients.append(client)
        return client

    async def send(self, frame: dict[str, Any]) -> None:
        for entry in self.sockets:
            if not entry["ws"].closed:
                await entry["ws"].send_str(json.dumps(frame))

    def drop(self) -> None:
        for entry in self.sockets:
            request = entry["ws"]._req
            if request is not None and request.transport is not None:
                request.transport.abort()

    def _ready(self, channel_id: str, subscription_id: str | None = None) -> dict[str, Any]:
        size = len(self.stream(channel_id))
        return {"type": "ready", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "cursor": cursor(channel_id, size),
                "sequence": str(size), "earliest_cursor": cursor(channel_id),
                **({"subscription_id": subscription_id} if subscription_id else {})}  # fmt: skip

    async def _dispatch(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await self._upgrade(request)
        try:
            return await self._http(request)
        except Exception as error:
            self.errors.append(error)
            return web.json_response({}, status=500)

    async def _http(self, request: web.Request) -> web.StreamResponse:
        item: dict[str, Any] = {"path": request.path, "query": dict(request.query), "headers": {k.lower(): v for k, v in request.headers.items()},
                                "at": now_ms(), "request": request}  # fmt: skip
        self.requests.append(item)
        if request.path == "/api/sdk/auth/user-token":
            item["body"] = await request.json()
            item["number"] = len(self.mints) + 1
            self.mints.append(item)
            custom = await call(self.options.get("mint"), item) or {}
            if custom.get("status"):
                return web.json_response({}, status=custom["status"], headers=custom.get("headers", {}))
            expires_in = custom.get("expires_in", self.options.get("first_ttl", 900) if item["number"] == 1 else 900)
            return web.json_response({"access_token": token(item["number"]), "token_type": "Bearer", "expires_in": expires_in})
        assert "x-humchat-sdk-key" not in item["headers"], "SDK key belongs only on mint"
        assert BEARER.match(item["headers"].get("authorization", ""))
        if request.path == "/me":
            identity = await call(self.options.get("identity"), item) or {}
            return web.json_response({"user_uuid": ACTOR, "org": ORG, "email": EMAIL, "roles": ["member"], "scopes": [], **identity})
        if request.path == "/api/sdk/capabilities":
            return web.json_response(self.capabilities)
        if request.path == "/api/org/members":
            custom = await call(self.options.get("members"), item) or {}
            if custom.get("status"):
                return web.json_response({}, status=custom["status"])
            return web.json_response([{"user_uuid": ACTOR, "org_uuid": ORG, "name": "Synthetic member", "email": EMAIL, "role": "member", "status": "active"}])
        if request.path == "/api/messages/sync":
            return web.json_response({"schema": 2, "org_uuid": ORG, "channel_id": request.query.get("channel_id"), "thread_root_id": None,
                                      "messages": [], "next_cursor": None})  # fmt: skip
        if request.path == "/api/events/sync":
            channel_id = request.query["channel_id"]
            after = request.query["after"]
            through = request.query.get("through") or cursor(channel_id, len(self.stream(channel_id)))
            events = self.stream(channel_id)[position(after) : position(through)]
            return web.json_response({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "events": events,
                                      "after_cursor": after, "through_cursor": through, "through_sequence": str(position(through)),
                                      "next_cursor": None})  # fmt: skip
        return web.json_response({}, status=404)

    async def _upgrade(self, request: web.Request) -> web.StreamResponse:
        item: dict[str, Any] = {"path": request.path, "query": dict(request.query), "headers": {k.lower(): v for k, v in request.headers.items()},
                                "at": now_ms()}  # fmt: skip
        self.upgrades.append(item)
        try:
            assert "x-humchat-sdk-key" not in item["headers"], "Socket uses only its minted Bearer"
            assert BEARER.match(item["headers"].get("authorization", ""))
            assert "token" not in item["query"] and "sdk_key" not in item["query"]
        except AssertionError as error:
            self.errors.append(error)
            return web.Response(status=500)
        ws = web.WebSocketResponse(max_msg_size=0)
        await ws.prepare(request)
        entry = {**item, "ws": ws, "subscriptions": {}}
        self.sockets.append(entry)
        try:
            if self.mode == "multiplex":
                await ws.send_str(json.dumps({"type": "stream_ready", "schema": 1, "org_uuid": ORG, "max_channels": 100}))
            elif self.mode == "durable":
                await ws.send_str(json.dumps(self._ready(item["query"]["channel_id"])))
            async for message in ws:
                if message.type != WSMsgType.TEXT or self.mode != "multiplex":
                    continue
                frame = json.loads(message.data)
                if frame["type"] == "subscribe":
                    entry["subscriptions"][frame["subscription_id"]] = frame["channel_id"]
                    await ws.send_str(json.dumps(self._ready(frame["channel_id"], frame["subscription_id"])))
                elif frame["type"] == "unsubscribe":
                    entry["subscriptions"].pop(frame["subscription_id"], None)
                    await ws.send_str(json.dumps({"type": "unsubscribed", "schema": 1, "subscription_id": frame["subscription_id"]}))
        except (ConnectionError, RuntimeError, asyncio.CancelledError):
            pass
        return ws


async def make_peer(mode: str, **options: Any) -> Peer:
    return await Peer(mode, **options).start()


def physical_sockets(mode: str) -> int:
    return 1 if mode == "multiplex" else 2


class Active:
    def __init__(self, subscriptions: list[Any], errors: list[Any]) -> None:
        self.subscriptions = subscriptions
        self.errors = errors

    def live(self) -> bool:
        return all(sub.status == "live" for sub in self.subscriptions)

    def blocked(self) -> bool:
        return all(sub.status == "blocked" for sub in self.subscriptions)


async def subscribe(client: HumChat) -> Active:
    subscriptions = [client.realtime.subscribe(channel) for channel in CHANNELS]
    errors: list[Any] = []
    for sub in subscriptions:
        sub.on("error", errors.append)
    await asyncio.gather(*(sub.ready for sub in subscriptions))
    return Active(subscriptions, errors)


def bearer(item: dict[str, Any]) -> str | None:
    return item["headers"].get("authorization")


@pytest.mark.parametrize("mode", ["durable", "multiplex"])
async def test_native_renewal_resumes_durable_consumption_from_its_committed_checkpoint(mode: str) -> None:
    entered, release = Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 2:
            entered.resolve()
            await release

    fixture = await make_peer(mode, first_ttl=3, mint=mint)
    store = MemoryCheckpointStore()
    try:
        client, consumer_id, seen = fixture.client(), "synthetic-renewal-checkpoint", []
        fixture.append(CHANNELS[0], "before-native-renewal")
        first = client.realtime.subscribe(CHANNELS[0], start="earliest")
        first.consume(lambda event, _: seen.append(event.state.id), consumer_id=consumer_id, checkpoint_store=store)
        second = client.realtime.subscribe(CHANNELS[1])
        await asyncio.gather(first.ready, second.ready)
        scope = fixture.scope(consumer_id, CHANNELS[0])

        async def at(sequence: int) -> bool:
            return await store.load(scope) == cursor(CHANNELS[0], sequence)

        await until(lambda: at(1), "first event checkpoint committed")
        await entered
        # No live hint is sent. Rotation must recover the new event through HTTP, starting
        # from the consumer's existing checkpoint rather than "now".
        fixture.append(CHANNELS[0], "during-native-renewal")
        release.resolve()

        async def caught_up() -> bool:
            return first.status == "live" and second.status == "live" and await at(2)

        await until(caught_up, "backlog consumed after token rotation")
        assert seen == ["before-native-renewal", "during-native-renewal"]
        assert len(fixture.mints) == 2
        assert len(fixture.upgrades) == physical_sockets(mode) * 2
        catchup = [
            item
            for item in fixture.requests
            if item["path"] == "/api/events/sync" and bearer(item) == f"Bearer {token(2)}" and item["query"].get("channel_id") == CHANNELS[0]
        ]
        assert any(item["query"].get("after") == cursor(CHANNELS[0], 1) for item in catchup), "New Bearer catches up from the acknowledged position"
    finally:
        release.resolve()
        await store.close()
        await fixture.close()


MODES = ["best_effort", "durable", "multiplex"]


@pytest.mark.parametrize("mode", MODES)
async def test_native_sdk_key_expiry_shares_one_mint_and_rotates_only_bearer_credentials(mode: str) -> None:
    fixture = await make_peer(mode, first_ttl=3)
    try:
        client = fixture.client()
        active = await subscribe(client)
        await until(lambda: len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(), "replacement sockets")
        assert len(fixture.mints) == 2, "Both channels share exactly one renewal"
        renewed = fixture.upgrades[physical_sockets(mode) :]
        assert all(bearer(item) == f"Bearer {token(2)}" for item in renewed)
        first_expiry = (math.floor(fixture.mints[0]["at"] / 1000) + 3) * 1000
        assert all(item["at"] < first_expiry for item in renewed), "Sockets rotate before the original token expires"
        assert (await client.connect()).actor_id == ACTOR
        assert len(await client.members.list()) == 1
        assert len(fixture.mints) == 2, "Verified renewed identity is cached for HTTP"
        assert client.realtime.get_stats().active_sockets == physical_sockets(mode)
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_an_old_http_401_joins_the_socket_renewal_already_in_flight(mode: str) -> None:
    old_request, release_401, renewal, release_mint = Deferred(), Deferred(), Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 2:
            renewal.resolve()
            await release_mint

    async def members(item: dict[str, Any]) -> dict[str, Any] | None:
        if bearer(item) == f"Bearer {token(1)}":
            old_request.resolve()
            await release_401
            return {"status": 401}
        return None

    fixture = await make_peer(mode, first_ttl=3, mint=mint, members=members)
    try:
        # The old-token request is held until the scheduled renewal (~1.5 s after the first
        # 3 s token), so Node's 1500 ms attempt timeout races that hold on slow runners.
        client = fixture.client(timeout_ms=5000)
        active = await subscribe(client)
        pending = asyncio.ensure_future(client.members.list())
        await old_request
        await renewal
        release_401.resolve()
        await asyncio.sleep(0.03)
        assert len(fixture.mints) == 2, "401 must not start a competing mint"
        release_mint.resolve()
        assert len(await pending) == 1
        await until(lambda: len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(), "all sockets adopt shared renewal")
        assert len(fixture.mints) == 2
        roster = [item for item in fixture.requests if item["path"] == "/api/org/members"]
        assert [bearer(item) for item in roster] == [f"Bearer {token(1)}", f"Bearer {token(2)}"]
    finally:
        release_401.resolve()
        release_mint.resolve()
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_ordinary_disconnect_reuses_the_verified_token_without_another_mint(mode: str) -> None:
    fixture = await make_peer(mode)
    try:
        client = fixture.client()
        active = await subscribe(client)
        fixture.drop()
        await until(lambda: len(fixture.upgrades) >= physical_sockets(mode) * 2 and active.live(), "network reconnection")
        assert len(fixture.mints) == 1
        assert all(bearer(item) == f"Bearer {token(1)}" for item in fixture.upgrades)
        assert (await client.connect()).actor_id == ACTOR
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_transient_mint_failures_retry_within_one_shared_renewal(mode: str) -> None:
    fixture = await make_peer(mode, first_ttl=3, mint=lambda item: {"status": 503} if item["number"] in (2, 3) else None)
    try:
        client = fixture.client(retry={"max_retries": 2, "initial_delay_ms": 5, "max_delay_ms": 10, "total_timeout_ms": 4000})
        active = await subscribe(client)
        await until(lambda: len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(), "renewal after bounded transient retries")
        assert len(fixture.mints) == 4, "One renewal makes two retries after its first attempt"
        assert all(bearer(item) == f"Bearer {token(4)}" for item in fixture.upgrades[physical_sockets(mode) :])
        await asyncio.sleep(0.06)
        assert len(fixture.mints) == 4
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_exhausted_transient_mint_attempts_reconnect_and_recover_without_permanent_blocking(mode: str) -> None:
    fixture = await make_peer(mode, first_ttl=3, mint=lambda item: {"status": 503} if item["number"] == 2 else None)
    try:
        client = fixture.client()
        active = await subscribe(client)
        await until(
            lambda: len(fixture.mints) == 3 and len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(),
            "a fresh reconnect cycle retries the transient login outage",
        )
        assert all(bearer(item) == f"Bearer {token(3)}" for item in fixture.upgrades[physical_sockets(mode) :])
        assert all(error.status not in (401, 403) for error in active.errors)
        assert client.realtime.get_stats().active_sockets == physical_sockets(mode)
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_rate_limited_renewal_honors_retry_after_and_still_shares_one_token(mode: str) -> None:
    fixture = await make_peer(
        mode, first_ttl=3, mint=lambda item: {"status": 429, "headers": {"retry-after": "0.08"}} if item["number"] == 2 else None
    )
    try:
        client = fixture.client(retry={"max_retries": 1, "initial_delay_ms": 1, "max_delay_ms": 1, "total_timeout_ms": 4000})
        active = await subscribe(client)
        await until(lambda: len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(), "recovery after login rate limit")
        assert len(fixture.mints) == 3
        assert fixture.mints[2]["at"] - fixture.mints[1]["at"] >= 70, "Retry-After delays the next mint attempt"
        assert all(bearer(item) == f"Bearer {token(3)}" for item in fixture.upgrades[physical_sockets(mode) :])
    finally:
        await fixture.close()


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("mode", MODES)
async def test_mint_status_stops_refresh_and_socket_reconnect(mode: str, status: int) -> None:
    fixture = await make_peer(mode, first_ttl=3, mint=lambda item: {"status": status} if item["number"] > 1 else None)
    try:
        client = fixture.client(retry={"max_retries": 2, "initial_delay_ms": 5, "max_delay_ms": 10, "total_timeout_ms": 4000})
        active = await subscribe(client)
        await until(active.blocked, "permanent login failure blocks both subscriptions")
        await asyncio.sleep(0.1)
        assert len(fixture.mints) == 2, "Permanent mint rejection must not retry"
        assert len(fixture.upgrades) == physical_sockets(mode)
        assert client.realtime.get_stats().active_sockets == 0
        assert any(error.status == status for error in active.errors)
        await rejects(client.members.list(), lambda error: error.status == status)
        assert len(fixture.mints) == 2, "Permanent key rejection remains latched for future HTTP calls"
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_ambiguous_session_revoked_is_terminal_even_when_its_reason_says_expired(mode: str) -> None:
    fixture = await make_peer(mode)
    try:
        client = fixture.client()
        active = await subscribe(client)
        await fixture.send({"type": "session_revoked", "reason": "session expired"})
        await until(active.blocked, "revocation blocks every subscription")
        await rejects(client.members.list(), SessionRevokedError)
        await asyncio.sleep(0.1)
        assert len(fixture.mints) == 1, "A reason string must never authorize reminting"
        assert len(fixture.upgrades) == physical_sockets(mode)
        assert client.realtime.get_stats().active_sockets == 0
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_only_explicit_token_expired_control_permits_shared_remint_and_reconnect(mode: str) -> None:
    entered, release = Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 2:
            entered.resolve()
            await release

    fixture = await make_peer(mode, mint=mint)
    try:
        client = fixture.client()
        active = await subscribe(client)
        revoked: list[Any] = []
        for sub in active.subscriptions:
            sub.on("session_revoked", revoked.append)
        # Multiple channels and duplicate queued frames all refer to the same old session;
        # they must not produce independent renewals or revoke the client.
        await fixture.send({"type": "session_revoked", "code": "TOKEN_EXPIRED", "reason": "session expired"})
        await fixture.send({"type": "session_revoked", "code": "TOKEN_EXPIRED", "reason": "session expired"})
        await entered
        await asyncio.sleep(0.02)
        assert len(fixture.mints) == 2
        release.resolve()
        await until(lambda: len(fixture.upgrades) == physical_sockets(mode) * 2 and active.live(), "explicit expiry recovery")
        await asyncio.sleep(0.05)
        assert len(fixture.mints) == 2, "Queued control from old sockets cannot cause a third mint"
        assert revoked == []
        assert len(await client.members.list()) == 1
        assert all(bearer(item) == f"Bearer {token(2)}" for item in fixture.upgrades[physical_sockets(mode) :])
    finally:
        release.resolve()
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_unknown_control_code_remains_terminal_regardless_of_expiry_reason(mode: str) -> None:
    fixture = await make_peer(mode)
    try:
        client = fixture.client()
        active = await subscribe(client)
        await fixture.send({"type": "session_revoked", "code": "FUTURE_OR_UNKNOWN_REASON", "reason": "session expired"})
        await until(active.blocked, "unknown reason stays terminal")
        await rejects(client.members.list(), SessionRevokedError)
        await asyncio.sleep(0.06)
        assert len(fixture.mints) == 1
        assert len(fixture.upgrades) == physical_sockets(mode)
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_unadvertised_token_expired_control_never_authorizes_minting(mode: str) -> None:
    fixture = await make_peer(mode, advertise_expiry=False)
    try:
        client = fixture.client()
        active = await subscribe(client)
        await fixture.send({"type": "session_revoked", "code": "TOKEN_EXPIRED", "reason": "session expired"})
        await until(active.blocked, "unadvertised control stays terminal")
        await rejects(client.members.list(), SessionRevokedError)
        await asyncio.sleep(0.06)
        assert len(fixture.mints) == 1
        assert len(fixture.upgrades) == physical_sockets(mode)
    finally:
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_revocation_invalidates_a_pending_mint_even_if_custom_transport_ignores_cancellation(mode: str) -> None:
    entered, release = Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 2:
            entered.resolve()
            await release

    fixture = await make_peer(mode, first_ttl=3, mint=mint)
    try:
        client = fixture.client(transport=IgnoreCancellationTransport())
        active = await subscribe(client)
        await entered
        await fixture.send({"type": "session_revoked", "reason": "session deprovisioned"})
        await until(active.blocked, "revocation interrupts pending renewal")
        await rejects(client.members.list(), SessionRevokedError)
        release.resolve()
        await asyncio.sleep(0.1)
        assert len(fixture.mints) == 2
        assert sum(1 for item in fixture.requests if item["path"] == "/me") == 1, (
            "A late minted token must not reach identity verification after revocation"
        )
        assert len(fixture.upgrades) == physical_sockets(mode)
        assert client.realtime.get_stats().active_sockets == 0
    finally:
        release.resolve()
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_close_cancels_a_pending_mint_and_prevents_late_sockets_or_timer_work(mode: str) -> None:
    entered, release, cancelled = Deferred(), Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 2:
            entered.resolve()
            transport = item["request"].transport
            while not release.future.done():
                if transport is None or transport.is_closing():
                    cancelled.resolve()
                await asyncio.sleep(0.01)

    fixture = await make_peer(mode, first_ttl=3, mint=mint)
    try:
        client = fixture.client()
        await subscribe(client)
        await entered
        await client.close()
        await asyncio.wait_for(cancelled.future, 1.5)
        release.resolve()
        await asyncio.sleep(0.1)
        assert len(fixture.mints) == 2
        assert len(fixture.upgrades) == physical_sockets(mode)
        assert client.realtime.get_stats().active_sockets == 0
        await rejects(client.connect(), "ABORT")
    finally:
        release.resolve()
        await fixture.close()


@pytest.mark.parametrize("mode", MODES)
async def test_renewed_identity_drift_blocks_sockets_before_a_new_bearer_is_used(mode: str) -> None:
    fixture = await make_peer(
        mode, first_ttl=3, identity=lambda item: {"user_uuid": "synthetic-different-member"} if bearer(item) == f"Bearer {token(2)}" else None
    )
    try:
        client = fixture.client()
        active = await subscribe(client)
        await until(active.blocked, "identity drift blocks both subscriptions")
        await rejects(client.members.list(), "AUTHENTICATION")
        await asyncio.sleep(0.06)
        assert len(fixture.mints) == 2
        assert len(fixture.upgrades) == physical_sockets(mode), "Mismatched token never reaches any socket"
        assert client.realtime.get_stats().active_sockets == 0
    finally:
        await fixture.close()
