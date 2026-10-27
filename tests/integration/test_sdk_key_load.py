"""Port of test/integration/sdk-key-load.test.mjs.

Bounded loopback concurrency checks, not production throughput measurements. No
credentials or customer data are used, and metrics contain counts only.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import re
import time
from collections.abc import Callable
from typing import Any

import pytest
from aiohttp import WSMsgType, web

from helpers import Deferred
from humchat import HumChat, MemoryCheckpointStore, PersistenceScope

ORG = "00000000-0000-4000-8000-000000000051"
ACTOR = "synthetic-load-member"
EMAIL = "synthetic.load@example.invalid"
SDK_KEY = "hsk_synthetic_load_fixture"
EPOCH = "synthetic-load-epoch"
BEARER = re.compile(r"^Bearer synthetic-load-token-\d+$")


def token(number: int) -> str:
    return f"synthetic-load-token-{number}"


def cursor(channel_id: str, sequence: int | str = 0) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def position(value: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))["sequence"])


async def until(condition: Callable[[], Any], label: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"Timed out: {label}")


class Peer:
    def __init__(self, mode: str, count: int, mint: Callable[[dict[str, Any]], Any] | None = None) -> None:
        self.mode, self.count, self.mint_hook = mode, count, mint
        self.channels = [f"{ORG}:team:load-{index}" for index in range(count)]
        self.requests: list[dict[str, Any]] = []
        self.mints: list[dict[str, Any]] = []
        self.upgrades: list[dict[str, Any]] = []
        self.sockets: list[web.WebSocketResponse] = []
        self.errors: list[BaseException] = []
        self.clients: list[HumChat] = []
        self.streams: dict[str, list[dict[str, Any]]] = {channel: [] for channel in self.channels}
        self.active_mints = 0
        self.maximum_mints = 0
        self.base_url = ""
        self._runner: web.AppRunner | None = None
        self.capabilities = {
            "sdk_protocol_version": 1,
            "authentication": {"user_sessions": True, "service_identities": False, "api_key_exchange": False, "app_binding_required": False,
                               "mock_authentication": True, "socket_session_end_codes": ["TOKEN_EXPIRED"],
                               "sdk_key_user_tokens": {"endpoint": "/api/sdk/auth/user-token", "key_header": "X-HumChat-Sdk-Key", "configured": True}},
            "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2], "durable_replay": True,
                          "event_protocol_versions": [1], "websocket_scope": "channel",
                          "multiplexed_events": {"protocol_versions": [1], "max_channels": 100}},
        }  # fmt: skip

    async def start(self) -> Peer:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app, handle_signals=False, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0, backlog=512)
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
        assert self.errors == [], f"Loopback peer must not hide fixture failures: {self.errors!r}"
        for item in self.mints:
            assert "authorization" not in item["headers"]
            assert item["headers"].get("x-humchat-sdk-key") == SDK_KEY
            assert item["body"] == {"org_uuid": ORG, "email": EMAIL, "expires_in": 900}

    def scope(self, consumer_id: str, channel_id: str) -> PersistenceScope:
        return PersistenceScope(server_origin=self.base_url, org_id=ORG, actor_id=ACTOR, consumer_id=consumer_id, channel_id=channel_id)

    def append(self, round_name: str) -> None:
        for channel_id in self.channels:
            rows, timestamp = self.streams[channel_id], 1_792_370_346
            index = len(rows) + 1
            identifier = f"{round_name}-{channel_id}"
            rows.append({
                "type": "event", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": str(index),
                "event_id": f"{EPOCH}:{index}", "cursor": cursor(channel_id, index), "actor_id": ACTOR, "kind": "message",
                "state": {"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "message_id": identifier, "sender_id": ACTOR,
                          "created_at": timestamp, "thread_root_id": None, "priority": "standard", "original_body": identifier,
                          "body": identifier, "revision": 1, "updated_at": timestamp, "deleted": False, "reaction_revision": 0,
                          "reactions": {}, "attachments": []},
            })  # fmt: skip

    def client(self) -> HumChat:
        realtime: dict[str, Any] = {"transport": self.mode, "reconnect_initial_delay_ms": 20, "reconnect_max_delay_ms": 80,
                                    "ping_interval_ms": 1000, "pong_timeout_ms": 500}  # fmt: skip
        if self.count > 50:
            realtime["max_channels"] = self.count
        # Deviation: Node uses 3 s / 6 s. httpx + loopback TCP setup on Windows hosts takes up to
        # several seconds for 100 concurrent requests, so deadlines are widened; every sharing
        # assertion (one mint, shared cooldown, no late work) is unchanged.
        client = HumChat(org_id=ORG, base_url=self.base_url, auth={"sdk_key": SDK_KEY, "act_as": EMAIL}, timeout_ms=15000,
                         retry={"max_retries": 0, "total_timeout_ms": 30000}, recovery={"mode": "durable"}, realtime=realtime)  # type: ignore[arg-type]  # fmt: skip
        self.clients.append(client)
        return client

    async def expire(self) -> None:
        frame = json.dumps({"type": "session_revoked", "code": "TOKEN_EXPIRED", "reason": "session expired"})
        for ws in list(self.sockets):
            if not ws.closed:
                try:
                    await ws.send_str(frame)
                except (ConnectionError, RuntimeError):
                    pass

    def drop(self) -> None:
        for ws in self.sockets:
            request = ws._req
            if request is not None and request.transport is not None:
                request.transport.abort()

    def all_closed(self) -> bool:
        return all(ws.closed for ws in self.sockets)

    def _ready(self, channel_id: str, subscription_id: str | None = None) -> dict[str, Any]:
        size = len(self.streams[channel_id])
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
        headers = {k.lower(): v for k, v in request.headers.items()}
        item: dict[str, Any] = {"path": request.path, "headers": headers, "at": time.time() * 1000, "request": request}
        self.requests.append(item)
        if request.path == "/api/sdk/auth/user-token":
            item["body"] = await request.json()
            item["number"] = len(self.mints) + 1
            self.mints.append(item)
            self.active_mints += 1
            self.maximum_mints = max(self.maximum_mints, self.active_mints)
            try:
                custom = None
                if self.mint_hook is not None:
                    custom = self.mint_hook(item)
                    if inspect.isawaitable(custom):
                        custom = await custom
                if custom and custom.get("status"):
                    return web.json_response(
                        {"message": f"Deliberately hostile response containing {SDK_KEY} {token(1)}"},
                        status=custom["status"],
                        headers={"x-request-id": SDK_KEY, **custom.get("headers", {})},
                    )
                return web.json_response({"access_token": token(item["number"]), "token_type": "Bearer", "expires_in": 900})
            finally:
                self.active_mints -= 1
        assert "x-humchat-sdk-key" not in headers
        assert BEARER.match(headers.get("authorization", ""))
        if request.path == "/me":
            return web.json_response({"user_uuid": ACTOR, "org": ORG, "email": EMAIL, "roles": ["member"], "scopes": []})
        if request.path == "/api/sdk/capabilities":
            return web.json_response(self.capabilities)
        if request.path == "/api/org/members":
            return web.json_response([{"user_uuid": ACTOR, "org_uuid": ORG, "name": "Synthetic member", "email": EMAIL, "role": "member", "status": "active"}])
        if request.path == "/api/events/sync":
            channel_id = request.query["channel_id"]
            after = request.query["after"]
            through = request.query.get("through") or cursor(channel_id, len(self.streams[channel_id]))
            return web.json_response({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH,
                                      "events": self.streams[channel_id][position(after) : position(through)], "after_cursor": after,
                                      "through_cursor": through, "through_sequence": str(position(through)), "next_cursor": None})  # fmt: skip
        return web.json_response({}, status=404)

    async def _upgrade(self, request: web.Request) -> web.StreamResponse:
        headers = {k.lower(): v for k, v in request.headers.items()}
        try:
            assert "x-humchat-sdk-key" not in headers
            assert BEARER.match(headers.get("authorization", ""))
            assert "token" not in request.query and "sdk_key" not in request.query
        except AssertionError as error:
            self.errors.append(error)
            return web.Response(status=500)
        self.upgrades.append({"path": request.path, "query": dict(request.query), "headers": headers})
        ws = web.WebSocketResponse(max_msg_size=0)
        await ws.prepare(request)
        self.sockets.append(ws)
        try:
            if self.mode == "multiplex":
                await ws.send_str(json.dumps({"type": "stream_ready", "schema": 1, "org_uuid": ORG, "max_channels": 100}))
            else:
                await ws.send_str(json.dumps(self._ready(request.query["channel_id"])))
            async for message in ws:
                if message.type != WSMsgType.TEXT or self.mode != "multiplex":
                    continue
                frame = json.loads(message.data)
                if frame["type"] == "subscribe":
                    await ws.send_str(json.dumps(self._ready(frame["channel_id"], frame["subscription_id"])))
                elif frame["type"] == "unsubscribe":
                    await ws.send_str(json.dumps({"type": "unsubscribed", "schema": 1, "subscription_id": frame["subscription_id"]}))
        except (ConnectionError, RuntimeError, asyncio.CancelledError):
            pass
        return ws


async def close_and_check(client: HumChat, fixture: Peer) -> None:
    await client.close()
    await until(fixture.all_closed, "all peer sockets closed")
    stats = client.realtime.get_stats()
    assert (stats.active_sockets, stats.channels, stats.subscriptions, stats.queued_events, stats.queued_bytes) == (0, 0, 0, 0, 0)
    counts = [len(fixture.requests), len(fixture.mints), len(fixture.upgrades)]
    await asyncio.sleep(0.18)  # Longer than the reconnect cap, to catch orphaned reconnect timers.
    assert [len(fixture.requests), len(fixture.mints), len(fixture.upgrades)] == counts, "Closed client must not start another HTTP call, mint, or socket"


CASES = [("channel", 50), ("multiplex", 100)]


@pytest.mark.parametrize(("mode", "count"), CASES)
async def test_channels_share_auth_and_recover_every_checkpoint_across_rotation_and_disconnect_storms(mode: str, count: int) -> None:
    initial, renewal, release_initial, release_renewal = Deferred(), Deferred(), Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        if item["number"] == 1:
            initial.resolve()
            await release_initial
        if item["number"] == 2:
            renewal.resolve()
            await release_renewal

    fixture = await Peer(mode, count, mint).start()
    store = MemoryCheckpointStore()
    try:
        client = fixture.client()
        fixture.append("initial")
        received: dict[str, list[str]] = {channel: [] for channel in fixture.channels}
        subscriptions = []
        for channel_id in fixture.channels:
            sub = client.realtime.subscribe(channel_id, start="earliest")
            sub.consume(
                lambda event, _, channel_id=channel_id: received[channel_id].append(event.state.id), consumer_id="load-consumer", checkpoint_store=store
            )
            subscriptions.append(sub)
        rosters = [asyncio.ensure_future(client.members.list()) for _ in range(count)]
        await initial
        await asyncio.sleep(0.04)
        assert len(fixture.mints) == 1, "All initial HTTP and channel waiters join one mint"
        release_initial.resolve()
        await asyncio.gather(*(sub.ready for sub in subscriptions), *rosters)

        async def settled_at(sequence: int) -> bool:
            if not all(sub.status == "live" for sub in subscriptions):
                return False
            values = await asyncio.gather(*(store.load(fixture.scope("load-consumer", channel)) for channel in fixture.channels))
            return all(value == cursor(channel, sequence) for value, channel in zip(values, fixture.channels, strict=True))

        await until(lambda: settled_at(1), "initial checkpoints on every channel")
        sockets = count if mode == "channel" else 1
        assert client.realtime.get_stats().active_sockets == sockets
        assert len(fixture.upgrades) == sockets
        await fixture.expire()
        await renewal
        renewed_rosters = [asyncio.ensure_future(client.members.list()) for _ in range(count)]
        await asyncio.sleep(0.04)
        assert len(fixture.mints) == 2, "Expiry storm and HTTP share one renewal"
        fixture.append("during-rotation")
        release_renewal.resolve()
        await asyncio.gather(*renewed_rosters)
        await until(lambda: settled_at(2), "every checkpoint catches up after rotation")
        assert len(fixture.upgrades) == sockets * 2
        assert all(item["headers"].get("authorization") == f"Bearer {token(2)}" for item in fixture.upgrades[sockets:])
        for round_name, sequence in (("drop-one", 3), ("drop-two", 4)):
            fixture.drop()
            fixture.append(round_name)
            await until(lambda sequence=sequence: settled_at(sequence), f"all {count} channels recover {round_name}")
        assert len(fixture.mints) == 2, "Network disconnect storms reuse the verified renewal"
        assert fixture.maximum_mints == 1
        assert len(fixture.upgrades) == sockets * 4
        for channel_id in fixture.channels:
            assert received[channel_id] == [f"{name}-{channel_id}" for name in ("initial", "during-rotation", "drop-one", "drop-two")]
        await close_and_check(client, fixture)
    finally:
        release_initial.resolve()
        release_renewal.resolve()
        await store.close()
        await fixture.close()


@pytest.mark.parametrize("status", [503, 429])
@pytest.mark.parametrize(("mode", "count"), CASES)
async def test_channels_survive_sustained_mint_failure_without_secret_leaks_or_orphaned_work(mode: str, count: int, status: int) -> None:
    outage = False

    def mint(item: dict[str, Any]) -> dict[str, Any] | None:
        if outage and item["number"] > 1:
            return {"status": status, **({"headers": {"retry-after": "0.08"}} if status == 429 else {})}
        return None

    fixture = await Peer(mode, count, mint).start()
    try:
        client = fixture.client()
        failures: list[Any] = []
        subscriptions = []
        for channel_id in fixture.channels:
            sub = client.realtime.subscribe(channel_id)
            sub.on("error", failures.append)
            subscriptions.append(sub)
        await asyncio.gather(*(sub.ready for sub in subscriptions))
        outage = True
        await fixture.expire()
        await until(lambda: len(fixture.mints) > 1, "first failed renewal reaches peer")
        await asyncio.sleep(0.35)
        assert len(fixture.mints) - 1 == 1, "All channel reconnect loops share the native acquisition cooldown after one failed mint"
        outage = False
        await until(lambda: all(sub.status == "live" for sub in subscriptions), "all channels recover after mint service returns")
        assert any(error.status == status for error in failures)
        assert all(sub.status != "blocked" for sub in subscriptions)
        assert fixture.maximum_mints == 1, "Only one native mint is in flight per client"
        exposed = repr(failures) + "".join(repr(vars(error)) + str(error) for error in failures)
        assert SDK_KEY not in exposed
        assert token(1) not in exposed
        for index in range(2, len(fixture.mints)):
            assert fixture.mints[index]["at"] - fixture.mints[index - 1]["at"] >= 990, (
                "Native acquisition cooldown persists across independently scheduled channel reconnects"
            )
        await close_and_check(client, fixture)
    finally:
        await fixture.close()


@pytest.mark.parametrize(("mode", "count"), CASES)
async def test_channels_share_increasing_backoff_across_consecutive_mint_failures(mode: str, count: int) -> None:
    fixture = await Peer(mode, count, lambda item: {"status": 503} if item["number"] in (2, 3) else None).start()
    try:
        client = fixture.client()
        subscriptions = [client.realtime.subscribe(channel_id) for channel_id in fixture.channels]
        await asyncio.gather(*(sub.ready for sub in subscriptions))
        await fixture.expire()
        await until(
            lambda: len(fixture.mints) == 4 and all(sub.status == "live" for sub in subscriptions),
            "two service failures recover through shared increasing backoff",
            10,
        )
        assert len(fixture.mints) == 4, "Each failed acquisition advances one shared backoff stage"
        assert fixture.maximum_mints == 1
        assert fixture.mints[2]["at"] - fixture.mints[1]["at"] >= 990, "First failure imposes at least one second"
        assert fixture.mints[3]["at"] - fixture.mints[2]["at"] >= 1990, "Second failure imposes at least two seconds"
        await close_and_check(client, fixture)
    finally:
        await fixture.close()


@pytest.mark.parametrize(("mode", "count"), CASES)
async def test_closing_queued_channels_and_http_waiters_cancels_shared_login_without_late_work(mode: str, count: int) -> None:
    entered, release, cancelled = Deferred(), Deferred(), Deferred()

    async def mint(item: dict[str, Any]) -> None:
        entered.resolve()
        transport = item["request"].transport
        while not release.future.done():
            if transport is None or transport.is_closing():
                cancelled.resolve()
            await asyncio.sleep(0.01)

    fixture = await Peer(mode, count, mint).start()
    try:
        client = fixture.client()
        subscriptions = [client.realtime.subscribe(channel_id) for channel_id in fixture.channels]
        # Observe every readiness future immediately: close must settle all of them, including
        # workers queued behind the admission slots.
        pending = asyncio.gather(*(sub.ready for sub in subscriptions), *(client.members.list() for _ in range(count)), return_exceptions=True)
        await entered
        await asyncio.sleep(0.04)
        assert len(fixture.mints) == 1
        await client.close()
        results = await pending
        assert all(getattr(result, "code", None) == "ABORT" for result in results)
        await asyncio.wait_for(cancelled.future, 1.5)
        release.resolve()
        await close_and_check(client, fixture)
        assert len(fixture.mints) == 1
        assert len(fixture.upgrades) == 0
        assert sum(1 for item in fixture.requests if item["path"] == "/me") == 0
        assert fixture.maximum_mints == 1
    finally:
        release.resolve()
        await fixture.close()
