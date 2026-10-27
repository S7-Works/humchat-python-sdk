"""Port of test/integration/multiplex.test.mjs (one shared /ws/events stream)."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from helpers import Deferred, LoopbackPeer, PeerSocket, Upgrade, rejects
from humchat import (
    HumChat,
    MemoryCheckpointStore,
    PermissionError,
    PersistenceScope,
    ProtocolError,
    SessionRevokedError,
    UnsupportedFeatureError,
)

ORG = "multiplex-org"
ACTOR = "multiplex-actor"
EPOCH = "multiplex-epoch"


def channel(name: str) -> str:
    return f"{ORG}:team:{name}"


FIRST, SECOND = channel("first"), channel("second")


def cursor(identifier: str, sequence: int | str) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": identifier, "epoch": EPOCH, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).rstrip(b"=").decode("ascii")


def position(value: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))["sequence"])


async def until(predicate: Callable[[], bool], label: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("Timed out: " + label)


async def sleep(milliseconds: float) -> None:
    await asyncio.sleep(milliseconds / 1000)


def observe(subscription: Any) -> dict[str, list[Any]]:
    result: dict[str, list[Any]] = {name: [] for name in ("event", "error", "checkpoint", "typing", "receipt", "connection")}
    for name, values in result.items():
        subscription.on(name, values.append)
    return result


class Fixture:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.streams: dict[str, list[dict[str, Any]]] = {}
        self.clients: list[HumChat] = []
        self.commands: list[dict[str, Any]] = []
        self.sync_requests: list[dict[str, Any]] = []
        self.failures: list[BaseException] = []
        self.subscriptions: dict[int, dict[str, str]] = {}
        self.peer = LoopbackPeer(
            self._http, upgrade=self._upgrade, on_socket=self._on_socket, on_frame=self._on_frame, auto_pong=options.get("auto_pong", True)
        )

    @property
    def upgrades(self) -> list[Upgrade]:
        return self.peer.upgrades

    @property
    def origin(self) -> str:
        return self.peer.base_url

    def stream(self, identifier: str) -> list[dict[str, Any]]:
        return self.streams.setdefault(identifier, [])

    def ready(self, identifier: str, subscription_id: str) -> dict[str, Any]:
        rows = self.stream(identifier)
        return {"type": "ready", "schema": 1, "org_uuid": ORG, "channel_id": identifier, "subscription_id": subscription_id, "epoch": EPOCH,
                "cursor": cursor(identifier, len(rows)), "sequence": str(len(rows)), "earliest_cursor": cursor(identifier, 0)}  # fmt: skip

    async def _http(self, request: Any) -> Any:
        path = request.path
        if path == "/me":
            return 200, {"user_uuid": ACTOR, "org": ORG, "email": "multiplex@example.invalid", "roles": ["member"], "scopes": []}
        if path == "/api/sdk/capabilities":
            messaging: dict[str, Any] = {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2], "durable_replay": True,
                                         "event_protocol_versions": [1], "websocket_scope": "channel"}  # fmt: skip
            if self.options.get("supported", True) is not False:
                messaging["multiplexed_events"] = {"protocol_versions": [1], "max_channels": self.options.get("max_channels", 100)}
            return 200, {"sdk_protocol_version": 1, "authentication": {"user_sessions": True, "service_identities": False, "api_key_exchange": False,
                         "app_binding_required": False, "mock_authentication": True}, "messaging": messaging}  # fmt: skip
        if path == "/api/events/sync":
            identifier = request.query["channel_id"]
            rows = self.stream(identifier)
            after = request.query["after"]
            through = request.query.get("through") or cursor(identifier, len(rows))
            self.sync_requests.append({"channel_id": identifier, "after": after, "through": through})
            events = rows[position(after) : position(through)][: int(request.query.get("limit", 200))]
            last = events[-1]["cursor"] if events else after
            page = {"schema": 1, "org_uuid": ORG, "channel_id": identifier, "epoch": EPOCH, "events": events, "after_cursor": after,
                    "through_cursor": through, "through_sequence": str(position(through)), "next_cursor": None if last == through else last}  # fmt: skip
            hook = self.options.get("sync")
            custom = await hook(page, identifier) if hook is not None else None
            return 200, custom if custom is not None else page
        return 404, {}

    async def _upgrade(self, upgrade: Upgrade) -> Any:
        hook = self.options.get("upgrade")
        return hook(upgrade, self.upgrades) if hook is not None else None

    async def _on_socket(self, socket: PeerSocket) -> None:
        self.subscriptions[id(socket)] = {}
        await socket.send({"type": "stream_ready", "schema": 1, "org_uuid": ORG, "max_channels": self.options.get("max_channels", 100)})

    async def _on_frame(self, socket: PeerSocket, raw: dict[str, Any]) -> None:
        subscriptions = self.subscriptions[id(socket)]
        try:
            self.commands.append(raw)
            if raw["type"] == "subscribe":
                assert re.fullmatch(r"[\x21-\x7e]{1,128}", raw["subscription_id"])
                assert raw["channel_id"].startswith(ORG + ":")
                subscriptions[raw["subscription_id"]] = raw["channel_id"]
                if self.options.get("deny") == raw["channel_id"]:
                    await socket.send({"type": "channel_error", "schema": 1, "subscription_id": raw["subscription_id"], "channel_id": raw["channel_id"],
                                       "code": "FORBIDDEN", "status": 403, "retryable": False})  # fmt: skip
                    return
                if self.options.get("no_ready") == raw["channel_id"]:
                    return
                await socket.send(self.ready(raw["channel_id"], raw["subscription_id"]))
            elif raw["type"] == "unsubscribe":
                subscriptions.pop(raw["subscription_id"], None)
                await socket.send({"type": "unsubscribed", "schema": 1, "subscription_id": raw["subscription_id"]})
            else:
                assert subscriptions.get(raw["subscription_id"]) == raw["channel_id"]
                if raw["type"] == "typing":
                    await socket.send({"type": "typing", "org_uuid": ORG, "channel_id": raw["channel_id"], "subscription_id": raw["subscription_id"],
                                       "user_uuid": ACTOR, "ts": 1_792_370_346, "ttl_ms": 5000})  # fmt: skip
                elif raw["type"] == "read":
                    await socket.send({"type": "receipt", "org_uuid": ORG, "channel_id": raw["channel_id"], "subscription_id": raw["subscription_id"],
                                       "user_uuid": ACTOR, "ts": raw["ts"]})  # fmt: skip
                else:
                    raise AssertionError("Unexpected client command")
        except Exception as error:
            self.failures.append(error)
            await socket.terminate()

    def client(self, **extra: Any) -> HumChat:
        async def provider(_: Any) -> dict[str, str]:
            return {"token": "synthetic-multiplex-token"}

        realtime = {"transport": "multiplex", "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20, "handler_timeout_ms": 1500,
                    "ping_interval_ms": 1000, "pong_timeout_ms": 500, **extra.pop("realtime", {})}  # fmt: skip
        options: dict[str, Any] = {"org_id": ORG, "base_url": self.origin, "token_provider": provider, "timeout_ms": 500,
                                   "retry": {"max_retries": 0, "total_timeout_ms": 1500}, **extra, "realtime": realtime}  # fmt: skip
        client = HumChat(**options)
        self.clients.append(client)
        return client

    def scope(self, consumer_id: str, channel_id: str) -> PersistenceScope:
        return PersistenceScope(server_origin=self.origin, org_id=ORG, actor_id=ACTOR, consumer_id=consumer_id, channel_id=channel_id)

    def append(self, identifier: str, message_id: str) -> dict[str, Any]:
        rows = self.stream(identifier)
        sequence = str(len(rows) + 1)
        event = {"type": "event", "schema": 1, "org_uuid": ORG, "channel_id": identifier, "epoch": EPOCH, "sequence": sequence,
                 "event_id": EPOCH + ":" + sequence, "cursor": cursor(identifier, sequence), "actor_id": ACTOR, "kind": "message",
                 "state": {"schema": 1, "org_uuid": ORG, "channel_id": identifier, "message_id": message_id, "sender_id": ACTOR,
                           "created_at": 1_792_370_346, "thread_root_id": None, "priority": "standard", "original_body": message_id,
                           "body": message_id, "revision": 1, "updated_at": 1_792_370_346, "deleted": False, "reaction_revision": 0,
                           "reactions": {}, "attachments": []}}  # fmt: skip
        rows.append(event)
        return event

    async def hint(self, identifier: str) -> None:
        for socket in list(self.peer.sockets):
            if not socket.open:
                continue
            for key, channel_id in list(self.subscriptions.get(id(socket), {}).items()):
                if channel_id == identifier:
                    await socket.send({**self.ready(identifier, key), "type": "watermark"})

    async def send(self, raw: Any) -> None:
        for socket in list(self.peer.sockets):
            if socket.open:
                await socket.send(raw)

    async def drop(self) -> None:
        for socket in list(self.peer.sockets):
            await socket.terminate()


Peer = Callable[..., Awaitable[Fixture]]


@pytest.fixture
async def peer() -> AsyncIterator[Peer]:
    fixtures: list[Fixture] = []

    async def make(**options: Any) -> Fixture:
        fixture = Fixture(options)
        await fixture.peer.start()
        fixtures.append(fixture)
        return fixture

    yield make
    for fixture in fixtures:
        await asyncio.gather(*(client.close() for client in fixture.clients))
        await fixture.peer.close()
        assert fixture.failures == [], "Multiplex fixture errors"


def subscribes(fixture: Fixture) -> list[dict[str, Any]]:
    return [command for command in fixture.commands if command["type"] == "subscribe"]


async def test_multiplex_shares_one_header_authenticated_physical_socket_and_preserves_channel_refcount_isolation(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    a, duplicate, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    seen_a, seen_b = observe(a), observe(b)
    await asyncio.gather(a.ready, duplicate.ready, b.ready)
    assert len(fixture.upgrades) == 1
    assert len(subscribes(fixture)) == 2
    assert fixture.upgrades[0].path == "/ws/events"
    assert fixture.upgrades[0].query == {}
    assert fixture.upgrades[0].headers["authorization"] == "Bearer synthetic-multiplex-token"
    assert fixture.upgrades[0].headers["x-humchat-stream-protocol"] == "v1"
    assert client.realtime.get_stats().active_sockets == 1
    assert client.realtime.get_stats().subscriptions == 3
    fixture.append(FIRST, "a1")
    fixture.append(SECOND, "b1")
    await fixture.hint(FIRST)
    await fixture.hint(SECOND)
    await until(lambda: len(seen_a["event"]) == 1 and len(seen_b["event"]) == 1, "independent replay")
    assert seen_a["event"][0].channel_id == FIRST
    assert seen_b["event"][0].channel_id == SECOND
    await a.typing()
    await b.mark_read(through=123)
    await until(lambda: len(seen_a["typing"]) == 1 and len(seen_b["receipt"]) == 1, "scoped ephemeral signals")
    assert len(seen_a["receipt"]) == 0
    assert len(seen_b["typing"]) == 0
    await a.close()
    assert client.realtime.get_stats().active_sockets == 1
    assert sum(1 for value in fixture.commands if value["type"] == "unsubscribe") == 0
    await duplicate.close()
    await until(lambda: any(value["type"] == "unsubscribe" for value in fixture.commands), "last-handle unsubscribe")
    fixture.append(SECOND, "b2")
    await fixture.hint(SECOND)
    await until(lambda: len(seen_b["event"]) == 2, "sibling remains live")
    await b.close()
    await until(lambda: client.realtime.get_stats().active_sockets == 0, "idle stream closed")


async def test_multiplex_requires_advertised_support_and_does_not_fall_back_to_per_channel_sockets(peer: Peer) -> None:
    fixture = await peer(supported=False)
    client = fixture.client()
    subscription = client.realtime.subscribe(FIRST)
    await rejects(subscription.ready, UnsupportedFeatureError)
    assert subscription.status == "blocked"
    assert len(fixture.upgrades) == 0


async def test_channel_denial_and_ready_timeout_block_only_their_channel(peer: Peer) -> None:
    denied, slow_channel = channel("denied"), channel("timeout")
    fixture = await peer(deny=denied, no_ready=slow_channel)
    client = fixture.client()
    a, forbidden, slow = client.realtime.subscribe(FIRST), client.realtime.subscribe(denied), client.realtime.subscribe(slow_channel)
    await a.ready
    await rejects(forbidden.ready, PermissionError)
    await rejects(slow.ready)
    assert a.status == "live"
    assert forbidden.status == "blocked"
    assert slow.status == "blocked"
    assert client.realtime.get_stats().active_sockets == 1
    assert len(fixture.upgrades) == 1


async def test_replacement_subscription_ignores_stale_ready_watermark_ephemeral_frames_from_old_ids(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    keep, old = client.realtime.subscribe(SECOND), client.realtime.subscribe(FIRST)
    await asyncio.gather(keep.ready, old.ready)
    prior = next(command for command in subscribes(fixture) if command["channel_id"] == FIRST)["subscription_id"]
    await old.close()
    replacement = client.realtime.subscribe(FIRST)
    seen = observe(replacement)
    await replacement.ready
    current = [command for command in subscribes(fixture) if command["channel_id"] == FIRST][-1]["subscription_id"]
    assert prior != current
    await fixture.send({"type": "ready", "subscription_id": prior, "channel_id": SECOND})
    await fixture.send({"type": "watermark", "subscription_id": prior, "channel_id": SECOND})
    await fixture.send({"type": "typing", "subscription_id": prior, "channel_id": SECOND})
    fixture.append(FIRST, "fresh")
    await fixture.hint(FIRST)
    await until(lambda: len(seen["event"]) == 1, "replacement receives current event")
    assert len(seen["error"]) == 0
    assert len(seen["typing"]) == 0
    assert keep.status == "live"


async def test_shared_socket_loss_replays_each_checkpoint_once_and_preserves_start_now_after_reconnect(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    fixture.append(FIRST, "before-start")
    store = MemoryCheckpointStore()
    try:
        a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
        seen_a: list[str] = []
        seen_b: list[str] = []
        a.consume(lambda event, _: seen_a.append(event.state.id), consumer_id="a", checkpoint_store=store)
        b.consume(lambda event, _: seen_b.append(event.state.id), consumer_id="b", checkpoint_store=store)
        await asyncio.gather(a.ready, b.ready)
        await fixture.drop()
        fixture.append(FIRST, "after-loss-a")
        fixture.append(SECOND, "after-loss-b")
        await until(lambda: len(seen_a) == 1 and len(seen_b) == 1, "both channels catch up")
        assert seen_a == ["after-loss-a"]
        assert seen_b == ["after-loss-b"]
        assert len(fixture.upgrades) == 2
        assert client.realtime.get_stats().reconnect_count == 1
        # The handler runs before its checkpoint CAS; give the acknowledgement a few loop turns.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and await store.load(fixture.scope("b", SECOND)) != cursor(SECOND, 1):
            await sleep(5)
        assert await store.load(fixture.scope("a", FIRST)) == cursor(FIRST, 2)
        assert await store.load(fixture.scope("b", SECOND)) == cursor(SECOND, 1)
        assert seen_a == ["after-loss-a"]
        assert seen_b == ["after-loss-b"]
    finally:
        await store.close()


async def test_slow_replay_handlers_do_not_consume_socket_admission_slots_or_overlap_across_reconnect(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"connect_concurrency": 1})
    fixture.append(FIRST, "slow")
    fixture.append(SECOND, "fast")
    entered, gate, store = Deferred(), Deferred(), MemoryCheckpointStore()
    active = maximum = calls = 0

    async def slow(_: Any, __: Any) -> None:
        nonlocal active, maximum, calls
        active += 1
        maximum = max(maximum, active)
        calls += 1
        entered.resolve()
        if calls == 1:
            await gate
        active -= 1

    try:
        a = client.realtime.subscribe(FIRST, start="earliest")
        a.consume(slow, consumer_id="slow", checkpoint_store=store)
        b = client.realtime.subscribe(SECOND, start="earliest")
        seen = observe(b)
        await entered
        await b.ready
        assert len(seen["event"]) == 1
        assert a.status == "syncing"
        await fixture.drop()
        await until(lambda: len(fixture.upgrades) == 2 and b.status == "live", "other channel reconnects")
        assert calls == 1
        assert await store.load(fixture.scope("slow", FIRST)) == cursor(FIRST, 0)
        gate.resolve()
        await a.ready
        assert calls == 2
        assert maximum == 1
        assert await store.load(fixture.scope("slow", FIRST)) == cursor(FIRST, 1)
    finally:
        gate.resolve()
        await store.close()


async def test_shared_authentication_retries_one_401_exactly_once_for_all_channels(peer: Peer) -> None:
    fixture = await peer(upgrade=lambda _upgrade, upgrades: 401 if len(upgrades) == 1 else None)
    reasons: list[str] = []

    async def provider(context: Any) -> dict[str, str]:
        reasons.append(context.reason)
        return {"token": "synthetic-" + str(len(reasons))}

    client = fixture.client(token_provider=provider)
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    await asyncio.gather(a.ready, b.ready)
    assert reasons == ["initial", "unauthorized"]
    assert len(fixture.upgrades) == 2
    assert fixture.upgrades[1].headers["authorization"] == "Bearer synthetic-2"
    assert len(subscribes(fixture)) == 2, "Shared refresh must not cause redundant per-channel retries"
    assert client.realtime.get_stats().active_sockets == 1


@pytest.mark.parametrize("status", [403, 401])
async def test_permanent_403_and_repeated_401_preserve_their_original_error_and_never_fan_out_refreshes(peer: Peer, status: int) -> None:
    fixture = await peer(upgrade=lambda _upgrade, _upgrades: status)
    calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"token": "synthetic-" + str(calls)}

    client = fixture.client(token_provider=provider)
    subscriptions = [client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)]
    for sub in subscriptions:
        await rejects(sub.ready, lambda error: error.status == status)
    await sleep(60)
    assert calls == (2 if status == 401 else 1)
    assert len(fixture.upgrades) == (2 if status == 401 else 1)
    assert all(sub.status == "blocked" for sub in subscriptions)


async def test_one_session_revoked_terminates_every_channel_without_credential_refresh_or_reconnect(peer: Peer) -> None:
    fixture = await peer()
    calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"token": "synthetic-" + str(calls)}

    client = fixture.client(token_provider=provider)
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    observe(a)
    observe(b)
    await asyncio.gather(a.ready, b.ready)
    await fixture.send({"type": "session_revoked", "reason": "session no longer valid"})
    await until(lambda: a.status == "blocked" and b.status == "blocked", "global revocation")
    await sleep(60)
    assert calls == 1
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().active_sockets == 0
    with pytest.raises(SessionRevokedError):
        client.realtime.subscribe(channel("new"))


async def test_shared_expiry_rotates_one_stream_and_preserves_both_subscriptions(peer: Peer) -> None:
    fixture = await peer()
    calls = 0

    async def provider(_: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"token": "synthetic-" + str(calls), "expires_at": math.ceil(time.time()) + (2 if calls == 1 else 300)}

    client = fixture.client(token_provider=provider)
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    await asyncio.gather(a.ready, b.ready)
    await until(lambda: len(fixture.upgrades) == 2 and a.status == "live" and b.status == "live", "shared credential rotation", 6)
    assert calls == 2
    assert client.realtime.get_stats().reconnect_count == 1
    assert client.realtime.get_stats().active_sockets == 1


async def test_missed_final_watermark_is_recovered_by_one_shared_periodic_poll(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    seen_a, seen_b = observe(a), observe(b)
    await asyncio.gather(a.ready, b.ready)
    fixture.append(FIRST, "unhinted-a")
    fixture.append(SECOND, "unhinted-b")
    await until(lambda: len(seen_a["event"]) == 1 and len(seen_b["event"]) == 1, "periodic authoritative replay", 6.5)
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().reconnect_count == 0


@pytest.mark.parametrize("kind", ["forged", "stream_error"])
async def test_cross_channel_correlation_forgery_and_stream_error_terminate_the_stream(peer: Peer, kind: str) -> None:
    fixture = await peer()
    client = fixture.client()
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    seen = observe(a)
    await asyncio.gather(a.ready, b.ready)
    identifier = next(command for command in fixture.commands if command.get("channel_id") == FIRST)["subscription_id"]
    frame = (
        {"type": "watermark", "subscription_id": identifier, "channel_id": SECOND}
        if kind == "forged"
        else {"type": "stream_error", "schema": 1, "code": "INVALID_REQUEST"}
    )
    await fixture.send(frame)
    await until(lambda: a.status == "blocked" and b.status == "blocked", "terminal stream protocol error")
    await until(lambda: any(isinstance(error, ProtocolError) for error in seen["error"]), "protocol error notified")
    assert client.realtime.get_stats().active_sockets == 0
    await sleep(30)
    assert len(fixture.upgrades) == 1


async def test_100_explicit_channels_share_one_socket_and_server_limits_count_pending_admissions(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_channels": 100, "connect_concurrency": 7})
    handles = [client.realtime.subscribe(channel("c" + str(index))) for index in range(100)]
    await asyncio.gather(*(handle.ready for handle in handles))
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().active_sockets == 1
    assert client.realtime.get_stats().channels == 100
    with pytest.raises(Exception) as raised:
        client.realtime.subscribe(channel("overflow"))
    assert getattr(raised.value, "code", None) == "BACKPRESSURE"
    limited = await peer(max_channels=1)
    limited_client = limited.client()
    a, b = limited_client.realtime.subscribe(FIRST), limited_client.realtime.subscribe(SECOND)
    await a.ready
    await rejects(b.ready, "BACKPRESSURE")
    assert a.status == "live"
    assert len(limited.upgrades) == 1


async def test_large_durable_backlog_and_sibling_delivery_remain_ordered_within_shared_page_budgets(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_buffered_events": 400})
    for index in range(1, 602):
        fixture.append(FIRST, "message-" + str(index))
    fixture.append(SECOND, "sibling")
    a, b = client.realtime.subscribe(FIRST, start="earliest"), client.realtime.subscribe(SECOND, start="earliest")
    seen_a, seen_b = observe(a), observe(b)
    await asyncio.gather(a.ready, b.ready)
    await until(lambda: len(seen_a["event"]) == 601 and len(seen_b["event"]) == 1, "observer notifications drained")
    assert [int(event.sequence) for event in seen_a["event"]] == list(range(1, 602))
    assert len(seen_b["event"]) == 1
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().queued_events == 0
    assert client.realtime.get_stats().queued_bytes == 0


async def test_impossible_command_cannot_fit_fails_once_without_an_endless_shared_reconnect_loop(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_buffered_bytes": 16})
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    for handle in (a, b):
        await rejects(handle.ready, "BACKPRESSURE")
    await sleep(50)
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().active_sockets == 0


async def test_oversized_frame_cannot_fit_fails_once_without_an_endless_shared_reconnect_loop(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_payload_bytes": 4096})
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    seen = observe(a)
    await asyncio.gather(a.ready, b.ready)
    await fixture.send({"type": "oversized", "data": "x" * 5000})
    await until(lambda: a.status == "blocked" and b.status == "blocked", "bounded frame rejection")
    await until(lambda: any(error.code == "BACKPRESSURE" for error in seen["error"]), "backpressure notified")
    await sleep(50)
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().active_sockets == 0


async def test_one_heartbeat_reconnects_a_silent_multiplex_socket_without_multiplying_physical_attempts(peer: Peer) -> None:
    fixture = await peer(auto_pong=False)
    client = fixture.client(realtime={"ping_interval_ms": 30, "pong_timeout_ms": 20})
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    await asyncio.gather(a.ready, b.ready)
    await until(lambda: len(fixture.upgrades) >= 2, "shared heartbeat reconnect")
    await client.close()
    assert len(fixture.upgrades) == 2
    assert client.realtime.get_stats().reconnect_count == 1


async def test_shared_page_pressure_retries_only_the_affected_logical_channel_and_leaves_the_physical_stream_open(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_buffered_events": 1})
    a, b = client.realtime.subscribe(FIRST), client.realtime.subscribe(SECOND)
    entered, gate = Deferred(), Deferred()
    attempts = 0
    seen = observe(b)
    store = MemoryCheckpointStore()

    async def held(_: Any, __: Any) -> None:
        nonlocal attempts
        attempts += 1
        entered.resolve()
        await gate

    try:
        a.consume(held, consumer_id="slow", checkpoint_store=store)
        await asyncio.gather(a.ready, b.ready)
        fixture.append(FIRST, "held")
        await fixture.hint(FIRST)
        await entered
        fixture.append(SECOND, "waiting")
        await fixture.hint(SECOND)
        await until(lambda: any(error.code == "BACKPRESSURE" and error.retryable for error in seen["error"]), "shared replay budget pressure")
        assert len(fixture.upgrades) == 1
        assert client.realtime.get_stats().reconnect_count == 0
        gate.resolve()
        await until(lambda: len(seen["event"]) == 1, "logical retry after release")
        assert attempts == 1
        assert len(fixture.upgrades) == 1
        # The page budget is released after the observer returns; wait for that settlement.
        await until(lambda: client.realtime.get_stats().queued_events == 0, "shared budget released")
    finally:
        gate.resolve()
        await store.close()
