"""Port of test/integration/durable.test.mjs (durable event consumption against a loopback peer)."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

from helpers import ROOT, Deferred, LoopbackPeer, PeerSocket, rejects
from humchat import (
    FileCheckpointStore,
    FileOutboxStore,
    HumChat,
    MemoryCheckpointStore,
    NetworkError,
    PersistenceScope,
    RecoveryGapError,
    UnsupportedFeatureError,
)

ORG = "durable-org"
ACTOR = "durable-actor"
CHANNEL = ORG + ":team:general"
EPOCH = "durable-local-epoch"
TIMESTAMP = 1_792_370_346


def encode_cursor(sequence: int | str, epoch: str = EPOCH, channel_id: str = CHANNEL) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": epoch, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def position(cursor: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))["sequence"])


def state(identifier: str, **extra: Any) -> dict[str, Any]:
    return {"schema": 1, "org_uuid": ORG, "channel_id": CHANNEL, "message_id": identifier, "sender_id": ACTOR, "created_at": TIMESTAMP,
            "thread_root_id": None, "priority": "standard", "original_body": "Message " + identifier, "body": "Message " + identifier,
            "revision": 1, "updated_at": TIMESTAMP, "deleted": False, "reaction_revision": 0, "reactions": {}, "attachments": [], **extra}


async def until(predicate: Callable[[], Any], label: str, timeout: float = 4.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("Timed out: " + label)


def capture(subscription: Any) -> dict[str, list[Any]]:
    events: dict[str, list[Any]] = {"event": [], "error": [], "handler_error": [], "checkpoint": [], "recovery_complete": []}
    for name, values in events.items():
        subscription.on(name, values.append)
    return events


class Store:
    """A duck-typed checkpoint store built from callables (the Node object-literal stores)."""

    def __init__(self, load: Callable[..., Awaitable[Any]], compare_and_set: Callable[..., Awaitable[Any]]) -> None:
        self.load = load
        self.compare_and_set = compare_and_set

    async def close(self) -> None:
        return None


@pytest.fixture
async def after() -> Any:
    callbacks: list[Callable[[], Any]] = []
    yield callbacks.append
    for callback in reversed(callbacks):
        result = callback()
        if inspect.isawaitable(result):
            await result


async def file_checkpoint(after: Any) -> tuple[Path, FileCheckpointStore]:
    root = ROOT / "target" / "durable"
    root.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="durable-peer-", dir=root)) / "checkpoint.json"
    store = await FileCheckpointStore.open(path)
    after(store.close)
    return path, store


class DurablePeer:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.events: list[dict[str, Any]] = []
        self.streams: dict[str, list[dict[str, Any]]] = {CHANNEL: self.events}
        self.requests: list[dict[str, Any]] = []
        self.send_attempts: list[dict[str, Any]] = []
        self.sent: dict[str, dict[str, Any]] = {}
        self.clients: list[HumChat] = []
        self.floor = options.get("floor", 0)
        self.server = LoopbackPeer(self._http, upgrade=self._upgrade, on_socket=self._on_socket, auto_pong=options.get("auto_pong", True))
        self.origin = ""

    @property
    def upgrades(self) -> list[Any]:
        return self.server.upgrades

    @property
    def sockets(self) -> list[PeerSocket]:
        return self.server.sockets

    def stream(self, channel_id: str) -> list[dict[str, Any]]:
        return self.streams.setdefault(channel_id, [])

    async def _upgrade(self, upgrade: Any) -> Any:
        hook = self.options.get("upgrade")
        return hook(upgrade) if hook else None

    async def _on_socket(self, socket: PeerSocket) -> None:
        channel_id = socket.query["channel_id"]
        count = len(self.stream(channel_id))
        floor = self.options.get("ready_floor", self.floor) if channel_id == CHANNEL else 0
        await socket.send({"type": "ready", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH,
                           "cursor": encode_cursor(count, EPOCH, channel_id), "sequence": str(count),
                           "earliest_cursor": encode_cursor(floor, EPOCH, channel_id)})

    async def _http(self, request: web.Request) -> Any:
        if request.path == "/me":
            return 200, {"user_uuid": ACTOR, "org": ORG, "id": 1, "email": "durable@example.invalid", "roles": ["member"],
                         "scopes": [], "expires_at": None, "app_uuid": None}
        if request.path == "/api/sdk/capabilities":
            supported = self.options.get("durable_supported", True)
            return 200, {"sdk_protocol_version": 1,
                         "authentication": {"user_sessions": True, "service_identities": False, "api_key_exchange": False,
                                            "app_binding_required": False, "mock_authentication": True},
                         "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
                                       "durable_replay": supported, "event_protocol_versions": [1] if supported else [], "websocket_scope": "channel"}}
        if request.path == "/api/messages/send":
            body = json.loads(await request.read())
            key = request.headers["x-idempotency-key"]
            self.send_attempts.append({"key": key, "body": body})
            previous = self.sent.get(key)
            if previous and json.dumps(previous["body"]) != json.dumps(body):
                return 409, {}
            receipt = previous["receipt"] if previous else {"status": "ACK", "message_id": body["id"], "created_at": TIMESTAMP,
                                                            "attachment_count": 0, "attachment_manifest_blake3": "0" * 64}
            self.sent[key] = {"body": body, "receipt": receipt}
            if self.options.get("fail_first_send_after_commit") and len(self.send_attempts) == 1:
                assert request.transport is not None
                request.transport.abort()
                return web.Response(status=200)
            return 200, receipt
        if request.path == "/api/events/sync":
            query = request.query
            channel_id = query["channel_id"]
            channel_events = self.stream(channel_id)
            after = query["after"]
            through = query.get("through") or encode_cursor(len(channel_events), EPOCH, channel_id)
            self.requests.append({"after": after, "through": through, "channel": channel_id})
            assert channel_id.startswith(ORG + ":")
            channel_floor = self.floor if channel_id == CHANNEL else 0
            if position(after) < channel_floor:
                return 410, {"code": "EVENT_GAP", "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH,
                             "earliest_cursor": encode_cursor(channel_floor, EPOCH, channel_id), "earliest_sequence": str(channel_floor)}
            limit = min(self.options.get("page_size", 200), int(query.get("limit", 200)))
            rows = [event for event in channel_events if position(after) < int(event["sequence"]) <= position(through)][:limit]
            last = rows[-1]["cursor"] if rows else after
            page = {"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "events": rows, "after_cursor": after,
                    "through_cursor": through, "through_sequence": str(position(through)),
                    "next_cursor": None if position(last) == position(through) else last}
            hook = self.options.get("sync")
            custom = None
            if hook:
                custom = hook({"page": page, "requests": self.requests, "events": channel_events, "channel_id": channel_id})
                if inspect.isawaitable(custom):
                    custom = await custom
            return 200, custom if custom is not None else page
        return 404, {}

    def scope(self, consumer_id: str = "consumer", channel_id: str = CHANNEL) -> PersistenceScope:
        return PersistenceScope(server_origin=self.origin, org_id=ORG, actor_id=ACTOR, consumer_id=consumer_id, channel_id=channel_id)

    def client(self, **overrides: Any) -> HumChat:
        async def provider(_: Any) -> dict[str, str]:
            return {"token": "synthetic-durable-token"}

        options: dict[str, Any] = {
            "org_id": ORG, "base_url": self.origin, "token_provider": provider, "timeout_ms": 1000,
            "retry": {"max_retries": 0, "total_timeout_ms": 2000}, "recovery": {"mode": "durable"},
            "realtime": {"handler_timeout_ms": 1000, "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20,
                         "ping_interval_ms": 1000, "pong_timeout_ms": 500},
            **overrides,
        }
        client = HumChat(**options)
        self.clients.append(client)
        return client

    def append(self, identifier: str, **extra: Any) -> dict[str, Any]:
        return self.append_to(CHANNEL, identifier, **extra)

    def append_to(self, channel_id: str, identifier: str, **extra: Any) -> dict[str, Any]:
        channel_events = self.stream(channel_id)
        sequence = str(len(channel_events) + 1)
        event = {"type": "event", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": sequence,
                 "event_id": EPOCH + ":" + sequence, "cursor": encode_cursor(sequence, EPOCH, channel_id), "actor_id": ACTOR,
                 "kind": "message", "state": state(identifier, channel_id=channel_id), **extra}
        channel_events.append(event)
        return event

    async def publish(self, event: dict[str, Any]) -> None:
        for socket in list(self.sockets):
            if socket.open and (not event.get("channel_id") or socket.query.get("channel_id") == event["channel_id"]):
                try:
                    await socket.send(event)
                except Exception:  # A closing socket is not a fixture error.
                    pass

    def prune(self, sequence: int) -> None:
        self.floor = sequence

    async def close(self) -> None:
        await asyncio.gather(*(client.close() for client in self.clients))
        await self.server.close()


async def peer(after: Any, **options: Any) -> DurablePeer:
    fixture = DurablePeer(options)
    await fixture.server.start()
    fixture.origin = fixture.server.base_url
    after(fixture.close)
    return fixture


async def memory(after: Any) -> MemoryCheckpointStore:
    store = MemoryCheckpointStore()
    after(store.close)
    return store


async def test_durable_consumption_checkpoints_only_after_an_awaited_handler_then_resumes_across_a_file_store_restart(after: Any) -> None:
    fixture = await peer(after, page_size=1)
    first = fixture.append("root")
    second = fixture.append("thread-reply", state=state("thread-reply", thread_root_id="root"))
    path, store = await file_checkpoint(after)
    gate, started = Deferred(), Deferred()
    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    observed = capture(subscription)
    handled: list[str] = []

    async def handler(event: Any, _: Any) -> None:
        handled.append(event.event_id)
        if event.event_id == first["event_id"]:
            started.resolve()
            await gate

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await started
    assert await store.load(fixture.scope()) == encode_cursor(0)
    gate.resolve()
    await subscription.ready
    await until(lambda: _equals(store.load(fixture.scope()), second["cursor"]), "second durable acknowledgement")
    assert handled == [first["event_id"], second["event_id"]]
    assert len(fixture.requests) >= 2
    assert any(event.complete is True for event in observed["recovery_complete"])
    assert fixture.upgrades[0].headers["x-humchat-event-protocol"] == "v1"
    assert fixture.upgrades[0].headers["authorization"] == "Bearer synthetic-durable-token"
    assert "token" not in fixture.upgrades[0].query
    await client.close()
    await store.close()
    reopened = await FileCheckpointStore.open(path)
    after(reopened.close)
    third = fixture.append("offline-message")
    following = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    replayed: list[str] = []
    following.consume(lambda event, _: replayed.append(event.event_id), consumer_id="consumer", checkpoint_store=reopened)
    await following.ready
    await until(lambda: _equals(reopened.load(fixture.scope()), third["cursor"]), "restart acknowledgement")
    assert replayed == [third["event_id"]]


async def _equals(value: Awaitable[Any], expected: Any) -> bool:
    return await value == expected


async def test_failed_handlers_leave_the_checkpoint_unadvanced_and_retry_the_event_after_restart(after: Any) -> None:
    fixture = await peer(after)
    event = fixture.append("failure")
    checkpoints = await memory(after)
    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    observed = capture(subscription)

    def fail(_: Any, __: Any) -> None:
        raise RuntimeError("Synthetic application failure")

    subscription.consume(fail, consumer_id="consumer", checkpoint_store=checkpoints)
    await rejects(subscription.ready)
    await until(lambda: subscription.status == "blocked", "failed consumer blocked")
    assert await checkpoints.load(fixture.scope()) == encode_cursor(0)
    await until(lambda: len(observed["handler_error"]) >= 1, "handler error observed")
    await client.close()
    received: list[str] = []
    restarted = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    restarted.consume(lambda value, _: received.append(value.event_id), consumer_id="consumer", checkpoint_store=checkpoints)
    await restarted.ready
    assert received == [event["event_id"]]
    assert await checkpoints.load(fixture.scope()) == event["cursor"]


async def test_a_checkpoint_cas_conflict_blocks_progress_instead_of_acknowledging_competing_work(after: Any) -> None:
    fixture = await peer(after)
    fixture.append("cas-conflict")
    counts = {"handled": 0, "attempts": 0}

    async def load(_: Any) -> str:
        return encode_cursor(0)

    async def compare_and_set(*_: Any) -> bool:
        counts["attempts"] += 1
        return False

    subscription = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    subscription.consume(lambda *_: counts.__setitem__("handled", counts["handled"] + 1), consumer_id="consumer",
                         checkpoint_store=Store(load, compare_and_set))
    await rejects(subscription.ready)
    assert subscription.status == "blocked"
    assert counts == {"handled": 1, "attempts": 1}


async def test_retention_gaps_expose_recovery_failure_and_never_overwrite_the_stored_checkpoint(after: Any) -> None:
    # Retention advances after the advertised ready floor, so the HTTP 410 path is exercised.
    fixture = await peer(after, ready_floor=0)
    fixture.append("pruned")
    fixture.append("retained")
    fixture.prune(1)
    store = await memory(after)
    await store.compare_and_set(fixture.scope(), None, encode_cursor(0))
    subscription = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    observed = capture(subscription)
    handled: list[Any] = []
    subscription.consume(lambda event, _: handled.append(event), consumer_id="consumer", checkpoint_store=store)
    await rejects(subscription.ready, lambda error: isinstance(error, RecoveryGapError) and error.status == 410)
    assert subscription.status == "blocked"
    assert handled == []
    assert await store.load(fixture.scope()) == encode_cursor(0)
    assert observed["event"] == []


async def test_captured_replay_watermark_and_overlapping_live_frames_deliver_contiguous_events_once(after: Any) -> None:
    holder: dict[str, Any] = {"injected": False}

    async def sync(context: dict[str, Any]) -> Any:
        if not holder["injected"]:
            holder["injected"] = True
            fixture = holder["fixture"]
            following = fixture.append("racing-live")
            await fixture.publish(fixture.events[0])  # replay overlap must not duplicate delivery
            await fixture.publish(following)
        return context["page"]

    fixture = await peer(after, sync=sync)
    holder["fixture"] = fixture
    historical = fixture.append("historical")
    store = await memory(after)
    subscription = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    handled: list[str] = []
    subscription.consume(lambda event, _: handled.append(event.event_id), consumer_id="consumer", checkpoint_store=store)
    await subscription.ready
    await until(lambda: len(handled) == 2, "overlapping live event acknowledged")
    assert handled == [historical["event_id"], EPOCH + ":2"]
    await until(lambda: _equals(store.load(fixture.scope()), encode_cursor(2)), "second checkpoint")


async def test_a_skipped_sequence_in_a_server_page_fails_closed_before_calling_the_consumer(after: Any) -> None:
    fixture = await peer(after, sync=lambda context: {**context["page"], "events": context["page"]["events"][1:]})
    fixture.append("one")
    fixture.append("two")
    store = await memory(after)
    handled: list[Any] = []
    subscription = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    subscription.consume(lambda event, _: handled.append(event), consumer_id="consumer", checkpoint_store=store)
    await rejects(subscription.ready, "PROTOCOL")
    assert handled == []
    assert await store.load(fixture.scope()) == encode_cursor(0)


async def test_stored_checkpoints_bound_to_another_channel_cannot_become_replay_positions(after: Any) -> None:
    fixture = await peer(after)
    fixture.append("one")
    store = await memory(after)
    await store.compare_and_set(fixture.scope(), None, encode_cursor(0, EPOCH, ORG + ":team:other"))
    subscription = fixture.client().realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    subscription.consume(lambda *_: None, consumer_id="consumer", checkpoint_store=store)
    await rejects(subscription.ready, "PROTOCOL")
    assert fixture.requests == []


async def test_reconnect_reloads_the_acknowledged_position_and_catches_up_only_unseen_committed_events(after: Any) -> None:
    fixture = await peer(after)
    first = fixture.append("initial")
    store = await memory(after)
    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    handled: list[str] = []
    subscription.consume(lambda event, _: handled.append(event.event_id), consumer_id="consumer", checkpoint_store=store)
    await subscription.ready
    second = fixture.append("created-while-disconnected")
    await fixture.sockets[-1].terminate()
    await until(lambda: _equals(store.load(fixture.scope()), second["cursor"]), "reconnect catch-up")
    assert handled == [first["event_id"], second["event_id"]]
    assert client.realtime.get_stats().reconnect_count >= 1
    assert any(request["after"] == first["cursor"] for request in fixture.requests)


async def test_a_replaced_stream_epoch_requires_explicit_recovery_without_rewriting_its_old_checkpoint(after: Any) -> None:
    fixture = await peer(after)
    fixture.append("new-epoch")
    store = await memory(after)
    previous = encode_cursor(1, "previous-stream-epoch")
    await store.compare_and_set(fixture.scope(), None, previous)
    subscription = fixture.client().realtime.subscribe(CHANNEL)
    capture(subscription)
    subscription.consume(lambda *_: None, consumer_id="consumer", checkpoint_store=store)
    await rejects(subscription.ready, RecoveryGapError)
    assert await store.load(fixture.scope()) == previous
    assert fixture.requests == []


async def test_an_older_server_cannot_silently_downgrade_the_default_durable_subscription(after: Any) -> None:
    fixture = await peer(after, durable_supported=False)
    subscription = fixture.client().realtime.subscribe(CHANNEL)
    capture(subscription)
    await rejects(subscription.ready, UnsupportedFeatureError)
    assert subscription.status == "blocked"
    assert fixture.upgrades == []


async def test_persisted_outbox_retries_an_ambiguous_committed_send_with_the_same_message_and_idempotency_ids(after: Any) -> None:
    fixture = await peer(after, fail_first_send_after_commit=True)
    root = ROOT / "target" / "durable"
    root.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="outbox-peer-", dir=root)) / "outbox.json"
    first_store = await FileOutboxStore.open(path)
    after(first_store.close)
    first_client = fixture.client()
    await rejects(first_client.outbox.send(consumer_id="consumer", key="reply-to-event-1", store=first_store,
                                           message={"channel_id": CHANNEL, "content": "Persist before sending"}), NetworkError)
    pending = await first_store.list_pending(fixture.scope())
    assert len(pending) == 1
    await first_client.close()
    await first_store.close()
    restarted = await FileOutboxStore.open(path)
    after(restarted.close)
    second_client = fixture.client()
    receipts = await second_client.outbox.flush(consumer_id="consumer", channel_id=CHANNEL, store=restarted)
    assert len(receipts) == 1
    assert receipts[0].id == pending[0].intent.id
    assert receipts[0].idempotency_key == pending[0].idempotency_key
    assert len(fixture.send_attempts) == 2
    assert fixture.send_attempts[1] == fixture.send_attempts[0]
    assert len(fixture.sent) == 1
    assert await restarted.list_pending(fixture.scope()) == []


async def test_resync_requested_inside_an_active_consumer_survives_until_that_delivery_finishes(after: Any) -> None:
    fixture = await peer(after)
    store = await memory(after)
    subscription = fixture.client().realtime.subscribe(CHANNEL)
    capture(subscription)
    handled: list[str] = []

    async def handler(event: Any, _: Any) -> None:
        handled.append(event.event_id)
        if event.sequence == "1":
            await subscription.resync()
            fixture.append("committed-without-live-hint")

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await subscription.ready
    await fixture.publish(fixture.append("resync-trigger"))
    # This deadline is shorter than the worker's five-second fallback poll.
    await until(lambda: _equals(store.load(fixture.scope()), encode_cursor(2)), "resync queued during delivery", 3.5)
    assert handled == [EPOCH + ":1", EPOCH + ":2"]


async def test_session_revocation_blocks_the_whole_client_without_refreshing_or_reconnecting(after: Any) -> None:
    fixture = await peer(after)
    calls = {"provider": 0}

    async def provider(_: Any) -> dict[str, str]:
        calls["provider"] += 1
        return {"token": "synthetic-revoked-session"}

    client = fixture.client(token_provider=provider)
    subscription = client.realtime.subscribe(CHANNEL)
    capture(subscription)
    await subscription.ready
    await fixture.publish({"type": "session_revoked", "reason": "fixture session ended"})
    await until(lambda: subscription.status == "blocked", "revoked channel blocked")
    await rejects(client.connect(), "SESSION_REVOKED")
    with pytest.raises(Exception) as raised:
        client.realtime.subscribe(ORG + ":team:other")
    assert getattr(raised.value, "code", None) == "SESSION_REVOKED"
    await asyncio.sleep(0.03)
    assert calls["provider"] == 1
    assert len(fixture.upgrades) == 1
    assert client.realtime.get_stats().reconnect_count == 0


async def test_durable_heartbeat_reconnects_a_socket_that_stops_answering_ping_frames(after: Any) -> None:
    fixture = await peer(after, auto_pong=False)
    # Node uses 15/10 ms. Windows event loops before Python 3.13 have ~15.6 ms timer
    # granularity, so such short liveness windows can expire before the first catch-up
    # finishes and `ready` never resolves; 200/100 ms keeps the same behaviour observable.
    client = fixture.client(realtime={"ping_interval_ms": 200, "pong_timeout_ms": 100, "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 10})
    subscription = client.realtime.subscribe(CHANNEL)
    capture(subscription)
    await subscription.ready
    await until(lambda: len(fixture.upgrades) >= 2, "durable heartbeat reconnect")
    assert client.realtime.get_stats().reconnect_count >= 1
    await client.close()


async def test_expiring_credentials_rotate_the_durable_handshake_through_the_shared_token_provider(after: Any) -> None:
    fixture = await peer(after)
    calls = {"provider": 0}

    async def provider(_: Any) -> dict[str, Any]:
        calls["provider"] += 1
        return {"token": f"synthetic-rotated-{calls['provider']}", "expires_at": int(time.time()) + (2 if calls["provider"] == 1 else 300)}

    client = fixture.client(token_provider=provider)
    subscription = client.realtime.subscribe(CHANNEL)
    capture(subscription)
    await subscription.ready
    await until(lambda: any(upgrade.headers.get("authorization") == "Bearer synthetic-rotated-2" for upgrade in fixture.upgrades),
                "rotated durable credential", 4.0)
    assert calls["provider"] == 2
    assert client.realtime.get_stats().reconnect_count >= 1


async def test_typed_event_page_reads_reject_forged_boundary_cursors_and_cross_tenant_event_scope(after: Any) -> None:
    cases: list[tuple[str, Callable[[dict[str, Any]], dict[str, Any]]]] = [
        ("forged boundary", lambda page: {**page, "through_cursor": encode_cursor(1, "forged-epoch")}),
        ("cross-tenant event", lambda page: {**page, "events": [{**event, "org_uuid": "another-org"} for event in page["events"]]}),
        ("event identity mismatch", lambda page: {**page, "events": [{**event, "event_id": EPOCH + ":2"} for event in page["events"]]}),
    ]
    for _name, transform in cases:
        fixture = await peer(after, sync=lambda context, transform=transform: transform(context["page"]))
        fixture.append("one")
        await rejects(fixture.client().events.sync(channel_id=CHANNEL, after=encode_cursor(0), through=encode_cursor(1)), "PROTOCOL")


async def test_cold_event_sync_shares_one_total_deadline_across_authentication_and_the_response(after: Any) -> None:
    async def slow_sync(context: dict[str, Any]) -> Any:
        await asyncio.sleep(0.25)
        return context["page"]

    fixture = await peer(after, sync=slow_sync)
    fixture.append("deadline-event")
    checkpoints = await memory(after)
    await checkpoints.compare_and_set(fixture.scope(), None, encode_cursor(0))
    delivered = {"count": 0}

    async def provider(_: Any) -> dict[str, str]:
        await asyncio.sleep(0.25)
        return {"token": "synthetic-slow-authentication"}

    client = fixture.client(timeout_ms=1000, retry={"max_retries": 0, "total_timeout_ms": 400}, token_provider=provider)
    started = time.perf_counter()

    async def run() -> None:
        page = await client.events.sync(channel_id=CHANNEL, after=encode_cursor(0), through=encode_cursor(1))
        delivered["count"] += len(page.events)
        await checkpoints.compare_and_set(fixture.scope(), encode_cursor(0), page.events[-1].cursor)

    await rejects(run(), "TIMEOUT")
    elapsed = time.perf_counter() - started
    assert 0.35 <= elapsed < 0.65, "Authentication and response must consume one 400ms operation budget"
    assert delivered["count"] == 0
    assert await checkpoints.load(fixture.scope()) == encode_cursor(0)
    assert len(fixture.requests) == 1, "The deadline must expire during the response, after authentication succeeded"


async def test_credential_rotation_remains_active_during_a_slow_checkpoint_load_without_overlapping_store_operations(after: Any) -> None:
    fixture = await peer(after)
    event = fixture.append("after-slow-load")
    backing = await memory(after)
    gate = Deferred()
    after(gate.resolve)
    counts = {"provider": 0, "load": 0, "active": 0, "peak": 0}

    async def operation(callback: Callable[[], Awaitable[Any]]) -> Any:
        counts["active"] += 1
        counts["peak"] = max(counts["peak"], counts["active"])
        try:
            return await callback()
        finally:
            counts["active"] -= 1

    async def load(scope: Any) -> Any:
        async def run() -> Any:
            counts["load"] += 1
            if counts["load"] == 1:
                await gate
            return await backing.load(scope)

        return await operation(run)

    async def compare_and_set(scope: Any, previous: Any, following: Any) -> Any:
        return await operation(lambda: backing.compare_and_set(scope, previous, following))

    async def provider(_: Any) -> dict[str, Any]:
        counts["provider"] += 1
        return {"token": f"synthetic-load-rotation-{counts['provider']}", "expires_at": int(time.time()) + (2 if counts["provider"] == 1 else 300)}

    client = fixture.client(token_provider=provider)
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    handled: list[str] = []
    subscription.consume(lambda value, _: handled.append(value.event_id), consumer_id="consumer", checkpoint_store=Store(load, compare_and_set))
    try:
        await until(lambda: counts["load"] == 1, "first checkpoint load started")
        await until(lambda: counts["provider"] == 2, "credentials rotated while storage is pending")
        assert counts["load"] == 1, "A new connection generation must wait for the existing store operation"
        assert counts["peak"] == 1
    finally:
        gate.resolve()
    await subscription.ready
    assert counts["load"] == 2
    assert counts["peak"] == 1
    assert handled == [event["event_id"]]
    assert await backing.load(fixture.scope()) == event["cursor"]


async def test_reconnect_waits_for_an_interrupted_consumer_to_settle_before_replaying_the_unacknowledged_event(after: Any) -> None:
    fixture = await peer(after)
    event = fixture.append("interrupted-handler")
    store = await memory(after)
    gate, entered = Deferred(), Deferred()
    after(gate.resolve)
    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    counts = {"calls": 0, "active": 0, "peak": 0}
    seen: dict[str, Any] = {}

    async def handler(_: Any, context: Any) -> None:
        counts["calls"] += 1
        counts["active"] += 1
        counts["peak"] = max(counts["peak"], counts["active"])
        try:
            if counts["calls"] == 1:
                seen["signal"] = context.signal
                entered.resolve()
                await gate
        finally:
            counts["active"] -= 1

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await entered
    await fixture.sockets[-1].terminate()
    await until(lambda: seen["signal"].aborted, "interrupted handler cancellation")
    await asyncio.sleep(0.06)
    assert counts["calls"] == 1, "A cancelled application coroutine can still run; reconnect must not overlap it"
    assert await store.load(fixture.scope()) == encode_cursor(0)
    gate.resolve()
    await subscription.ready
    assert counts["calls"] == 2, "The cancelled attempt is replayed after settling"
    assert counts["peak"] == 1
    assert await store.load(fixture.scope()) == event["cursor"]


async def test_reconnect_waits_for_an_in_flight_checkpoint_write_and_reloads_its_committed_result(after: Any) -> None:
    fixture = await peer(after)
    event = fixture.append("checkpoint-race")
    backing = await memory(after)
    await backing.compare_and_set(fixture.scope(), None, encode_cursor(0))
    gate, writing = Deferred(), Deferred()
    after(gate.resolve)
    counts = {"loads": 0, "writes": 0, "active": 0, "peak": 0}

    async def operation(callback: Callable[[], Awaitable[Any]]) -> Any:
        counts["active"] += 1
        counts["peak"] = max(counts["peak"], counts["active"])
        try:
            return await callback()
        finally:
            counts["active"] -= 1

    async def load(scope: Any) -> Any:
        counts["loads"] += 1
        return await operation(lambda: backing.load(scope))

    async def compare_and_set(scope: Any, previous: Any, following: Any) -> Any:
        async def run() -> Any:
            counts["writes"] += 1
            writing.resolve()
            await gate
            return await backing.compare_and_set(scope, previous, following)

        return await operation(run)

    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)
    handled: list[str] = []
    subscription.consume(lambda value, _: handled.append(value.event_id), consumer_id="consumer", checkpoint_store=Store(load, compare_and_set))
    await writing
    await fixture.sockets[-1].terminate()
    await until(lambda: client.realtime.get_stats().reconnect_count > 0, "checkpoint-write reconnect")
    await asyncio.sleep(0.03)
    assert counts["loads"] == 1
    assert await backing.load(fixture.scope()) == encode_cursor(0)
    gate.resolve()
    await subscription.ready
    assert counts["peak"] == 1
    assert counts["loads"] == 2
    assert counts["writes"] == 1
    assert handled == [event["event_id"]]
    assert await backing.load(fixture.scope()) == event["cursor"]


async def test_an_interrupted_consumer_that_ignores_cancellation_blocks_at_its_original_deadline_without_overlapping_replay(after: Any) -> None:
    fixture = await peer(after)
    fixture.append("non-cooperative-handler")
    store = await memory(after)
    gate, entered = Deferred(), Deferred()
    after(gate.resolve)
    client = fixture.client(realtime={"handler_timeout_ms": 120, "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 10})
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    observed = capture(subscription)
    calls = {"count": 0}

    async def handler(*_: Any) -> None:
        calls["count"] += 1
        entered.resolve()
        await gate

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await entered
    await fixture.sockets[-1].terminate()
    await rejects(subscription.ready, "PROTOCOL")
    assert subscription.status == "blocked"
    assert calls["count"] == 1
    await until(lambda: any(event.event == "consume" for event in observed["handler_error"]), "consume handler error")
    assert await store.load(fixture.scope()) == encode_cursor(0)
    gate.resolve()
    await asyncio.sleep(0.025)
    assert calls["count"] == 1
    assert await store.load(fixture.scope()) == encode_cursor(0)
    assert client.realtime.get_stats().queued_events == 0


async def test_closing_during_a_consumer_aborts_delivery_without_acknowledging_its_later_completion(after: Any) -> None:
    fixture = await peer(after)
    fixture.append("close-during-handler")
    store = await memory(after)
    gate, started = Deferred(), Deferred()
    after(gate.resolve)
    seen: dict[str, Any] = {}
    client = fixture.client()
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(subscription)

    async def handler(_: Any, context: Any) -> None:
        seen["signal"] = context.signal
        started.resolve()
        await gate

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await started
    await client.close()
    assert seen["signal"].aborted is True
    await rejects(subscription.ready, "ABORT")
    gate.resolve()
    await asyncio.sleep(0.025)
    assert await store.load(fixture.scope()) == encode_cursor(0)
    assert len(fixture.upgrades) == 1


async def test_durable_replay_crosses_500_events_without_applying_best_effort_page_thread_or_time_limits(after: Any) -> None:
    fixture = await peer(after)
    for index in range(701):
        fixture.append(f"large-replay-{index}", state=state(f"large-replay-{index}", thread_root_id="large-replay-0" if index else None))
    store = await memory(after)
    client = fixture.client(recovery={"mode": "durable", "max_pages": 1, "max_threads": 0, "max_state_refresh": 0, "timeout_ms": 1},
                            realtime={"max_buffered_events": 50, "max_buffered_bytes": 128 * 1024})
    subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    observed = capture(subscription)
    handled: list[str] = []

    def handler(event: Any, _: Any) -> None:
        handled.append(event.sequence)
        assert client.realtime.get_stats().queued_events <= 50

    subscription.consume(handler, consumer_id="consumer", checkpoint_store=store)
    await asyncio.wait_for(subscription.ready, 20)
    assert handled == [str(index + 1) for index in range(701)]
    assert await store.load(fixture.scope()) == encode_cursor(701)
    assert len(fixture.requests) == 15
    assert all(request["through"] == encode_cursor(701) for request in fixture.requests)
    await until(lambda: any(event.complete and event.states == 701 and event.pages == 15 for event in observed["recovery_complete"]), "complete")
    assert client.realtime.get_stats().queued_events == 0
    assert client.realtime.get_stats().queued_bytes == 0


async def test_shared_channel_delivery_pressure_retries_from_its_own_checkpoint_and_releases_the_global_budget(after: Any) -> None:
    fixture = await peer(after)
    other_channel = ORG + ":team:other"
    first = fixture.append("held-page")
    second = fixture.append_to(other_channel, "waiting-page")
    store = await memory(after)
    gate, entered = Deferred(), Deferred()
    after(gate.resolve)
    calls = {"provider": 0}

    async def provider(_: Any) -> dict[str, str]:
        calls["provider"] += 1
        return {"token": "synthetic-shared-session"}

    client = fixture.client(token_provider=provider, realtime={"connect_concurrency": 2, "max_buffered_events": 1, "max_buffered_bytes": 16 * 1024,
                                                               "handler_timeout_ms": 2000, "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20})
    first_subscription = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(first_subscription)

    async def hold(*_: Any) -> None:
        entered.resolve()
        await gate

    first_subscription.consume(hold, consumer_id="consumer", checkpoint_store=store)
    await entered
    second_subscription = client.realtime.subscribe(other_channel, start="earliest")
    observed = capture(second_subscription)
    handled: list[str] = []
    second_subscription.consume(lambda event, _: handled.append(event.event_id), consumer_id="consumer", checkpoint_store=store)
    await until(lambda: any(error.code == "BACKPRESSURE" and error.retryable for error in observed["error"]), "shared queue backoff")
    assert handled == []
    assert await store.load(fixture.scope("consumer", other_channel)) == encode_cursor(0, EPOCH, other_channel)
    assert client.realtime.get_stats().queued_events == 1
    gate.resolve()
    await asyncio.gather(first_subscription.ready, second_subscription.ready)
    assert await store.load(fixture.scope()) == first["cursor"]
    assert await store.load(fixture.scope("consumer", other_channel)) == second["cursor"]
    assert handled == [second["event_id"]]
    assert calls["provider"] == 1
    assert client.realtime.get_stats().queued_events == 0
    assert client.realtime.get_stats().queued_bytes == 0
    assert client.realtime.get_stats().reconnect_count >= 1


async def test_a_slow_durable_consumer_releases_handshake_admission_so_another_channel_can_become_live(after: Any) -> None:
    fixture = await peer(after)
    other_channel = ORG + ":team:independent"
    first = fixture.append("slow-consumer")
    second = fixture.append_to(other_channel, "independent-consumer")
    store = await memory(after)
    gate, entered = Deferred(), Deferred()
    after(gate.resolve)
    client = fixture.client(realtime={"connect_concurrency": 1, "handler_timeout_ms": 3000})
    slow = client.realtime.subscribe(CHANNEL, start="earliest")
    capture(slow)

    async def hold(*_: Any) -> None:
        entered.resolve()
        await gate

    slow.consume(hold, consumer_id="consumer", checkpoint_store=store)
    await entered
    independent = client.realtime.subscribe(other_channel, start="earliest")
    capture(independent)
    handled: list[str] = []
    independent.consume(lambda event, _: handled.append(event.event_id), consumer_id="consumer", checkpoint_store=store)
    await until(lambda: independent.status == "live", "another channel admitted before the slow handler finishes", 1.5)
    assert handled == [second["event_id"]]
    assert await store.load(fixture.scope("consumer", other_channel)) == second["cursor"]
    assert await store.load(fixture.scope()) == encode_cursor(0)
    gate.resolve()
    await asyncio.gather(slow.ready, independent.ready)
    assert await store.load(fixture.scope()) == first["cursor"]


async def test_a_reconnect_denied_after_access_revocation_preserves_http_403_despite_trailing_socket_errors(after: Any) -> None:
    flags = {"denied": False}
    fixture = await peer(after, upgrade=lambda _: 403 if flags["denied"] else None)
    calls = {"provider": 0}

    async def provider(_: Any) -> dict[str, str]:
        calls["provider"] += 1
        return {"token": "synthetic-revoked-grant"}

    client = fixture.client(token_provider=provider)
    subscription = client.realtime.subscribe(CHANNEL)
    observed = capture(subscription)
    await subscription.ready
    flags["denied"] = True
    await fixture.sockets[-1].terminate()
    await until(lambda: subscription.status == "blocked", "permanent permission failure stops reconnect", 1.5)
    await until(lambda: any(error.code == "PERMISSION" and error.status == 403 for error in observed["error"]), "403 observed")
    attempts = len(fixture.upgrades)
    await asyncio.sleep(0.05)
    assert len(fixture.upgrades) == attempts
    assert attempts == 2
    assert calls["provider"] == 1
    assert client.realtime.get_stats().active_sockets == 0
