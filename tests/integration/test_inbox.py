"""Port of test/integration/inbox.test.mjs (conversation inbox watcher against a loopback peer)."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import os
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

from helpers import ROOT, LoopbackPeer, PeerSocket, rejects
from humchat import AbortSignal, HumChat, MemoryCheckpointStore, PersistenceScope

ORG = "inbox-org"
ACTOR = "inbox-self"
PEER_ID = "inbox-peer"
EPOCH = "inbox-epoch"


def channel(identifier: str) -> str:
    return f"{ORG}:conv:{identifier}"


def cursor(identifier: str, sequence: int) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": channel(identifier), "epoch": EPOCH, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def sequence(value: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))["sequence"])


def conversation(identifier: str) -> dict[str, Any]:
    return {"id": identifier, "channel_key": channel(identifier), "name": None, "kind": "dm", "member_count": 2, "my_role": "member",
            "members": [{"user_uuid": ACTOR, "role": "member"}, {"user_uuid": PEER_ID, "role": "member"}], "last_message_at": None}


async def until(predicate: Callable[[], Any], message: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out: " + message)


class Deferred:
    def __init__(self) -> None:
        self.future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.future.add_done_callback(lambda done: done.cancelled() or done.exception())

    def resolve(self, value: Any = None) -> None:
        if not self.future.done():
            self.future.set_result(value)

    def reject(self, error: BaseException) -> None:
        if not self.future.done():
            self.future.set_exception(error)

    def __await__(self) -> Any:
        return asyncio.shield(self.future).__await__()


async def aborted(signal: AbortSignal) -> None:
    """``new Promise(resolve => signal.addEventListener('abort', resolve, {once: true}))``."""
    if signal.aborted:
        return
    waiter = asyncio.get_running_loop().create_future()
    signal.add_listener(lambda _: waiter.done() or waiter.set_result(None))
    await waiter


def settled_flag(future: Any) -> dict[str, bool]:
    flag = {"done": False}
    future.add_done_callback(lambda _: flag.__setitem__("done", True))
    return flag


class Store:
    def __init__(self, load: Callable[..., Any], compare_and_set: Callable[..., Any]) -> None:
        self.load = load
        self.compare_and_set = compare_and_set


@pytest.fixture
async def after() -> Any:
    callbacks: list[Callable[[], Any]] = []
    yield callbacks.append
    for callback in reversed(callbacks):
        result = callback()
        if inspect.isawaitable(result):
            await result


class InboxPeer:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.streams: dict[str, list[dict[str, Any]]] = {}
        self.clients: list[HumChat] = []
        self.requests: list[str] = []
        self.sends: list[dict[str, Any]] = []
        self.discovery_status = 200
        self.discovery_gate: asyncio.Future[Any] | None = None
        self.durable = True
        self.denied = False
        self.server = LoopbackPeer(self._http, on_socket=self._on_socket)
        self.origin = ""

    @property
    def upgrades(self) -> list[Any]:
        return self.server.upgrades

    @property
    def sockets(self) -> list[PeerSocket]:
        return self.server.sockets

    def all_closed(self, identifier: str | None = None) -> bool:
        return all(not socket.open for socket in self.sockets if identifier is None or socket.query["channel_id"] == channel(identifier))

    async def _on_socket(self, socket: PeerSocket) -> None:
        identifier = socket.query["channel_id"][len(f"{ORG}:conv:") :]
        count = len(self.streams.get(identifier, []))
        await socket.send({"type": "ready", "schema": 1, "org_uuid": ORG, "channel_id": channel(identifier), "epoch": EPOCH,
                           "cursor": cursor(identifier, count), "sequence": str(count), "earliest_cursor": cursor(identifier, 0)})

    async def _http(self, request: web.Request) -> Any:
        self.requests.append(request.path)
        if request.path == "/me":
            return 200, {"user_uuid": ACTOR, "org": ORG, "id": 1, "email": "inbox@example.invalid", "roles": ["member"],
                         "scopes": [], "expires_at": None, "app_uuid": None}
        if request.path == "/api/sdk/capabilities":
            return 200, {"sdk_protocol_version": 1,
                         "authentication": {"user_sessions": True, "service_identities": False, "api_key_exchange": False,
                                            "app_binding_required": False, "mock_authentication": True},
                         "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
                                       "durable_replay": self.durable, "event_protocol_versions": [1] if self.durable else [],
                                       "websocket_scope": "channel"}}
        if request.path == "/api/conversations":
            if self.discovery_gate is not None:
                await asyncio.shield(self.discovery_gate)
            return self.discovery_status, list(self.rows.values())
        if request.path == "/api/messages/send":
            message = json.loads(await request.read())
            self.sends.append(message)
            return 200, {"status": "ACK", "message_id": message["id"], "created_at": 1000, "attachment_count": 0,
                         "attachment_manifest_blake3": "0" * 64}
        if request.path == "/api/events/sync":
            if self.denied:
                return 403, {}
            identifier = request.query["channel_id"][len(f"{ORG}:conv:") :]
            events = self.streams.get(identifier, [])
            after = request.query["after"]
            through = request.query.get("through") or cursor(identifier, len(events))
            page = [event for event in events if sequence(after) < int(event["sequence"]) <= sequence(through)][:200]
            last = page[-1]["cursor"] if page else after
            return 200, {"schema": 1, "org_uuid": ORG, "channel_id": channel(identifier), "epoch": EPOCH, "events": page,
                         "after_cursor": after, "through_cursor": through, "through_sequence": str(sequence(through)),
                         "next_cursor": None if sequence(last) == sequence(through) else last}
        return 404, {}

    def client(self, **overrides: Any) -> HumChat:
        async def provider(_: Any) -> dict[str, str]:
            return {"token": "synthetic-inbox-token"}

        options: dict[str, Any] = {
            "org_id": ORG, "base_url": self.origin, "token_provider": provider, "timeout_ms": 500,
            "retry": {"max_retries": 0, "total_timeout_ms": 1000},
            "realtime": {"handler_timeout_ms": 500, "ping_interval_ms": 1000, "pong_timeout_ms": 500,
                         "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20},
            **overrides,
        }
        client = HumChat(**options)
        self.clients.append(client)
        return client

    def add(self, identifier: str) -> None:
        self.rows[identifier] = conversation(identifier)

    def append(self, identifier: str, message_id: str) -> dict[str, Any]:
        events = self.streams.setdefault(identifier, [])
        number = len(events) + 1
        event = {"type": "event", "schema": 1, "org_uuid": ORG, "channel_id": channel(identifier), "epoch": EPOCH,
                 "sequence": str(number), "cursor": cursor(identifier, number), "event_id": f"{EPOCH}:{number}", "kind": "message",
                 "actor_id": PEER_ID,
                 "state": {"schema": 1, "org_uuid": ORG, "channel_id": channel(identifier), "message_id": message_id, "sender_id": PEER_ID,
                           "created_at": 1000, "thread_root_id": None, "priority": "standard", "original_body": message_id,
                           "body": message_id, "revision": 1, "updated_at": 1000, "deleted": False, "reaction_revision": 0,
                           "reactions": {}, "attachments": []}}
        events.append(event)
        return event

    async def publish(self, event: dict[str, Any]) -> None:
        for socket in list(self.sockets):
            if socket.open and socket.query.get("channel_id") == event["channel_id"]:
                try:
                    await socket.send(event)
                except Exception:  # A closing socket is not a fixture error.
                    pass

    def scope(self, identifier: str, consumer_id: str = "inbox") -> PersistenceScope:
        return PersistenceScope(server_origin=self.origin, org_id=ORG, actor_id=ACTOR, consumer_id=consumer_id, channel_id=channel(identifier))

    async def close(self) -> None:
        if self.discovery_gate is not None and not self.discovery_gate.done():
            self.discovery_gate.set_result(None)
        await asyncio.gather(*(client.close() for client in self.clients))
        await self.server.close()


async def fixture(after: Any) -> InboxPeer:
    peer = InboxPeer()
    await peer.server.start()
    peer.origin = peer.server.base_url
    after(peer.close)
    return peer


async def memory(after: Any) -> MemoryCheckpointStore:
    store = MemoryCheckpointStore()
    after(store.close)
    return store


def watch(client: HumChat, store: Any, handler: Callable[..., Any], **options: Any) -> Any:
    return client.inbox.watch(handler, **{"consumer_id": "inbox", "checkpoint_store": store, "poll_interval_ms": 100, **options})


async def loads(store: Any, scope: Any, expected: Any) -> bool:
    return await store.load(scope) == expected


async def test_inbox_discovers_a_remotely_created_dm_and_consumes_its_first_pre_discovery_message_then_resumes_from_checkpoint(after: Any) -> None:
    peer = await fixture(after)
    store = await memory(after)
    first = peer.client()
    observed: list[str] = []
    inbox = watch(first, store, lambda event, _: observed.append(event.state.id))
    await inbox.ready
    assert inbox.get_stats().conversations == 0
    peer.add("new")
    initial = peer.append("new", "before-discovery")
    await until(lambda: loads(store, peer.scope("new"), initial["cursor"]), "first message checkpoint")
    assert observed == ["before-discovery"]
    second = peer.append("new", "live")
    await peer.publish(second)
    await until(lambda: loads(store, peer.scope("new"), second["cursor"]), "live checkpoint")
    await first.close()
    await inbox.done
    third = peer.append("new", "while-offline")
    resumed: list[str] = []
    following = watch(peer.client(), store, lambda event, _: resumed.append(event.state.id))
    await following.ready
    await until(lambda: loads(store, peer.scope("new"), third["cursor"]), "resumed checkpoint")
    assert resumed == ["while-offline"]


async def test_membership_removal_closes_only_owned_conversation_handles_and_rejoining_uses_retained_checkpoint(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    peer.add("b")
    store = await memory(after)
    client = peer.client()
    seen: list[str] = []
    inbox = watch(client, store, lambda event, _: seen.append(event.state.id))
    await inbox.ready
    await until(lambda: all(entry.status == "live" for entry in inbox.get_stats().channels), "two live channels")
    event = peer.append("a", "a-first")
    await peer.publish(event)
    await until(lambda: loads(store, peer.scope("a"), event["cursor"]), "first checkpoint")
    del peer.rows["a"]
    await inbox.refresh()
    assert inbox.get_stats().conversations == 1
    assert client.realtime.get_stats().channels == 1
    missed = peer.append("a", "a-away")
    await peer.publish(missed)
    await asyncio.sleep(0.03)
    assert seen == ["a-first"]
    peer.add("a")
    await inbox.refresh()
    await until(lambda: loads(store, peer.scope("a"), missed["cursor"]), "rejoin checkpoint")
    assert seen == ["a-first", "a-away"]


async def test_automatic_inbox_rejoin_waits_for_a_cancelled_consumer_to_finish_cleanup_before_replaying(after: Any) -> None:
    peer = await fixture(after)
    peer.add("cleanup")
    event = peer.append("cleanup", "command")
    store = await memory(after)
    client = peer.client(realtime={"handler_timeout_ms": 3000})
    cleanup = Deferred()
    after(cleanup.resolve)
    state = {"active": 0, "peak": 0, "cancellation_observed": False, "cleanup_finished": False}
    starts: list[str] = []
    signals: list[AbortSignal] = []

    async def handler(item: Any, context: Any) -> None:
        starts.append(item.event_id)
        signals.append(context.signal)
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        try:
            if len(starts) == 1:
                await aborted(context.signal)
                state["cancellation_observed"] = True
                await cleanup
                state["cleanup_finished"] = True
            else:
                assert state["cleanup_finished"] is True, "Replacement must wait for cancelled application cleanup"
        finally:
            state["active"] -= 1

    inbox = watch(client, store, handler, poll_interval_ms=10000)
    await until(lambda: len(starts) == 1, "original consumer starts")
    del peer.rows["cleanup"]
    removal = inbox.refresh()
    removed = settled_flag(removal)
    await until(lambda: state["cancellation_observed"] and peer.all_closed(), "removal cancels the consumer and closes its socket")
    assert signals[0].aborted is True
    assert state["active"] == 1
    assert removed["done"] is False, "Removal must await cancelled application cleanup"
    assert inbox.get_stats().conversations == 0
    assert len(starts) == 1
    peer.add("cleanup")
    assert inbox.refresh() is removal, "Rejoin discovery coalesces with pending retirement"
    assert await store.load(peer.scope("cleanup")) != event["cursor"]
    cleanup.resolve()
    await removal
    await inbox.refresh()
    await until(lambda: loads(store, peer.scope("cleanup"), event["cursor"]), "replacement replay acknowledgement")
    assert starts == [event["event_id"], event["event_id"]]
    assert state["peak"] == 1
    assert state["active"] == 0
    assert inbox.status == "watching"
    assert inbox.error is None


async def test_automatic_inbox_rejoin_waits_for_an_in_flight_checkpoint_write_before_loading_the_retained_cursor(after: Any) -> None:
    peer = await fixture(after)
    peer.add("commit")
    event = peer.append("commit", "command")
    storage = await memory(after)
    write = Deferred()
    after(write.resolve)
    flags = {"write_started": False}
    cas_results: list[bool] = []

    async def compare_and_set(scope: Any, previous: Any, following: Any) -> bool:
        tracked = scope.channel_id == channel("commit") and following == event["cursor"]
        if tracked:
            flags["write_started"] = True
            await write
        result = await storage.compare_and_set(scope, previous, following)
        if tracked:
            cas_results.append(result)
        return result

    client = peer.client(realtime={"handler_timeout_ms": 3000})
    observed: list[str] = []
    inbox = watch(client, Store(storage.load, compare_and_set), lambda item, _: observed.append(item.event_id), poll_interval_ms=10000)
    await until(lambda: flags["write_started"], "original checkpoint write starts")
    del peer.rows["commit"]
    removal = inbox.refresh()
    removed = settled_flag(removal)
    await until(peer.all_closed, "removal closes the socket with its checkpoint write still pending")
    assert removed["done"] is False, "Removal must await the original checkpoint write"
    assert inbox.get_stats().conversations == 0
    assert observed == [event["event_id"]]
    assert cas_results == []
    peer.add("commit")
    assert inbox.refresh() is removal, "Rejoin discovery coalesces with pending checkpoint retirement"
    write.resolve()
    await removal
    await inbox.refresh()
    await until(lambda: len(inbox.get_stats().channels) == 1 and inbox.get_stats().channels[0].status == "live",
                "replacement resumes from committed checkpoint")
    assert await storage.load(peer.scope("commit")) == event["cursor"]
    assert observed == [event["event_id"]]
    assert cas_results == [True]
    following = peer.append("commit", "next")
    await peer.publish(following)
    await until(lambda: loads(storage, peer.scope("commit"), following["cursor"]), "replacement can acknowledge a new message")
    assert observed == [event["event_id"], following["event_id"]]
    assert inbox.status == "watching"
    assert inbox.error is None
    assert inbox.last_channel_error is None


@pytest.mark.parametrize("failure", ["rejection", "timeout", "conflict"])
async def test_automatic_inbox_rejoin_blocks_with_sanitized_error_after_checkpoint_retirement_failure(after: Any, failure: str) -> None:
    error_code = "CONFLICT" if failure == "conflict" else "PERSISTENCE"
    peer = await fixture(after)
    peer.add("store-failure")
    event = peer.append("store-failure", "command")
    storage = MemoryCheckpointStore()
    write = Deferred()
    pending: dict[str, Any] = {}

    async def cleanup() -> None:
        write.resolve()
        if "write" in pending:
            try:
                await pending["write"]
            except Exception:
                pass
        await storage.close()

    after(cleanup)

    async def held(scope: Any, previous: Any, following: Any) -> bool:
        result = await write
        return False if result is False else await storage.compare_and_set(scope, previous, following)

    def compare_and_set(scope: Any, previous: Any, following: Any) -> Any:
        if following == event["cursor"]:
            pending["write"] = asyncio.ensure_future(held(scope, previous, following))
            return pending["write"]
        return storage.compare_and_set(scope, previous, following)

    client = peer.client(retry={"max_retries": 0, "total_timeout_ms": 500})
    deliveries = {"count": 0}
    inbox = watch(client, Store(storage.load, compare_and_set), lambda *_: deliveries.__setitem__("count", deliveries["count"] + 1),
                  poll_interval_ms=10000)
    await until(lambda: "write" in pending, "checkpoint write is held")
    del peer.rows["store-failure"]
    removal = inbox.refresh()
    await until(peer.all_closed, "retirement closes the socket before the store settles")
    peer.add("store-failure")
    assert inbox.refresh() is removal
    private_detail = "private-checkpoint-backend-password"
    if failure == "rejection":
        write.reject(RuntimeError(private_detail))
    if failure == "conflict":
        write.resolve(False)
    await until(lambda: inbox.status == "blocked", "retirement reports the store failure within its 500 ms budget", 1.5)
    error = await rejects(removal, error_code)
    assert private_detail not in str(error)
    assert error.__cause__ is None
    await rejects(inbox.done, error_code)
    assert inbox.error is not None
    assert inbox.error.code == error_code
    assert private_detail not in str(inbox.error)
    assert deliveries["count"] == 1
    assert len(peer.upgrades) == 1
    assert inbox.get_stats().conversations == 0
    assert await storage.load(peer.scope("store-failure")) != event["cursor"]
    if failure == "timeout":
        write.resolve()
        await pending["write"]
        assert await storage.load(peer.scope("store-failure")) == event["cursor"], "An already-started write may finish after retirement times out"
    await rejects(inbox.refresh(), error_code)
    assert inbox.status == "blocked"
    assert deliveries["count"] == 1
    assert len(peer.upgrades) == 1


async def test_client_close_cancels_automatic_inbox_retirement_while_a_checkpoint_write_remains_pending(after: Any) -> None:
    peer = await fixture(after)
    peer.add("store-close")
    event = peer.append("store-close", "command")
    storage = MemoryCheckpointStore()
    write = Deferred()
    pending: dict[str, Any] = {}

    async def cleanup() -> None:
        write.resolve()
        if "write" in pending:
            await pending["write"]
        await storage.close()

    after(cleanup)

    async def held(scope: Any, previous: Any, following: Any) -> bool:
        await write
        return await storage.compare_and_set(scope, previous, following)

    def compare_and_set(scope: Any, previous: Any, following: Any) -> Any:
        if following == event["cursor"]:
            pending["write"] = asyncio.ensure_future(held(scope, previous, following))
            return pending["write"]
        return storage.compare_and_set(scope, previous, following)

    client = peer.client(retry={"max_retries": 0, "total_timeout_ms": 3000})
    deliveries = {"count": 0}
    inbox = watch(client, Store(storage.load, compare_and_set), lambda *_: deliveries.__setitem__("count", deliveries["count"] + 1),
                  poll_interval_ms=10000)
    await until(lambda: "write" in pending, "checkpoint write is held")
    del peer.rows["store-close"]
    removal = inbox.refresh()
    rejected_removal = asyncio.ensure_future(rejects(removal, "ABORT"))
    await until(peer.all_closed, "retirement reaches its pending checkpoint write")
    peer.add("store-close")
    assert inbox.refresh() is removal
    close = asyncio.ensure_future(client.close())
    await until(close.done, "client close completes before the 3 s checkpoint deadline", 1.0)
    await close
    await rejected_removal
    await inbox.done
    assert inbox.status == "closed"
    assert await storage.load(peer.scope("store-close")) != event["cursor"]
    assert client.realtime.get_stats().channels == 0
    assert deliveries["count"] == 1
    write.resolve()
    await pending["write"]
    assert await storage.load(peer.scope("store-close")) == event["cursor"], "An in-flight write may commit after close"
    assert inbox.status == "closed"
    assert deliveries["count"] == 1
    assert len(peer.upgrades) == 1


async def test_removing_multiple_conversations_cancels_every_consumer_before_awaiting_any_cleanup(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    peer.add("b")
    first = peer.append("a", "a-command")
    second = peer.append("b", "b-command")
    store = await memory(after)
    cleanup = Deferred()
    after(cleanup.resolve)
    signals: dict[str, AbortSignal] = {}
    cancelled: set[str] = set()
    client = peer.client(realtime={"handler_timeout_ms": 3000})

    async def handler(item: Any, context: Any) -> None:
        signals[item.state.id] = context.signal
        await aborted(context.signal)
        cancelled.add(item.state.id)
        if item.state.id == "a-command":
            await cleanup

    inbox = watch(client, store, handler, poll_interval_ms=10000)
    await until(lambda: len(signals) == 2, "both consumers start")
    peer.rows.clear()
    removal = inbox.refresh()
    removed = settled_flag(removal)
    await until(lambda: len(cancelled) == 2 and peer.all_closed(), "both removed conversations are cancelled while the first cleanup is held", 1.0)
    assert removed["done"] is False
    assert inbox.get_stats().conversations == 0
    assert all(signal.aborted for signal in signals.values())
    cleanup.resolve()
    await removal
    assert await store.load(peer.scope("a")) != first["cursor"]
    assert await store.load(peer.scope("b")) != second["cursor"]
    assert inbox.status == "watching"
    assert client.realtime.get_stats().channels == 0


async def test_removing_an_inbox_consumer_with_a_shared_observer_waits_for_cleanup_and_preserves_other_conversations(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    peer.add("b")
    first = peer.append("a", "a-command")
    store = await memory(after)
    cleanup = Deferred()
    after(cleanup.resolve)
    client = peer.client(realtime={"handler_timeout_ms": 3000})
    observed: list[str] = []
    flags = {"cancellation_observed": False}

    async def handler(item: Any, context: Any) -> None:
        observed.append(item.state.id)
        if item.state.id == "a-command":
            await aborted(context.signal)
            flags["cancellation_observed"] = True
            await cleanup

    inbox = watch(client, store, handler, poll_interval_ms=10000)
    await until(lambda: len(observed) == 1 and any(entry.channel_id == channel("b") and entry.status == "live" for entry in inbox.get_stats().channels),
                "consumer starts and the other channel becomes live")
    observer = client.realtime.subscribe(channel("a"), start="earliest")
    after(observer.close)
    del peer.rows["a"]
    removal = inbox.refresh()
    removed = settled_flag(removal)
    await until(lambda: flags["cancellation_observed"] and observer.status == "blocked" and peer.all_closed("a"),
                "shared channel stops while consumer cleanup remains held")
    assert removed["done"] is False, "Blocking the remaining observer must preserve the consumer cleanup deadline"
    assert inbox.status == "watching"
    assert inbox.get_stats().conversations == 1
    cleanup.resolve()
    await removal
    assert inbox.status == "watching"
    assert inbox.error is None
    following = peer.append("b", "b-command")
    await peer.publish(following)
    await until(lambda: loads(store, peer.scope("b"), following["cursor"]), "remaining inbox conversation still processes messages")
    assert observed == ["a-command", "b-command"]
    assert await store.load(peer.scope("a")) != first["cursor"]
    await observer.close()
    assert client.realtime.get_stats().channels == 1


async def test_automatic_inbox_rejoin_after_settled_work_retains_the_checkpoint_without_replaying_acknowledged_events(after: Any) -> None:
    peer = await fixture(after)
    peer.add("settled")
    event = peer.append("settled", "command")
    store = await memory(after)
    observed: list[str] = []
    inbox = watch(peer.client(), store, lambda item, _: observed.append(item.event_id), poll_interval_ms=10000)
    await until(lambda: loads(store, peer.scope("settled"), event["cursor"]), "original work is acknowledged")
    del peer.rows["settled"]
    await inbox.refresh()
    peer.add("settled")
    await inbox.refresh()
    await until(lambda: len(inbox.get_stats().channels) == 1 and inbox.get_stats().channels[0].status == "live", "rejoined channel catches up")
    assert observed == [event["event_id"]]
    assert await store.load(peer.scope("settled")) == event["cursor"]
    following = peer.append("settled", "after-rejoin")
    await peer.publish(following)
    await until(lambda: loads(store, peer.scope("settled"), following["cursor"]), "rejoined channel acknowledges new work")
    assert observed == [event["event_id"], following["event_id"]]
    assert inbox.status == "watching"


@pytest.mark.parametrize("closing", ["watcher", "client"])
async def test_closing_cancels_a_pending_automatic_rejoin_without_awaiting_consumer_cleanup(after: Any, closing: str) -> None:
    peer = await fixture(after)
    peer.add("closing")
    event = peer.append("closing", "command")
    store = await memory(after)
    client = peer.client(realtime={"handler_timeout_ms": 3000})
    cleanup = Deferred()
    after(cleanup.resolve)
    state = {"deliveries": 0, "cancellation_observed": False, "cleanup_finished": False}

    async def handler(_: Any, context: Any) -> None:
        state["deliveries"] += 1
        await aborted(context.signal)
        state["cancellation_observed"] = True
        await cleanup
        state["cleanup_finished"] = True

    inbox = watch(client, store, handler, poll_interval_ms=10000)
    await until(lambda: state["deliveries"] == 1, "original consumer starts")
    del peer.rows["closing"]
    removal = inbox.refresh()
    rejected_rejoin = asyncio.ensure_future(rejects(removal, "ABORT"))
    await until(lambda: state["cancellation_observed"] and peer.all_closed(), "retirement reaches the held consumer cleanup")
    peer.add("closing")
    assert inbox.refresh() is removal, "Rejoin remains pending while retirement awaits cleanup"
    close = asyncio.ensure_future(inbox.close() if closing == "watcher" else client.close())
    await until(close.done, "explicit close completes while application cleanup is held", 1.0)
    await close
    await rejected_rejoin
    await inbox.done
    assert inbox.status == "closed"
    assert state["cleanup_finished"] is False
    assert state["deliveries"] == 1
    assert client.realtime.get_stats().channels == 0
    assert await store.load(peer.scope("closing")) != event["cursor"]
    upgrades = len(peer.upgrades)
    cleanup.resolve()
    await until(lambda: state["cleanup_finished"], "application cleanup finishes after explicit close")
    assert len(peer.upgrades) == upgrades
    assert state["deliveries"] == 1


async def test_automatic_inbox_rejoin_blocks_at_the_handler_deadline_when_a_cancelled_consumer_never_settles(after: Any) -> None:
    peer = await fixture(after)
    peer.add("stuck")
    event = peer.append("stuck", "command")
    store = await memory(after)
    client = peer.client(realtime={"handler_timeout_ms": 500})
    state: dict[str, Any] = {"deliveries": 0, "signal": None}
    never = asyncio.get_running_loop().create_future()
    after(lambda: never.done() or never.set_result(None))

    def handler(_: Any, context: Any) -> Any:
        state["deliveries"] += 1
        state["signal"] = context.signal
        return asyncio.shield(never)

    inbox = watch(client, store, handler, poll_interval_ms=10000)
    await until(lambda: state["deliveries"] == 1, "never-settling consumer starts")
    del peer.rows["stuck"]
    removal = inbox.refresh()
    await until(lambda: state["signal"].aborted and peer.all_closed(), "retirement cancels the stuck consumer and closes its socket")
    peer.add("stuck")
    assert inbox.refresh() is removal, "Rejoin waits for the never-settling consumer"
    await until(lambda: inbox.status == "blocked", "rejoin observes the 500 ms handler deadline", 1.5)
    await rejects(removal, "PROTOCOL")
    await rejects(inbox.done, "PROTOCOL")
    assert inbox.status == "blocked"
    assert inbox.error is not None and inbox.error.code == "PROTOCOL"
    assert state["deliveries"] == 1
    assert len(peer.upgrades) == 1
    assert await store.load(peer.scope("stuck")) != event["cursor"]
    await rejects(inbox.refresh(), "PROTOCOL")
    assert state["deliveries"] == 1


async def test_discovery_failure_is_terminal_observable_and_stops_polling_and_delivery(after: Any) -> None:
    peer = await fixture(after)
    store = await memory(after)
    inbox = watch(peer.client(), store, lambda *_: None)
    await inbox.ready
    peer.discovery_status = 403
    await rejects(inbox.refresh(), "PERMISSION")
    await rejects(inbox.done, "PERMISSION")
    assert inbox.status == "blocked"
    assert inbox.error is not None and inbox.error.code == "PERMISSION"
    count = len(peer.requests)
    await asyncio.sleep(0.25)
    assert len(peer.requests) == count


async def test_initial_discovery_failures_reject_ready_and_unsupported_durable_replay_does_not_silently_fall_back(after: Any) -> None:
    peer = await fixture(after)
    store = await memory(after)
    peer.discovery_status = 500
    failed = watch(peer.client(), store, lambda *_: None)
    await rejects(failed.ready, "SERVER")
    await rejects(failed.done, "SERVER")
    peer.discovery_status = 200
    peer.durable = False
    unsupported = watch(peer.client(), store, lambda *_: None)
    await rejects(unsupported.ready, "UNSUPPORTED_FEATURE")
    assert peer.upgrades == []


async def test_conversation_capacity_rejects_explicitly_before_partially_subscribing(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    peer.add("b")
    store = await memory(after)
    inbox = watch(peer.client(), store, lambda *_: None, max_conversations=1)
    await rejects(inbox.ready, "BACKPRESSURE")
    await rejects(inbox.done, "BACKPRESSURE")
    assert peer.upgrades == []
    assert inbox.get_stats().conversations == 0


async def test_channel_permission_failure_and_consumer_failure_are_visible_and_do_not_advance_checkpoints(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    event = peer.append("a", "first")
    store = await memory(after)
    client = peer.client()

    def fail(*_: Any) -> None:
        raise RuntimeError("Application failure with private details")

    failing = watch(client, store, fail)
    await rejects(failing.done, "PROTOCOL")
    assert failing.status == "blocked"
    assert await store.load(peer.scope("a")) != event["cursor"]
    assert failing.error is not None and "private details" not in str(failing.error)
    await failing.close()
    peer.denied = True
    denied = watch(client, store, lambda *_: None)
    await rejects(denied.done, "PERMISSION")
    assert denied.status == "blocked"
    assert await store.load(peer.scope("a")) != event["cursor"]


async def test_closing_during_discovery_cancels_polling_without_opening_sockets_and_leaves_the_caller_store_open(after: Any) -> None:
    peer = await fixture(after)
    peer.add("late")
    gate: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    peer.discovery_gate = gate
    store = await memory(after)
    client = peer.client()

    def never(*_: Any) -> None:
        raise AssertionError("No callback after close")

    inbox = watch(client, store, never)
    await until(lambda: "/api/conversations" in peer.requests, "held discovery request")
    pending = inbox.refresh()
    await client.close()
    gate.set_result(None)
    await rejects(pending, "ABORT")
    await rejects(inbox.ready, "ABORT")
    await inbox.done
    count = len(peer.requests)
    await asyncio.sleep(0.25)
    assert inbox.status == "closed"
    assert len(peer.requests) == count
    assert peer.upgrades == []
    assert await store.load(peer.scope("late")) is None


async def test_discovery_refresh_coalesces_and_rejects_foreign_routing_or_missing_self_membership(after: Any) -> None:
    peer = await fixture(after)
    store = await memory(after)
    client = peer.client()
    inbox = watch(client, store, lambda *_: None)
    await inbox.ready
    gate: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    peer.discovery_gate = gate
    first = inbox.refresh()
    second = inbox.refresh()
    assert first is second
    peer.rows["foreign"] = {**conversation("foreign"), "channel_key": "other-org:conv:foreign"}
    gate.set_result(None)
    await rejects(first, "PROTOCOL")
    assert peer.upgrades == []
    await inbox.close()
    peer.rows.clear()
    peer.rows["missing"] = {**conversation("missing"), "members": [{"user_uuid": "outsider", "role": "member"}, {"user_uuid": PEER_ID, "role": "member"}]}
    following = watch(client, store, lambda *_: None)
    await rejects(following.ready, "PROTOCOL")
    assert peer.upgrades == []


async def test_closing_an_inbox_aborts_its_active_consumer_without_acknowledging_and_preserves_an_unrelated_manual_subscription(after: Any) -> None:
    peer = await fixture(after)
    peer.add("a")
    event = peer.append("a", "held")
    store = await memory(after)
    client = peer.client()
    manual = client.realtime.subscribe(channel("manual"), start="earliest")
    await manual.ready
    began = asyncio.get_running_loop().create_future()
    seen: dict[str, Any] = {}

    async def handler(_: Any, context: Any) -> None:
        seen["signal"] = context.signal
        if not began.done():
            began.set_result(None)
        await aborted(context.signal)

    inbox = watch(client, store, handler)
    await began
    await inbox.close()
    await inbox.done
    assert seen["signal"].aborted is True
    assert manual.status == "live"
    assert client.realtime.get_stats().channels == 1
    assert await store.load(peer.scope("a")) != event["cursor"]


async def test_runnable_dm_responder_discovers_a_retained_command_replies_in_its_thread_and_releases_file_store_locks(after: Any) -> None:
    peer = await fixture(after)
    peer.add("example")
    event = peer.append("example", "command")
    event["state"]["body"] = "!status"
    event["state"]["original_body"] = "!status"
    artifacts = ROOT / "target" / "dm-inbox"
    artifacts.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="responder-", dir=artifacts))
    keep = {"PATH", "SYSTEMROOT", "TEMP", "TMP", "USERPROFILE", "APPDATA", "LOCALAPPDATA"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in keep}
    environment.update({"HUMCHAT_ORG_ID": ORG, "HUMCHAT_BASE_URL": peer.origin, "HUMCHAT_TOKEN": "synthetic-example-token",
                        "HUMCHAT_STATE_DIR": str(directory), "HUMCHAT_CONSUMER_ID": "example", "HUMCHAT_RUN_FOR_MS": "600"})
    child = await asyncio.create_subprocess_exec(sys.executable, str(ROOT / "examples" / "dm_responder.py"), cwd=str(ROOT), env=environment,
                                                 stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    def kill() -> None:
        if child.returncode is None:
            child.kill()

    after(kill)
    stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
    output, errors = stdout.decode(), stderr.decode()
    assert child.returncode == 0, errors
    assert errors == ""
    assert '"event":"watching"' in output
    assert '"event":"reply"' in output
    assert '"event":"stopped"' in output
    assert len(peer.sends) == 1
    assert peer.sends[0]["channel_id"] == channel("example")
    assert peer.sends[0]["thread_root_id"] == "command"
    assert peer.sends[0]["content"] == "All systems operational."
    assert not (directory / "dm-checkpoints.json.lock").exists()
    assert not (directory / "dm-outbox.json.lock").exists()
