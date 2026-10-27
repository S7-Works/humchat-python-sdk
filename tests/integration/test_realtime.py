"""Port of test/integration/realtime.test.mjs (best-effort channel worker).

A loopback protocol peer, not an alternative authentication authority. Every credential
is a fixed synthetic string; no private keys or user tokens exist.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from helpers import Deferred, LoopbackPeer, PeerSocket, Upgrade, code, rejects
from humchat import HumChat, HumChatError

ORG = "realtime-org"
ACTOR = "realtime-actor"
CHANNEL = f"{ORG}:team:general"
SECOND = f"{ORG}:team:second"
THIRD = f"{ORG}:team:third"
TIMESTAMP = 1_792_370_346
CAPS = {
    "sdk_protocol_version": 1,
    "authentication": {
        "user_sessions": True, "service_identities": False, "api_key_exchange": False, "app_binding_required": False, "mock_authentication": True,
    },
    "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2], "durable_replay": False, "websocket_scope": "channel"},
}
EVENT_NAMES = [
    "message", "message_updated", "message_deleted", "reaction_updated", "state_reconciled", "recovery_complete",
    "recovery_incomplete", "resync", "error", "handler_error", "connection", "session_revoked", "auth_required", "unknown_event",
]  # fmt: skip


def state(identifier: str, **extra: Any) -> dict[str, Any]:
    return {
        "schema": 1, "org_uuid": ORG, "channel_id": CHANNEL, "message_id": identifier, "sender_id": ACTOR, "created_at": TIMESTAMP,
        "thread_root_id": None, "priority": "standard", "original_body": f"Message {identifier}", "body": f"Message {identifier}",
        "revision": 1, "updated_at": TIMESTAMP, "deleted": False, "reaction_revision": 0, "reactions": {}, "attachments": [], **extra,
    }  # fmt: skip


def message(identifier: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": identifier, "org_uuid": ORG, "channel_id": CHANNEL, "sender_id": ACTOR, "created_at": TIMESTAMP,
        "thread_root_id": None, "priority": "standard", "content": f"Message {identifier}", "is_long": False, **extra,
    }  # fmt: skip


async def until(predicate: Callable[[], bool], label: str, timeout: float = 4.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("Timed out: " + label)


async def sleep(milliseconds: float) -> None:
    await asyncio.sleep(milliseconds / 1000)


def capture(subscription: Any, names: list[str] = EVENT_NAMES) -> dict[str, list[Any]]:
    events: dict[str, list[Any]] = {name: [] for name in names}
    for name in names:
        subscription.on(name, events[name].append)
    return events


def has(subscription_value: Any) -> bool:
    return subscription_value is not None


class Fixture:
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        self.states: dict[str, dict[str, Any]] = {}
        self.clients: list[HumChat] = []
        self.peer = LoopbackPeer(self._http, upgrade=self._upgrade, auto_pong=options.get("auto_pong", True))

    @property
    def requests(self) -> list[dict[str, Any]]:
        return self.peer.request_log

    @property
    def upgrades(self) -> list[Upgrade]:
        return self.peer.upgrades

    @property
    def sockets(self) -> list[PeerSocket]:
        return self.peer.sockets

    async def _upgrade(self, upgrade: Upgrade) -> Any:
        hook = self.options.get("upgrade")
        if hook is None:
            return None
        result = hook(upgrade)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    async def _http(self, request: Any) -> Any:
        path = request.path
        if path == "/me":
            return 200, {"user_uuid": ACTOR, "org": ORG, "id": 1, "email": "realtime@example.test", "roles": ["member"],
                         "scopes": [], "expires_at": None, "app_uuid": None}  # fmt: skip
        if path == "/api/sdk/capabilities":
            return 200, CAPS
        if path == "/api/messages/sync":
            scope = {"channel": request.query.get("channel_id"), "thread": request.query.get("thread_root_id"), "cursor": request.query.get("native_cursor")}
            hook = self.options.get("history")
            if hook is not None:
                custom = await hook(scope)
                if custom is not None:
                    return 200, custom
            rows = sorted(
                (row for row in self.states.values() if row["channel_id"] == scope["channel"] and row["thread_root_id"] == scope["thread"]),
                key=lambda row: (row["created_at"], row["message_id"]),
            )
            if scope["cursor"]:
                cursor = json.loads(bytes.fromhex(scope["cursor"]).decode("utf-8"))
                rows = [row for row in rows if row["created_at"] < cursor["created_at"]
                        or (row["created_at"] == cursor["created_at"] and row["message_id"] < cursor["message_id"])]  # fmt: skip
            limit = min(self.options.get("page_size", 200), int(request.query.get("limit", 200)))
            more = len(rows) > limit
            rows = rows[-limit:]
            next_cursor = None
            if more:
                oldest = rows[0]
                next_cursor = json.dumps({"schema": 2, "org_uuid": ORG, "channel_id": scope["channel"], "thread_root_id": scope["thread"],
                                          "created_at": oldest["created_at"], "message_id": oldest["message_id"]},
                                         separators=(",", ":")).encode("utf-8").hex()  # fmt: skip
            return 200, {"schema": 2, "org_uuid": ORG, "channel_id": scope["channel"], "thread_root_id": scope["thread"],
                         "messages": rows, "next_cursor": next_cursor}  # fmt: skip
        match = re.fullmatch(r"/api/messages/([^/]+)/state", path)
        if match:
            value = self.states.get(match.group(1))
            return (200, value) if value is not None else (404, {})
        return 404, {}

    def client(self, **overrides: Any) -> HumChat:
        async def provider(_: Any) -> dict[str, str]:
            return {"token": "synthetic-realtime-token"}

        options: dict[str, Any] = {
            "org_id": ORG, "base_url": self.peer.base_url, "token_provider": provider, "timeout_ms": 700,
            "retry": {"max_retries": 0, "total_timeout_ms": 1500}, "recovery": {"mode": "best_effort", "max_pages": 3, "max_threads": 20},
            "realtime": {"max_channels": 5, "connect_concurrency": 2, "max_buffered_events": 100, "max_buffered_bytes": 100_000,
                         "reconnect_initial_delay_ms": 10, "reconnect_max_delay_ms": 30, "ping_interval_ms": 500, "pong_timeout_ms": 200,
                         "handler_timeout_ms": 500},
            **overrides,
        }  # fmt: skip
        client = HumChat(**options)
        self.clients.append(client)
        return client

    def latest(self, bound: str = CHANNEL) -> PeerSocket | None:
        return self.peer.latest("/ws/chat", channel_id=bound)

    async def send(self, frame: Any, bound: str = CHANNEL) -> None:
        socket = self.latest(bound)
        assert socket is not None, "Expected an open controlled socket"
        await socket.send(frame)

    def sync_requests(self) -> int:
        return sum(1 for request in self.requests if request["path"] == "/api/messages/sync")


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


async def barrier() -> None:
    """Stand-in for Node's ping/pong ordering barrier: the client reads frames in order."""
    await sleep(100)


async def test_headers_authenticate_one_shared_channel_socket_closing_one_handle_preserves_the_other(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    first = client.realtime.subscribe(CHANNEL)
    second = client.realtime.subscribe(CHANNEL)
    seen_first = capture(first)
    seen_second = capture(second)
    await asyncio.gather(first.ready, second.ready)
    assert len(fixture.upgrades) == 1
    assert fixture.upgrades[0].query.get("channel_id") == CHANNEL
    assert "token" not in fixture.upgrades[0].query
    assert fixture.upgrades[0].headers["authorization"] == "Bearer synthetic-realtime-token"
    assert client.realtime.get_stats().active_sockets == 1
    await first.close()
    await fixture.send(message("after-first-close"))
    await until(lambda: len(seen_second["message"]) == 1, "remaining handle receives live message")
    assert len(seen_first["message"]) == 0
    assert seen_second["message"][0].message.id == "after-first-close"
    await second.close()
    await until(lambda: fixture.latest() is None, "last handle releases physical socket")
    assert client.realtime.get_stats().active_sockets == 0


async def test_bootstrap_history_reconciles_state_without_replaying_old_message_commands(peer: Peer) -> None:
    fixture = await peer()
    fixture.states["historic-root"] = state("historic-root")
    fixture.states["historic-reply"] = state("historic-reply", thread_root_id="historic-root")
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    assert len(seen["message"]) == 0
    assert any(event.state.id == "historic-root" for event in seen["state_reconciled"])
    assert any(request["query"].get("thread_root_id") == "historic-root" for request in fixture.requests)
    assert len(seen["recovery_incomplete"]) > 0, "Best-effort history cannot claim complete event recovery"


async def test_buffered_duplicate_live_frames_are_delivered_once_after_recovery_finishes(peer: Peer) -> None:
    gate = Deferred()
    history_started = Deferred()
    blocked = False

    async def history(scope: dict[str, Any]) -> None:
        nonlocal blocked
        if not blocked and scope["thread"] is None:
            blocked = True
            history_started.resolve()
            await gate

    fixture = await peer(history=history)
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await history_started
    await fixture.send(message("buffered-message"))
    await fixture.send(message("buffered-message"))
    # The same committed creation can appear in both the HTTP snapshot and the already-open
    # socket. Its live notification must still be delivered once.
    fixture.states["buffered-message"] = state("buffered-message")
    await sleep(30)
    assert len(seen["message"]) == 0
    gate.resolve()
    await sub.ready
    await until(lambda: len(seen["message"]) == 1, "one buffered message delivered")
    await fixture.send(message("buffered-message"))
    await sleep(40)
    assert len(seen["message"]) == 1


async def test_disconnect_and_resync_reconcile_roots_and_known_threads_without_fake_edit_events(peer: Peer) -> None:
    fixture = await peer()
    fixture.states["known-root"] = state("known-root")
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    fixture.states["missed-reply"] = state("missed-reply", thread_root_id="known-root", created_at=TIMESTAMP + 1)
    latest = fixture.latest()
    assert latest is not None
    await latest.terminate()
    await until(lambda: len(fixture.sockets) >= 2 and sub.status == "live", "reconnection and recovery")
    await until(lambda: any(event.state.id == "missed-reply" for event in seen["state_reconciled"]), "known-thread catch-up")
    assert any(event.message.id == "missed-reply" and event.source == "recovery" and event.replayed for event in seen["message"]), (
        "Previously empty, scanned threads must retain their baseline for new reply recovery"
    )
    recovered = sum(1 for request in fixture.requests if request["query"].get("thread_root_id") == "known-root")
    assert recovered >= 2
    fixture.states["known-root"] = state("known-root", body="Edited while offline", revision=2, updated_at=TIMESTAMP + 2)
    await fixture.send({"type": "resync", "channel_id": CHANNEL})
    await until(
        lambda: any(event.state.id == "known-root" and event.state.revision == 2 for event in seen["state_reconciled"]), "resync state reconciliation"
    )
    assert len(seen["message_updated"]) == 0, "Snapshots must not become fabricated edit notifications"
    assert len(seen["recovery_incomplete"]) >= 2


async def test_403_handshake_blocks_without_refreshing_or_repeatedly_reconnecting(peer: Peer) -> None:
    fixture = await peer(upgrade=lambda _: 403)
    provider_calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return {"token": "synthetic-forbidden-token"}

    sub = fixture.client(token_provider=provider).realtime.subscribe(CHANNEL)
    capture(sub)
    await rejects(sub.ready, code("PERMISSION"))
    await sleep(80)
    assert sub.status == "blocked"
    assert provider_calls == 1
    assert len(fixture.upgrades) == 1


async def test_401_handshake_refreshes_once_and_preserves_header_only_authentication(peer: Peer) -> None:
    fixture = await peer(upgrade=lambda request: 401 if request.number == 1 else None)
    reasons: list[str] = []

    async def provider(context: Any) -> dict[str, str]:
        reasons.append(context.reason)
        return {"token": f"synthetic-token-{len(reasons)}"}

    sub = fixture.client(token_provider=provider).realtime.subscribe(CHANNEL)
    capture(sub)
    await sub.ready
    assert reasons == ["initial", "unauthorized"]
    assert len(fixture.upgrades) == 2
    assert fixture.upgrades[1].headers["authorization"] == "Bearer synthetic-token-2"
    assert "token" not in fixture.upgrades[1].query


async def test_repeated_401_handshake_is_terminal_for_its_connection_attempt(peer: Peer) -> None:
    fixture = await peer(upgrade=lambda _: 401)
    provider_calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return {"token": f"synthetic-token-{provider_calls}"}

    sub = fixture.client(token_provider=provider).realtime.subscribe(CHANNEL)
    capture(sub)
    await rejects(sub.ready, code("AUTHENTICATION"))
    await sleep(80)
    assert len(fixture.upgrades) == 2
    assert provider_calls == 2
    assert sub.status == "blocked"


async def test_session_revocation_closes_every_channel_and_cannot_be_bypassed_by_automatic_refresh(peer: Peer) -> None:
    fixture = await peer()
    provider_calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return {"token": f"synthetic-token-{provider_calls}"}

    client = fixture.client(token_provider=provider)
    first = client.realtime.subscribe(CHANNEL)
    second = client.realtime.subscribe(SECOND)
    first_seen = capture(first)
    capture(second)
    await asyncio.gather(first.ready, second.ready)
    await fixture.send({"type": "session_revoked", "reason": "session deprovisioned"})
    await until(lambda: first.status == "blocked" and second.status == "blocked", "all channels blocked on revocation")
    await until(lambda: len(first_seen["session_revoked"]) == 1, "revocation notification")
    await sleep(100)
    assert len(first_seen["session_revoked"]) == 1
    assert provider_calls == 1
    assert len(fixture.upgrades) == 2
    assert client.realtime.get_stats().active_sockets == 0
    await rejects(client.connect(), code("SESSION_REVOKED"))


async def test_a_silent_peer_triggers_pong_timeout_and_reconnect_instead_of_staying_falsely_live(peer: Peer) -> None:
    fixture = await peer(auto_pong=False)
    realtime = {"ping_interval_ms": 30, "pong_timeout_ms": 20, "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 10}
    sub = fixture.client(realtime=realtime).realtime.subscribe(CHANNEL)
    capture(sub)
    await sub.ready
    await until(lambda: fixture.peer.ping_count > 0 and len(fixture.upgrades) >= 2, "pong timeout caused reconnect")


async def test_active_pongs_keep_a_connection_stable_across_several_heartbeat_intervals(peer: Peer) -> None:
    fixture = await peer()
    sub = fixture.client(realtime={"ping_interval_ms": 25, "pong_timeout_ms": 100}).realtime.subscribe(CHANNEL)
    capture(sub)
    await sub.ready
    await until(lambda: fixture.peer.ping_count >= 3, "three answered pings")
    assert len(fixture.upgrades) == 1
    assert sub.status == "live"


async def test_wrong_scope_frames_and_malformed_embedded_state_block_instead_of_becoming_commands(peer: Peer) -> None:
    fixture = await peer()
    cases = [
        message("foreign-org", org_uuid="another-org"),
        message("foreign-channel", channel_id=SECOND),
        {"type": "edit", "org_uuid": ORG, "channel_id": CHANNEL, "message_id": "malformed-state", "state": None},
    ]
    for frame in cases:
        sub = fixture.client().realtime.subscribe(CHANNEL)
        seen = capture(sub)
        await sub.ready
        await fixture.send(frame)
        await until(lambda sub=sub: sub.status == "blocked", "malformed channel is blocked")
        await until(lambda seen=seen: any(error.code == "PROTOCOL" for error in seen["error"]), "protocol error notified")
        assert len(seen["message"]) == 0
        assert len(seen["message_updated"]) == 0
        await sub.close()


async def test_stale_content_cannot_replace_a_newer_edit_or_resurrect_a_tombstone(peer: Peer) -> None:
    fixture = await peer()
    current = state("versioned-root", revision=3, body="Newest edited body", updated_at=TIMESTAMP + 3)
    fixture.states["versioned-root"] = current
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    await fixture.send({"type": "edit", "org_uuid": ORG, "channel_id": CHANNEL, "message_id": "versioned-root",
                        "state": {**current, "revision": 2, "body": "Older edit", "updated_at": TIMESTAMP + 2}})  # fmt: skip
    await sleep(40)
    assert len(seen["message_updated"]) == 0
    await fixture.send({"type": "reaction", "org_uuid": ORG, "channel_id": CHANNEL, "message_id": "versioned-root",
                        "state": state("versioned-root", reaction_revision=2, reactions={"\U0001f44d": [ACTOR]})})  # fmt: skip
    await until(lambda: len(seen["reaction_updated"]) == 1, "independent reaction revision merged")
    assert seen["reaction_updated"][0].state.revision == 3
    assert seen["reaction_updated"][0].state.body == "Newest edited body"
    await fixture.send({"type": "delete", "org_uuid": ORG, "channel_id": CHANNEL, "message_id": "versioned-root",
                        "state": {**current, "revision": 4, "deleted": True, "reaction_revision": 2, "reactions": {"\U0001f44d": [ACTOR]},
                                  "updated_at": TIMESTAMP + 4}})  # fmt: skip
    await until(lambda: len(seen["message_deleted"]) == 1, "tombstone delivered")
    await fixture.send(message("versioned-root"))
    await sleep(40)
    assert len(seen["message"]) == 0
    assert sub.status == "live"


async def test_equal_revision_conflicting_state_is_a_protocol_failure(peer: Peer) -> None:
    fixture = await peer()
    original = state("conflicting-root", revision=2, body="First edit", updated_at=TIMESTAMP + 1)
    fixture.states["conflicting-root"] = original
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    await fixture.send({"type": "edit", "org_uuid": ORG, "channel_id": CHANNEL, "message_id": "conflicting-root",
                        "state": {**original, "body": "Conflicting edit at same revision"}})  # fmt: skip
    await until(lambda: sub.status == "blocked", "equal revision conflict detected")
    await until(lambda: any(error.code == "PROTOCOL" for error in seen["error"]), "protocol error notified")
    assert len(seen["message_updated"]) == 0


async def test_a_hung_handler_cannot_grow_the_event_queue_beyond_its_configured_budget(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_buffered_events": 2, "max_buffered_bytes": 50_000, "handler_timeout_ms": 1000})
    sub = client.realtime.subscribe(CHANNEL)
    seen = capture(sub)
    started = Deferred()

    async def hung(_: Any) -> None:
        started.resolve()
        await asyncio.Event().wait()

    sub.on("message", hung)
    await sub.ready
    await fixture.send(message("hung-handler-first"))
    await started
    for index in range(5):
        await fixture.send(message(f"overload-{index}"))
    await until(lambda: sub.status == "blocked", "bounded queue blocks overloaded channel", 0.7 * 3)
    await until(lambda: any(error.code == "BACKPRESSURE" for error in seen["error"]), "backpressure notified")
    assert client.realtime.get_stats().queued_events <= 2
    assert client.realtime.get_stats().queued_bytes <= 50_000


async def test_handler_failures_are_sanitized_and_do_not_automatically_replay_business_callbacks(peer: Peer) -> None:
    fixture = await peer()
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    calls = 0

    def failing(_: Any) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("SYNTHETIC_SECRET_FROM_HANDLER")

    sub.on("message", failing)
    await sub.ready
    await fixture.send(message("failing-handler"))
    await until(lambda: sub.status == "blocked", "failed handler blocks channel")
    await until(lambda: len(seen["handler_error"]) == 1, "handler error notified")
    assert "SYNTHETIC_SECRET_FROM_HANDLER" not in repr(seen["handler_error"])
    assert "SYNTHETIC_SECRET_FROM_HANDLER" not in str(seen["handler_error"][0].error)
    await sleep(100)
    assert calls == 1
    assert len(fixture.upgrades) == 1


async def test_channel_admission_and_connection_concurrency_are_bounded(peer: Peer) -> None:
    active = 0
    maximum = 0

    async def upgrade(_: Upgrade) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await sleep(40)
        active -= 1

    fixture = await peer(upgrade=upgrade)
    client = fixture.client(realtime={"max_channels": 2, "connect_concurrency": 1})
    results = client.realtime.subscribe_many([CHANNEL, SECOND, THIRD])
    assert len(results) == 3
    assert results[0].subscription is not None
    assert results[1].subscription is not None
    assert isinstance(results[2].error, HumChatError)
    for result in results:
        if result.subscription is not None:
            capture(result.subscription)
    await asyncio.gather(*(result.subscription.ready for result in results if result.subscription is not None))
    assert maximum == 1
    assert len(fixture.upgrades) == 2
    assert client.realtime.get_stats().active_sockets == 2
    assert client.realtime.get_stats().channels == 2


async def test_multiple_channels_rotate_an_expiring_token_through_one_shared_refresh(peer: Peer) -> None:
    fixture = await peer()
    reasons: list[str] = []

    async def provider(context: Any) -> dict[str, Any]:
        reasons.append(context.reason)
        return {"token": f"synthetic-rotation-{len(reasons)}", "expires_at": int(time.time()) + (3 if len(reasons) == 1 else 120)}

    client = fixture.client(token_provider=provider)
    first = client.realtime.subscribe(CHANNEL)
    second = client.realtime.subscribe(SECOND)
    capture(first)
    capture(second)
    await asyncio.gather(first.ready, second.ready)
    await until(
        lambda: sum(1 for entry in fixture.sockets if entry.headers.get("authorization") == "Bearer synthetic-rotation-2") == 2,
        "both channels use refreshed credential",
        6,
    )
    await until(lambda: first.status == "live" and second.status == "live", "rotated channels recover")
    assert reasons == ["initial", "expiring"]
    assert client.realtime.get_stats().active_sockets == 2


async def test_bounded_history_stops_at_its_page_budget_and_reports_incomplete_coverage(peer: Peer) -> None:
    fixture = await peer(page_size=1)
    fixture.states["older-root"] = state("older-root")
    fixture.states["newer-root"] = state("newer-root", created_at=TIMESTAMP + 1)
    recovery = {"mode": "best_effort", "max_pages": 1, "max_threads": 0, "max_state_refresh": 0}
    sub = fixture.client(recovery=recovery).realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    await until(lambda: len(seen["recovery_complete"]) >= 1, "recovery notifications")
    assert fixture.sync_requests() == 1
    assert len(seen["message"]) == 0
    assert len(seen["state_reconciled"]) == 1
    assert seen["recovery_complete"][-1].complete is False
    assert any(any(re.search(r"page|limit|budget", reason, re.IGNORECASE) for reason in event.limitations) for event in seen["recovery_incomplete"])


async def test_repeated_resync_requests_coalesce_into_one_running_attempt_and_one_rerun(peer: Peer) -> None:
    gate = Deferred()
    started = Deferred()
    hold = False

    async def history(scope: dict[str, Any]) -> None:
        if hold and scope["thread"] is None:
            started.resolve()
            await gate

    fixture = await peer(history=history)
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    await until(lambda: len(seen["recovery_complete"]) == 1, "initial recovery notification")
    before = len(seen["recovery_complete"])
    hold = True
    await sub.resync()
    await started
    await asyncio.gather(*(sub.resync() for _ in range(5)))
    gate.resolve()
    await until(lambda: len(seen["recovery_complete"]) >= before + 2 and sub.status == "live", "coalesced resync rerun")
    await sleep(40)
    assert fixture.sync_requests() == 3
    assert len(seen["recovery_complete"]) == before + 2


async def test_one_oversized_queued_event_cannot_bypass_the_byte_budget(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"max_buffered_bytes": 400, "max_buffered_events": 100})
    sub = client.realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    await fixture.send(message("large-event", content="x" * 1000))
    await until(lambda: sub.status == "blocked", "byte budget enforcement")
    await until(lambda: any(error.code == "BACKPRESSURE" for error in seen["error"]), "backpressure notified")
    assert len(seen["message"]) == 0
    assert client.realtime.get_stats().queued_bytes <= 400


async def test_a_transient_503_upgrade_retries_with_backoff_and_becomes_live(peer: Peer) -> None:
    fixture = await peer(upgrade=lambda request: 503 if request.number == 1 else None)
    client = fixture.client()
    sub = client.realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    assert len(fixture.upgrades) == 2
    await until(lambda: any(event.state == "backoff" for event in seen["connection"]), "backoff notification")
    assert client.realtime.get_stats().reconnect_count == 1


async def test_a_long_creation_shared_by_catch_up_and_buffered_live_delivery_is_deduplicated(peer: Peer) -> None:
    gate = Deferred()
    recovering = Deferred()
    root_reads = 0

    async def history(scope: dict[str, Any]) -> None:
        nonlocal root_reads
        if scope["thread"] is None:
            root_reads += 1
            if root_reads == 2:
                recovering.resolve()
                await gate

    fixture = await peer(history=history)
    sub = fixture.client().realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    latest = fixture.latest()
    assert latest is not None
    await latest.terminate()
    await recovering
    full_body = "Long synthetic body. " * 300
    fixture.states["long-recovered-root"] = state("long-recovered-root", original_body=full_body, body=full_body, created_at=TIMESTAMP + 10)
    await fixture.send(message("long-recovered-root", content=full_body[:900] + "…", is_long=True, body_blake3="a" * 64, created_at=TIMESTAMP + 10))
    gate.resolve()
    await until(lambda: sub.status in ("live", "blocked") and fixture.sync_requests() >= 2, "long-message overlap processed")
    await until(lambda: sub.status != "syncing", "recovery settled")
    assert sub.status == "live"
    await sleep(40)
    assert sum(1 for event in seen["message"] if event.message.id == "long-recovered-root") == 1
    assert sum(1 for error in seen["error"] if error.code == "PROTOCOL") == 0


async def test_a_near_expiry_credential_rotates_before_the_server_revokes_its_old_socket(peer: Peer) -> None:
    # Align within a second so the initial integer expiry leaves enough time for handshakes,
    # while always remaining shorter than an incorrect 1-second floor.
    await until(lambda: 100 <= (time.time() * 1000) % 1000 < 250, "short-token clock window", 1.5)
    expires_at = int(time.time()) + 1
    fixture = await peer()
    reasons: list[str] = []

    async def provider(context: Any) -> dict[str, Any]:
        reasons.append(context.reason)
        return {"token": f"synthetic-short-{len(reasons)}", "expires_at": expires_at if len(reasons) == 1 else int(time.time()) + 120}

    sub = fixture.client(token_provider=provider).realtime.subscribe(CHANNEL)
    seen = capture(sub)
    await sub.ready
    old = fixture.latest()
    assert old is not None

    async def revoke_old() -> None:
        await asyncio.sleep(max(0.0, expires_at - time.time()))
        if old.open:
            await old.send({"type": "session_revoked", "reason": "session expired"})

    expiry = asyncio.ensure_future(revoke_old())
    try:
        await until(lambda: len(fixture.upgrades) >= 2 or sub.status == "blocked", "short-token refresh", 2)
        assert sub.status != "blocked"
        await until(lambda: sub.status == "live", "short-token replacement socket ready")
        assert reasons == ["initial", "expiring"]
        assert len(seen["session_revoked"]) == 0
        assert fixture.upgrades[1].at < expires_at * 1000, "Replacement must open before the old credential expires"
    finally:
        expiry.cancel()


async def test_closing_during_a_resync_waiting_on_a_handler_cannot_revive_a_stale_generation(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client()
    sub = client.realtime.subscribe(CHANNEL)
    seen = capture(sub)
    handler_started = Deferred()

    async def hung(_: Any) -> None:
        handler_started.resolve()
        await asyncio.Event().wait()

    sub.on("message", hung)
    await sub.ready
    await fixture.send(message("resync-before-close"))
    await handler_started
    await sub.resync()
    await client.close()
    counts_at_close = {name: len(values) for name, values in seen.items()}
    await sleep(30)
    assert sub.status == "closed"
    assert {name: len(values) for name, values in seen.items()} == counts_at_close, (
        "Close cancels queued control notifications and stale recovery callbacks"
    )
    assert client.realtime.get_stats().active_sockets == 0
    assert client.realtime.get_stats().queued_events == 0


async def test_a_handle_attached_during_initial_buffered_delivery_waits_for_the_shared_ready_boundary(peer: Peer) -> None:
    history_gate = Deferred()
    history_started = Deferred()
    handler_gate = Deferred()
    handler_started = Deferred()
    hold_history = True

    async def history(scope: dict[str, Any]) -> None:
        nonlocal hold_history
        if hold_history and scope["thread"] is None:
            hold_history = False
            history_started.resolve()
            await history_gate

    fixture = await peer(history=history)
    client = fixture.client()
    first = client.realtime.subscribe(CHANNEL)
    capture(first)

    async def held(_: Any) -> None:
        handler_started.resolve()
        await handler_gate

    first.on("message", held)
    await history_started
    await fixture.send(message("held-initial-delivery"))
    history_gate.resolve()
    await handler_started
    second = client.realtime.subscribe(CHANNEL)
    capture(second)
    second_ready = False

    async def watch() -> None:
        nonlocal second_ready
        try:
            await second.ready
            second_ready = True
        except Exception:
            pass

    watcher = asyncio.ensure_future(watch())
    await sleep(20)
    assert second_ready is False, "A live socket alone is not completed initial recovery"
    handler_gate.resolve()
    await asyncio.gather(first.ready, second.ready)
    await watcher
    assert len(fixture.upgrades) == 1


async def test_a_resync_notification_flood_keeps_only_one_active_callback_and_one_pending_notification(peer: Peer) -> None:
    history_gate = Deferred()
    history_started = Deferred()
    callback_gate = Deferred()
    callback_started = Deferred()
    root_reads = 0

    async def history(scope: dict[str, Any]) -> None:
        nonlocal root_reads
        if scope["thread"] is None:
            root_reads += 1
            if root_reads == 2:
                history_started.resolve()
                await history_gate

    fixture = await peer(history=history)
    try:
        sub = fixture.client(realtime={"handler_timeout_ms": 2000}).realtime.subscribe(CHANNEL)
        seen = capture(sub)
        calls = 0
        active = 0
        maximum_active = 0

        async def on_resync(_: Any) -> None:
            nonlocal calls, active, maximum_active
            calls += 1
            invocation = calls
            active += 1
            maximum_active = max(maximum_active, active)
            try:
                if invocation == 1:
                    callback_started.resolve()
                    await callback_gate
            finally:
                active -= 1

        sub.on("resync", on_resync)
        await sub.ready
        await fixture.send({"type": "resync", "channel_id": CHANNEL})
        await asyncio.gather(callback_started, history_started)
        for _ in range(100):
            await fixture.send({"type": "resync", "channel_id": CHANNEL})
        await barrier()
        assert calls == 1
        assert maximum_active == 1
        callback_gate.resolve()
        await until(lambda: calls == 2, "one coalesced pending resync notification")
        await sleep(25)
        assert calls == 2
        assert maximum_active == 1
        history_gate.resolve()
        await until(lambda: len(seen["recovery_complete"]) == 3 and sub.status == "live", "bounded recovery after notification flood")
    finally:
        history_gate.resolve()
        callback_gate.resolve()


async def test_client_shutdown_cancels_active_control_dispatch_and_discards_pending_notifications(peer: Peer) -> None:
    fixture = await peer()
    client = fixture.client(realtime={"handler_timeout_ms": 2000})
    sub = client.realtime.subscribe(CHANNEL)
    callback_gate = Deferred()
    callback_started = Deferred()
    first_listener_calls = 0
    later_listener_calls = 0
    handler_errors = 0

    async def first_listener(_: Any) -> None:
        nonlocal first_listener_calls
        first_listener_calls += 1
        callback_started.resolve()
        await callback_gate

    def later_listener(_: Any) -> None:
        nonlocal later_listener_calls
        later_listener_calls += 1

    def on_handler_error(_: Any) -> None:
        nonlocal handler_errors
        handler_errors += 1

    try:
        sub.on("resync", first_listener)
        sub.on("resync", later_listener)
        sub.on("handler_error", on_handler_error)
        await sub.ready
        await sub.resync()
        await callback_started
        await sub.resync()
        await client.close()
        callback_gate.resolve()
        await sleep(30)
        assert sub.status == "closed"
        assert first_listener_calls == 1, "The pending notification must be discarded on close"
        assert later_listener_calls == 0, "Closing must abort remaining listeners in the active dispatch"
        assert handler_errors == 0, "Cancelling a control handler is not an application failure"
        assert client.realtime.get_stats().subscriptions == 0
    finally:
        callback_gate.resolve()


async def test_a_resync_requested_during_the_final_buffered_drain_schedules_another_recovery(peer: Peer) -> None:
    history_gate = Deferred()
    history_started = Deferred()
    handler_gate = Deferred()
    handler_started = Deferred()
    root_reads = 0

    async def history(scope: dict[str, Any]) -> None:
        nonlocal root_reads
        if scope["thread"] is None:
            root_reads += 1
            if root_reads == 2:
                history_started.resolve()
                await history_gate

    fixture = await peer(history=history)
    try:
        recovery = {"mode": "best_effort", "max_threads": 0, "max_state_refresh": 0}
        sub = fixture.client(recovery=recovery).realtime.subscribe(CHANNEL)
        seen = capture(sub)

        async def held(_: Any) -> None:
            handler_started.resolve()
            await handler_gate

        sub.on("message", held)
        await sub.ready
        await sub.resync()
        await history_started
        await fixture.send(message("buffered-before-resync-rerun"))
        await barrier()
        history_gate.resolve()
        await handler_started
        await until(lambda: len(seen["recovery_complete"]) == 2, "second recovery notified")
        assert len(seen["recovery_complete"]) == 2
        await sub.resync()
        handler_gate.resolve()
        await until(
            lambda: root_reads == 3 and len(seen["recovery_complete"]) == 3 and sub.status == "live",
            "resync requested inside the previous recovery drain",
        )
        await sleep(25)
        assert root_reads == 3
        assert len(seen["message"]) == 1
    finally:
        history_gate.resolve()
        handler_gate.resolve()
