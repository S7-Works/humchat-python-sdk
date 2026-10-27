"""Opt-in SDK-key direct-message acceptance against a real HumChat deployment.

Port of test/acceptance/sdk-key-direct-messaging.mjs. It creates two persistent, labeled
messages (a DM and a thread reply) between two designated members, verifies inbox delivery
in real time or durable recovery, and reads both back from both members' histories.

Configuration comes only from the process environment (never arguments):

  HUMCHAT_BASE_URL               explicit HumChat origin (no path, credentials, query, fragment)
  HUMCHAT_ORG_ID                 organization UUID
  HUMCHAT_SDK_KEY                private hsk_ credential (never printed)
  HUMCHAT_ACT_AS                 designated sender's member email
  HUMCHAT_TEST_RECIPIENT_EMAIL   designated recipient's different member email
  HUMCHAT_ACCEPTANCE_SEND        must be exactly "true" (opt-in to persistent writes)
  HUMCHAT_ACCEPTANCE_TIMEOUT_MS  optional total deadline, 10000-600000 (default 180000)

Run from the repository root:  python tests/acceptance/sdk_key_direct_messaging.py
It prints one sanitized JSON report (status, fixed check names, counts, error category) and
exits 0 only when every check passes. Integration tests import ``run_acceptance`` only
against synthetic loopback peers.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import uuid
from collections.abc import Awaitable, Mapping
from typing import Any, TypeVar
from urllib.parse import urlsplit

import httpx

from humchat import AbortController, HumChat, HumChatError, MemoryCheckpointStore

T = TypeVar("T")
SDK_CODES = {
    "CONFIGURATION", "VALIDATION", "AUTHENTICATION", "SESSION_REVOKED", "PERMISSION", "NOT_FOUND", "CONFLICT",
    "RATE_LIMIT", "NETWORK", "TIMEOUT", "ABORT", "SERVER", "PROTOCOL", "UNSUPPORTED_FEATURE", "BACKPRESSURE",
    "RECOVERY_GAP", "PERSISTENCE",
}  # fmt: skip
ALLOWED_GETS = {"/me", "/api/sdk/capabilities", "/api/org/members", "/api/conversations", "/api/messages/sync", "/api/events/sync"}
_ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


class AcceptanceFailure(Exception):
    def __init__(self, code: str) -> None:
        super().__init__("Acceptance check failed")
        self.acceptance_code = code


def require(condition: bool, code: str) -> None:
    if not condition:
        raise AcceptanceFailure(code)


def normalize_email(value: str) -> str:
    return "".join(ch.lower() if "A" <= ch <= "Z" else ch for ch in value.strip())


def required(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    require(isinstance(value, str) and len(value.strip()) > 0, "MISSING_CONFIGURATION")
    assert value is not None
    return value


def safe_failure(error: BaseException) -> dict[str, Any]:
    if isinstance(error, AcceptanceFailure):
        return {"code": error.acceptance_code}
    code = getattr(error, "code", None)
    result: dict[str, Any] = {"code": code if code in SDK_CODES else "UNEXPECTED_FAILURE"}
    status = getattr(error, "status", None)
    if isinstance(status, int) and not isinstance(status, bool) and 400 <= status <= 599:
        result["status"] = status
    return result


def new_ulid() -> str:
    import time

    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    return "".join(_ULID_ALPHABET[(value >> (5 * index)) & 31] for index in reversed(range(26)))


class GuardedTransport(httpx.AsyncBaseTransport):
    """Every request stays on the configured origin and on the expected routes."""

    def __init__(self, origin: str, state: dict[str, bool]) -> None:
        self.inner = httpx.AsyncHTTPTransport()
        self.origin = origin
        self.state = state

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        port = f":{url.port}" if url.port is not None else ""
        require(f"{url.scheme}://{url.host}{port}" == self.origin and not url.username and not url.password, "ORIGIN_MISMATCH")
        method, path = request.method.upper(), url.path
        login = method == "POST" and path == "/api/sdk/auth/user-token"
        write = method == "POST" and path in ("/api/conversations", "/api/messages/send")
        require(login or (method == "GET" and path in ALLOWED_GETS) or (write and self.state["writes_allowed"]), "UNEXPECTED_REQUEST")
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()


def _origin(base_url: str) -> str:
    try:
        parts = urlsplit(base_url)
        port = parts.port
    except ValueError:
        raise AcceptanceFailure("INVALID_ORIGIN") from None
    require(
        parts.scheme in ("https", "http") and not parts.username and not parts.password and not parts.query and not parts.fragment
        and parts.path in ("", "/") and bool(parts.hostname),
        "INVALID_ORIGIN",
    )  # fmt: skip
    default = {"http": 80, "https": 443}[parts.scheme]
    return f"{parts.scheme}://{parts.hostname}{'' if port in (None, default) else f':{port}'}"


async def run_acceptance(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return a sanitized report. Never passes credentials, identities or bodies to a logger."""
    env = os.environ if env is None else env
    report: dict[str, Any] = {
        "passed": False, "checks": [], "message_send_attempts": 0, "accepted_messages": 0, "receiver_deliveries": 0,
        "sender_deliveries": 0, "receiver_live_deliveries": 0, "receiver_recovery_deliveries": 0, "sender_live_deliveries": 0,
        "sender_recovery_deliveries": 0, "history_matches": 0,
    }  # fmt: skip
    clients: list[HumChat] = []
    stores: list[MemoryCheckpointStore] = []
    stop = AbortController()
    loop = asyncio.get_running_loop()
    stopped: asyncio.Future[None] = loop.create_future()
    timer: asyncio.TimerHandle | None = None
    previous_handlers: dict[int, Any] = {}

    def abort(reason: BaseException) -> None:
        if not stop.signal.aborted:
            stop.abort(reason)
        if not stopped.done():
            stopped.set_exception(reason)
            stopped.exception()

    async def wait(awaitable: Awaitable[T]) -> T:
        if stop.signal.aborted:
            raise stop.signal.reason
        task = asyncio.ensure_future(awaitable)
        done, _ = await asyncio.wait({task, stopped}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            return task.result()
        task.cancel()
        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        raise stop.signal.reason

    def check(name: str) -> None:
        report["checks"].append(name)

    def throw_if_aborted() -> None:
        if stop.signal.aborted:
            raise stop.signal.reason

    try:
        require(env.get("HUMCHAT_ACCEPTANCE_SEND") == "true", "OPT_IN_REQUIRED")
        base_url = required(env, "HUMCHAT_BASE_URL")
        origin = _origin(base_url)
        org_id = required(env, "HUMCHAT_ORG_ID").lower()
        sdk_key = required(env, "HUMCHAT_SDK_KEY")
        sender_email = normalize_email(required(env, "HUMCHAT_ACT_AS"))
        recipient_email = normalize_email(required(env, "HUMCHAT_TEST_RECIPIENT_EMAIL"))
        require(sender_email != recipient_email, "DISTINCT_RECIPIENT_REQUIRED")
        raw_deadline = env.get("HUMCHAT_ACCEPTANCE_TIMEOUT_MS")
        try:
            duration = 180_000 if raw_deadline is None else int(raw_deadline, 10)
        except ValueError:
            duration = -1
        require(10_000 <= duration <= 600_000, "INVALID_DEADLINE")
        timer = loop.call_later(duration / 1000, lambda: abort(AcceptanceFailure("DEADLINE_EXCEEDED")))
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                previous_handlers[signum] = signal.signal(
                    signum, lambda *_: loop.call_soon_threadsafe(abort, AcceptanceFailure("CANCELLED"))
                )
            except (ValueError, OSError):
                pass
        state = {"writes_allowed": False}
        for act_as in (sender_email, recipient_email):
            clients.append(
                HumChat(
                    base_url=base_url, org_id=org_id, auth={"sdk_key": sdk_key, "act_as": act_as}, transport=GuardedTransport(origin, state),
                    recovery={"mode": "durable"}, realtime={"max_channels": 50}, retry={"total_timeout_ms": min(duration, 30_000)},
                )
            )
        sender, receiver = clients
        options = {"signal": stop.signal}
        sender_identity, receiver_identity = await wait(asyncio.gather(*(client.connect(**options) for client in clients)))
        require(
            sender_identity.org_id == org_id and receiver_identity.org_id == org_id
            and normalize_email(sender_identity.email) == sender_email and normalize_email(receiver_identity.email) == recipient_email
            and sender_identity.actor_id != receiver_identity.actor_id,
            "IDENTITY_MISMATCH",
        )  # fmt: skip
        check("both_verified_member_identities")
        capabilities = await wait(asyncio.gather(*(client.get_capabilities(**options) for client in clients)))
        for capability in capabilities:
            tokens = capability.authentication.sdk_key_user_tokens
            messaging = capability.messaging
            require(
                tokens is not None and tokens.configured is True and capability.authentication.user_sessions
                and 1 in messaging.send_receipt_versions and 1 in messaging.message_state_versions and 2 in messaging.history_versions
                and messaging.durable_replay and 1 in messaging.event_protocol_versions,
                "CAPABILITY_MISMATCH",
            )  # fmt: skip
        check("both_messaging_capabilities")
        rosters = await wait(asyncio.gather(*(client.members.list(**options) for client in clients)))
        for roster in rosters:
            for identity in (sender_identity, receiver_identity):
                matches = [member for member in roster if normalize_email(member.email) == normalize_email(identity.email)]
                require(len(matches) == 1 and matches[0].actor_id == identity.actor_id and matches[0].status == "active", "ROSTER_MISMATCH")
        check("both_rosters_identify_exact_active_participants")
        listings = await wait(asyncio.gather(*(client.conversations.list(**options) for client in clients)))
        for identity, conversations in zip((sender_identity, receiver_identity), listings, strict=True):
            require(len(conversations) < 50, "INBOX_CAPACITY_EXCEEDED")
            for conversation in conversations:
                actors = [member.actor_id for member in conversation.members]
                require(
                    conversation.channel_id.startswith(f"{org_id}:conv:") and conversation.member_count == len(actors)
                    and len(set(actors)) == len(actors) and identity.actor_id in actors,
                    "CONVERSATION_SCOPE_MISMATCH",
                )  # fmt: skip
        check("both_conversation_scopes_and_capacity")

        root_id, reply_id, run_label = new_ulid(), new_ulid(), str(uuid.uuid4())
        root_body = f"[HumChat SDK acceptance {run_label}] Test 1/2: direct message. This test message is intentionally retained."
        reply_body = f"[HumChat SDK acceptance {run_label}] Test 2/2: thread reply. This test message is intentionally retained."
        root_received: asyncio.Future[Any] = loop.create_future()
        reply_received: asyncio.Future[Any] = loop.create_future()
        expected: dict[str, str | None] = {"channel": None}

        def matches_message(message: Any, identifier: str, actor_id: str, body: str, thread_root_id: str | None, channel_id: str | None) -> bool:
            return (
                message.id == identifier and message.org_id == org_id and message.sender_id == actor_id and not message.deleted
                and message.body == body and message.thread_root_id == thread_root_id
                and (channel_id is None or message.channel_id == channel_id)
            )  # fmt: skip

        def handler(for_receiver: bool) -> Any:
            def handle(event: Any, context: Any) -> None:
                if event.kind != "message" or event.state.id != (root_id if for_receiver else reply_id):
                    return
                valid = (
                    event.org_id == org_id and event.channel_id == event.state.channel_id and event.channel_id.startswith(f"{org_id}:conv:")
                    and matches_message(
                        event.state, root_id if for_receiver else reply_id,
                        sender_identity.actor_id if for_receiver else receiver_identity.actor_id,
                        root_body if for_receiver else reply_body, None if for_receiver else root_id, expected["channel"],
                    )
                )  # fmt: skip
                if not valid:
                    abort(AcceptanceFailure("DELIVERY_MISMATCH"))
                    return
                side = "receiver" if for_receiver else "sender"
                report[f"{side}_deliveries"] += 1
                report[f"{side}_{'live' if context.source == 'live' else 'recovery'}_deliveries"] += 1
                target = root_received if for_receiver else reply_received
                if not target.done():
                    target.set_result(event.state)

            return handle

        stores.extend([MemoryCheckpointStore(), MemoryCheckpointStore()])
        watchers = [
            client.inbox.watch(handler(index == 1), consumer_id=f"acceptance-{run_label}-{index}", checkpoint_store=stores[index],
                               poll_interval_ms=1_000, max_conversations=50)
            for index, client in enumerate(clients)
        ]

        def watch_done(future: asyncio.Future[None]) -> None:
            if not future.cancelled() and future.exception() is not None:
                abort(future.exception())  # type: ignore[arg-type]

        for watcher in watchers:
            watcher.done.add_done_callback(watch_done)
        await wait(asyncio.gather(*(watcher.ready for watcher in watchers)))

        async def wait_for_live(channel_id: str | None = None) -> None:
            while True:
                throw_if_aborted()
                ready = True
                for watcher in watchers:
                    channels = watcher.get_stats().channels
                    live = (
                        all(item.status == "live" for item in channels)
                        if channel_id is None
                        else any(item.channel_id == channel_id and item.status == "live" for item in channels)
                    )
                    ready = ready and watcher.status == "watching" and live
                if ready:
                    return
                await asyncio.sleep(0.1)

        await wait(wait_for_live())
        check("both_inboxes_ready_before_writes")
        state["writes_allowed"] = True
        throw_if_aborted()
        report["message_send_attempts"] += 1
        direct = await wait(
            sender.messages.send_direct(recipient_id=receiver_identity.actor_id, content=root_body, id=root_id, idempotency_key=root_id, **options)
        )
        report["accepted_messages"] += 1
        expected["channel"] = direct.conversation.channel_id
        members = [member.actor_id for member in direct.conversation.members]
        require(
            direct.receipt.id == root_id and direct.receipt.channel_id == expected["channel"] and direct.conversation.member_count == 2
            and len(members) == 2 and len(set(members)) == 2 and sender_identity.actor_id in members and receiver_identity.actor_id in members,
            "DIRECT_RECEIPT_MISMATCH",
        )  # fmt: skip
        check("sender_direct_message_accepted")
        received_root = await wait(root_received)
        require(matches_message(received_root, root_id, sender_identity.actor_id, root_body, None, expected["channel"]), "DELIVERY_MISMATCH")
        check("receiver_inbox_received_direct_message")
        await wait(asyncio.gather(*(watcher.refresh() for watcher in watchers)))
        await wait(wait_for_live(expected["channel"]))
        check("both_conversation_streams_live_before_reply")
        throw_if_aborted()
        report["message_send_attempts"] += 1
        reply = await wait(receiver.messages.reply(received_root, content=reply_body, id=reply_id, idempotency_key=reply_id, **options))
        report["accepted_messages"] += 1
        require(reply.id == reply_id and reply.channel_id == expected["channel"], "REPLY_RECEIPT_MISMATCH")
        await wait(reply_received)
        check("sender_inbox_received_thread_reply")

        async def find_history(client: HumChat, identifier: str, actor_id: str, body: str, thread_root_id: str | None) -> None:
            pages = 0
            assert expected["channel"] is not None
            async for page in client.messages.iterate_history(channel_id=expected["channel"], thread_root_id=thread_root_id, limit=200, **options):
                found = next((message for message in page.messages if message.id == identifier), None)
                if found is not None:
                    require(matches_message(found, identifier, actor_id, body, thread_root_id, expected["channel"]), "HISTORY_MISMATCH")
                    report["history_matches"] += 1
                    return
                pages += 1
                if pages >= 5:
                    break
            raise AcceptanceFailure("MESSAGE_ABSENT_FROM_HISTORY")

        await wait(
            asyncio.gather(
                *(
                    lookup
                    for client in clients
                    for lookup in (
                        find_history(client, root_id, sender_identity.actor_id, root_body, None),
                        find_history(client, reply_id, receiver_identity.actor_id, reply_body, root_id),
                    )
                )
            )
        )
        check("both_messages_persisted_in_both_member_histories")
        require(report["message_send_attempts"] == 2 and report["accepted_messages"] == 2 and report["history_matches"] == 4, "COUNT_MISMATCH")
        report["passed"] = True
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        report["failure"] = safe_failure(error if not isinstance(error, asyncio.CancelledError) else AcceptanceFailure("CANCELLED"))
    finally:
        if timer is not None:
            timer.cancel()
        abort(AcceptanceFailure("FINISHED"))
        closed = await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
        stores_closed = await asyncio.gather(*(store.close() for store in stores), return_exceptions=True)
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)
        if any(isinstance(result, BaseException) for result in [*closed, *stores_closed]):
            report["passed"] = False
            report["failure"] = {"code": "SHUTDOWN_FAILED"}
    return report


def main() -> int:
    if len(sys.argv) != 1:
        report: dict[str, Any] = {"passed": False, "failure": {"code": "CLI_ARGUMENTS_REFUSED"}}
    else:
        report = asyncio.run(run_acceptance())
    sys.stdout.write(json.dumps(report) + "\n")
    sys.stdout.flush()
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["HumChatError", "run_acceptance"]
