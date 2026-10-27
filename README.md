# HumChat Python SDK — durable delivery preview

`humchat-sdk` (import name `humchat`) is an async-first backend Python SDK for sending and receiving HumChat messages. It is a port of the Node SDK [`@humchat/node`](https://github.com/S7-Works/humchat-node-sdk) `0.1.0-alpha.8` with the same capabilities and behaviour: HumChat SDK-key login with automatic renewal, recipient lookup, direct-message helpers, a managed conversation inbox, optional shared WebSockets, durable replay, persistent checkpoints, a send outbox, and outgoing-webhook management and verification.

Preview status: `auth={"sdk_key": ..., "act_as": ...}` logs in through HumChat, caches and renews tokens, shares acquisition across callers, verifies the member identity and backs off during outages. Caller-managed `token_provider` authentication is also supported. This version is prepared for review; it is not a production acceptance or package-index publication milestone. See the [login guide](docs/SDK_KEY_LOGIN_GUIDE.md), [release gates](docs/RELEASING.md) and the [Node-to-Python mapping](docs/MIGRATION.md#from-the-node-sdk).

The source is public at [S7-Works/humchat-python-sdk](https://github.com/S7-Works/humchat-python-sdk). The package is **not published to PyPI**: its classifiers include `Private :: Do Not Upload`, so index uploads are refused. Licensing is pending: [LICENSE](LICENSE) remains `UNLICENSED`, with no open-source license granted.

Requires Python 3.10 or later and a compatible HumChat server with `/api/sdk/capabilities`, send receipts v1, message state v1, and history v2. Runtime dependencies are [`httpx`](https://www.python-httpx.org/) and [`websockets`](https://websockets.readthedocs.io/). The package ships `py.typed` inline type information. This repository contains the SDK; it does not include the HumChat server, and the SDK has **no built-in server address**: `base_url`, `org_id` and credentials are always supplied by your application.

## Build and install locally

From the root of this repository:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
```

To install into another backend, build a wheel and install that reviewed file:

```powershell
python -m pip wheel . --no-deps -w dist
python -m pip install dist\humchat_sdk-0.1.0a1-py3-none-any.whl
```

Begin with the [runnable examples](examples/README.md). The repository includes [authentication](docs/AUTHENTICATION.md), [reliability](docs/RELIABILITY.md), [shared connections](docs/MULTIPLEX.md), [direct messages](docs/DIRECT_MESSAGES.md), [webhooks](docs/WEBHOOKS.md), [API reference](docs/API.md), [migration](docs/MIGRATION.md), [troubleshooting](docs/TROUBLESHOOTING.md), [support](docs/SUPPORT.md), and [release preparation](docs/RELEASING.md) guides.

## Configure and send

For delegated member login, use an SDK key created by your organization administrator in HumChat:

```python
import asyncio
import os

from humchat import HumChat


async def main() -> None:
    async with HumChat(
        org_id=os.environ["HUMCHAT_ORG_ID"],
        base_url=os.environ["HUMCHAT_BASE_URL"],
        auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]},
    ) as humchat:
        await humchat.connect()
        await humchat.messages.send_direct(recipient_id=os.environ["HUMCHAT_RECIPIENT_ID"], content="Hello from my backend.")


asyncio.run(main())
```

The SDK contacts only `base_url`. Keep the key server-side; it can act as members of its organization. Use messaging, channels, history and sockets with these delegated credentials; admin authorization must be enforced by the server. The following advanced example uses the caller-managed `token_provider` mode instead. Configure exactly one mode.

For person-to-person messaging, use `members.list()`, `conversations.open_direct(member_id=...)`, or `messages.send_direct(recipient_id=..., content=...)`. `inbox.watch(handler, consumer_id=..., checkpoint_store=...)` discovers conversations belonging to the authenticated identity and consumes retained/new events with per-channel checkpoints. It includes new DMs and groups, with polling and explicit limits. See the [direct-message guide](docs/DIRECT_MESSAGES.md) and [runnable responder](examples/dm_responder.py). The lower-level channel API below remains available.

```python
import os

from humchat import ConflictError, HumChat, HumChatError, TokenProviderContext, TokenResult


async def token_provider(context: TokenProviderContext) -> TokenResult:
    # Obtain or renew a caller-managed session here. Expiry is Unix seconds.
    # The SDK never stores or exchanges a refresh token.
    return TokenResult(token=os.environ["HUMCHAT_TOKEN"])


async def main() -> None:
    humchat = HumChat(
        org_id=os.environ["HUMCHAT_ORG_ID"],
        base_url=os.environ["HUMCHAT_BASE_URL"],
        token_provider=token_provider,
        timeout_ms=10_000,
    )
    try:
        me = await humchat.connect()
        listing = await humchat.channels.list()
        channel = next((c for c in listing.channels if c.slug == "general"), None)
        if channel is None:
            raise SystemExit("Choose an accessible channel")
        sent = await humchat.messages.send(channel_id=channel.id, content="Build completed.")
        state = await humchat.messages.get_state(channel_id=channel.id, message_id=sent.id)
        await humchat.messages.reply(state, content="Tests passed too.")
        await humchat.messages.set_reaction(channel_id=channel.id, message_id=sent.id, emoji="✅", present=True)
        print({"actor_id": me.actor_id, "message_id": sent.id, "status": sent.status})
    except ConflictError:
        ...  # A stale revision or a reused key with different intent requires a caller decision.
    except HumChatError as error:
        print(error.code, error.status, error.message_id)
    finally:
        await humchat.close()
```

The environment-token example is static: it cannot renew an expired token. A long-running backend must implement renewal inside `token_provider`. Its context contains `reason` (`initial`, `expiring`, or `unauthorized`), an optional `previous.expires_at`, and a shared acquisition `signal`. Return `TokenResult(token, expires_at=None)` (or a mapping with those keys). The provider may be async or sync. The SDK also uses the server's verified expiry when available. Tokens are supplied as Bearer headers; redirects are never followed.

`get_config()` returns a credential-free copy; `get_capabilities()` returns normalized server capability metadata. Returned identity and capability objects can be safely modified by the caller without changing the client's bound identity.

`base_url` is required so each client explicitly chooses its HumChat server. It must be an HTTP(S) origin, without credentials, a path, query, or fragment. Use HTTPS for a deployed server. Top-level `api_key` is rejected with `UnsupportedFeatureError`; delegated member login uses the separate `auth` configuration above.

## Durable consumption and restart recovery

Durable recovery is the default. It requires the server capability `durable_replay: true` and event protocol v1. An older server fails `subscription.ready` with `UnsupportedFeatureError`; choose `recovery={"mode": "best_effort"}` explicitly to use compatibility recovery.

```python
from humchat import FileCheckpointStore, FileOutboxStore, HumChat

checkpoints = await FileCheckpointStore.open("./private-state/checkpoints.json")
outbox = await FileOutboxStore.open("./private-state/outbox.json")
consumer_id = "status-responder-v1"
humchat = HumChat(org_id=..., base_url=..., token_provider=token_provider, recovery={"mode": "durable"})
identity = await humchat.connect()
sub = humchat.realtime.subscribe(os.environ["HUMCHAT_CHANNEL_ID"], start="now")
sub.on("error", lambda error: print(error.code))


async def consume(event, context):
    if event.kind != "message" or event.state.sender_id == identity.actor_id:
        return
    if event.state.body.strip() != "!status":
        return
    await humchat.outbox.send(
        consumer_id=consumer_id,
        key=event.event_id,
        store=outbox,
        message={
            "channel_id": event.channel_id,
            "content": "All systems operational.",
            "thread_root_id": event.state.thread_root_id or event.state.id,
        },
        signal=context.signal,
    )


sub.consume(consume, consumer_id=consumer_id, checkpoint_store=checkpoints)
await sub.ready
# At application shutdown, close the client before its caller-owned stores:
# await humchat.close(); await checkpoints.close(); await outbox.close()
```

Register `consume()` synchronously after subscribing, before awaiting any other work. One acknowledged consumer is supported per canonical channel; additional handles may observe events. `start` is `"now"` by default, `"earliest"` for the retained log, or `EventCursor(cursor)` for an explicit event position. An existing checkpoint takes precedence. On first use, the start position is persisted before any callback; a restart cannot silently establish a later start point.

The consumer receives a `DurableEvent` (`event_id`, `actor_id`, `sequence`, `cursor`, `kind`, `state`, plus `org_id`/`channel_id`/`epoch`) and a `ConsumerContext` (`signal`, `source`). Kinds are `message`, `edit`, `delete` and `reaction`; restore/undo produce the corresponding state transition. Sequence values are decimal **strings**, matching the Node SDK and the wire. `state` is the immutable snapshot at that event, including the full body and original thread root. Do not replace it with a latest-state fetch when processing historical transitions.

Events run in committed channel order. A checkpoint advances only after awaited observers, the consumer callback, and the checkpoint compare-and-set succeed. Failure blocks the channel without acknowledging that event. Close its handles and create a new subscription using the same consumer/store to retry. A disconnected generation reloads the persisted checkpoint. An `event` observer receives the same envelope but does not itself provide a persistent checkpoint. Normal `message`/mutation events remain available; use `consume()` for acknowledged work.

An interrupted `consume()` invocation receives an aborted `context.signal`. Automatic reconnect waits for that application's coroutine to settle before replaying the event; if it ignores cancellation past its original handler deadline, the channel blocks. Cancellation cannot undo external side effects. Closing a client or manually creating a replacement does not forcibly stop application coroutines; settle or reconcile those effects before restarting ownership. Observational `on()` callbacks do not have the acknowledged consumer's cross-connection settlement barrier.

Delivery is **at least once while the cursor is retained**, not exactly once for arbitrary external effects. A crash after a callback's side effect but before its checkpoint can repeat that callback. Use `event_id` as an application inbox key. `outbox.send()` binds a stable key to exact message intent and persists the generated message/idempotency IDs before sending; it marks completion before returning. Reusing a key with different intent raises `ConflictError`. `outbox.flush(consumer_id=..., channel_id=..., store=...)` retries pending sends after restart. It sends sequentially and stops at the first failure. The checkpoint and outbox files are separate transactions; the responder's safe order is persist/send/record reply, then acknowledge input. Server idempotency-record retention remains unverified.

`events.sync(channel_id=..., after=..., through=None, limit=None)` exposes authenticated forward replay directly. Continue with `next_cursor`, keeping the first page's `through_cursor` to retain a fixed committed watermark. History cursors are incompatible. `RecoveryGapError` includes `earliest_cursor`/`earliest_sequence` when a cursor expired or its epoch changed. The SDK stops; it never skips to the new floor automatically. Restoring state cannot recreate expired business events, so the application must explicitly choose how to resume.

The server logs writes committed after its event-log migration; older message history is not backfilled. Retention is server-configured. A finished durable catch-up emits `recovery_complete` with `mode="durable"`, `complete=True` and `cursor`; this covers the captured log watermark, not future events or pre-migration history. The worker negotiates a ready watermark over its channel socket, then reads bounded HTTP event pages. WebSocket events act as progress hints, with periodic HTTP checks to survive missed notifications. Large backlogs are processed page by page; `recovery.max_bytes` bounds a decoded page and every callback has `handler_timeout_ms`.

`MemoryCheckpointStore`/`MemoryOutboxStore` are process-local alternatives. File stores serialize operations, enforce compare-and-set, validate content, fsync a temporary file and replace the destination atomically with `os.replace`. They hold an exclusive sibling `.lock` for one process and default to 64 MiB per store (`max_bytes=`). On a process crash, inspect the stale lock and confirm its owner is gone before manually removing it; locks are never automatically stolen. Close stores explicitly. Store files contain message content and should be in a private persistent directory. **The file format is the same as the Node SDK's** (camelCase fields, identical scope hashes), so a Python worker can open a store written by a Node worker once that process has released its lock. Custom adapters must make compare-and-set atomic, bound their own I/O, and preserve the full server/org/actor/consumer/channel scope.

## Optional shared connection

Set `realtime={"transport": "multiplex"}` to receive multiple channels through one socket on an upgraded server. Durable replay/checkpoints remain independent per channel. The default is `"channel"`; unsupported multiplex capability fails explicitly. See the [shared connection guide](docs/MULTIPLEX.md) and [runnable worker](examples/multiplex_worker.py).

## Compatibility listener and thread reply

```python
humchat = HumChat(org_id=..., base_url=..., token_provider=token_provider, recovery={"mode": "best_effort"})
identity = await humchat.connect()
subscription = humchat.realtime.subscribe(os.environ["HUMCHAT_CHANNEL_ID"])
subscription.on("error", lambda error: print(error.code))
subscription.on("recovery_incomplete", lambda coverage: print(coverage.limitations))


async def on_message(event):
    message = event.message
    if message.sender_id == identity.actor_id or event.source != "live":
        return
    if message.is_long:
        body = (await humchat.messages.get_state(channel_id=message.channel_id, message_id=message.id)).body
    else:
        body = message.content
    if body.strip() == "!status":
        await humchat.messages.reply(message, content="All systems operational.")


subscription.on("message", on_message)
await subscription.ready
```

Install listeners before awaiting `ready`. It resolves once the initial bounded recovery attempt and buffered deliveries finish. It does not certify complete historical coverage. Initial history emits `state_reconciled`, so an old command is not delivered as a new command on startup.

| API | Behavior |
| --- | --- |
| `realtime.subscribe(channel_id, start=...)` | Immediate `ChannelSubscription` with `ready`, `status`, `on`/`off`, and shared handles per channel. Must be called inside a running event loop. |
| `realtime.subscribe_many(channel_ids)` | Per-input `SubscriptionResult(channel_id, subscription)` or `SubscriptionResult(channel_id, error)`. |
| `subscription.typing()` | Sends a typing signal while live. |
| `subscription.mark_read(through=...)` | Explicit read watermark in Unix seconds. |
| `subscription.resync()` | Schedules event-log replay (durable) or bounded snapshot reconciliation (best effort). Observe `recovery_complete` for completion. Safe to call inside a handler. |
| `subscription.close()` | Removes this observer; removes the logical channel when its last observer closes. |
| `realtime.get_stats()` | `RealtimeStats` with socket/channel/subscription counts, queued events/bytes, reconnects, recoveries, auth-refresh failures, dropped ephemeral events. |
| `humchat.close()` | Cancels HTTP work, channel sockets, recovery and timers; `async with HumChat(...)` does the same on exit. |

Events are `event`, `checkpoint`, `message`, `message_updated`, `message_deleted`, `reaction_updated`, `state_reconciled`, `typing`, `presence`, `receipt`, `call_invite`, `connection`, `resync`, `recovery_incomplete`, `recovery_complete`, `session_revoked`, `auth_required`, `error`, `handler_error`, and `unknown_event`. Listeners may be sync or async. Message events receive `MessageEvent(message, source, replayed)`; mutation/reconciliation events receive `StateEvent(state, source)`. See the [API reference](docs/API.md) for every payload.

Application deliveries are awaited in order per channel. Different channels run independently. A failed or timed-out application handler blocks that channel and emits `handler_error`. Control notifications such as `connection`/`error` are observational: each kind has at most one active dispatch and one latest pending value per handle, so a slow observer may skip intermediate notifications. Read `subscription.status` for the terminal state.

## Compatibility recovery and shared real-time limits

Best-effort recovery (`recovery={"mode": "best_effort"}`) emits `recovery_incomplete` with reasons and `recovery_complete` with **`complete=False`** for every finished attempt. The SDK opens the socket before scanning history v2, buffers live frames, reconciles root messages and recently known threads, refreshes bounded cached states, and drains buffered events. This is not a durable event stream or an exactly-once consumer.

Transport outages and HTTP upgrade 408/429/502/503/504 retry with exponential full jitter until closed or blocked; Retry-After is honored and a stable connection resets the backoff after 30 seconds. Ping/pong detects half-open sockets. One failed 401 upgrade may refresh; repeated 401 and channel 403/404 stop that channel. A `session_revoked` frame is terminal unless it carries exactly `code: "TOKEN_EXPIRED"` and that code was advertised by the verified session's capabilities (`socket_session_end_codes`). Only that negotiated expiry invalidates the old token and reconnects through normal verified acquisition.

| Setting (`realtime=` / `recovery=`) | Default |
| --- | --- |
| `max_channels`, `connect_concurrency` | 50 channels, 5 simultaneous authenticated handshakes. |
| `max_buffered_events`, `max_buffered_bytes` | 1,000 events / 16 MiB across the client. |
| `max_payload_bytes` | 10 MiB per WebSocket frame. |
| `max_cached_messages`, `max_cached_bytes` | 1,000 entries / 8 MiB per channel; LRU eviction. |
| `reconnect_initial_delay_ms`, `reconnect_max_delay_ms` | 500 ms / 30 seconds. |
| `ping_interval_ms`, `pong_timeout_ms`, `handler_timeout_ms` | 30 / 10 / 30 seconds. |
| `recovery.max_pages`, `max_threads` | 3 pages per root/thread scan, 20 recently observed roots. |
| `recovery.max_bytes`, `max_state_refresh`, `timeout_ms` | 16 MiB, 50 cached states, 30-second recovery deadline. |

## HTTP methods

All resource methods accept optional keyword arguments `signal` (a `humchat.AbortSignal`) and `timeout_ms`. Ordinary asyncio cancellation also works. IDs are opaque but must use the state protocol's identifier grammar: ASCII letters, digits, `.`, `_`, `:`, or `-`, excluding `.` and `..` alone. Message, thread, and operation IDs are 1–128 characters; channel keys allow 1–1024 and actor IDs 1–256. Generated outgoing IDs are monotonic ULIDs. Times are Unix seconds.

| Method | Result / important behavior |
| --- | --- |
| `connect()` | Validates `/me`, organization, actor, and required protocol capabilities; also called lazily. |
| `channels.list(team_id=None)` | `ChannelList` with `channels`, `org_id`, `team_id`, `team_name`. |
| `conversations.list()` | Accessible conversations; `channel_id` comes from `channel_key`, distinct from management `id`. |
| `messages.send(channel_id=, content=, id=None, thread_root_id=None, priority=None, idempotency_key=None)` | Acceptance receipt, not a complete message or a read receipt. |
| `messages.reply(message, content=, id=None, priority=None)` | Reuses the channel and original thread root, including when replying to a reply. |
| `messages.get_state(channel_id=, message_id=)` | Validated state, original/current body, revisions, deletion flag and attachments. |
| `messages.history(channel_id=, thread_root_id=None, cursor=None, limit=None)` | One history v2 page. |
| `messages.iterate_history(...)` | Async iterator of pages from newest page toward older pages; rows within each page are ascending. |
| `messages.edit(channel_id=, message_id=, content=, expected_revision=, operation_id=None)` | Author-only edit with optimistic concurrency. |
| `messages.delete(channel_id=, message_id=, expected_revision=, operation_id=None)` | Author-only tombstone with optimistic concurrency. |
| `messages.set_reaction(channel_id=, message_id=, emoji=, present=, operation_id=None)` | Sets desired reaction membership; never toggles. |
| `close()` | Cancels this client's requests/waits; does not log out the shared user session. |

## Retries, errors, and identity

Defaults are a 10-second timeout per HTTP attempt, 3 retries, full jitter from 250 ms up to 2 seconds, and a 30-second total request deadline (`retry={"max_retries", "initial_delay_ms", "max_delay_ms", "total_timeout_ms"}`). GETs and stable send/mutation intents retry transport failures, attempt timeouts, and HTTP 408/429/502/503/504. Other 4xx errors and generic 500 are not automatically retried. `Retry-After` is honored within the deadline, with a shared cooldown for 429. One 401 can request a new token; a second 401 ends that operation. Every newly acquired token is checked through `/me` and capability discovery before a write can use it. A changed organization or actor blocks the client and requires a new instance.

IDs, idempotency keys, operation IDs, and the serialized body remain unchanged during retries. An exhausted send can have been committed: use its error's `message_id` and `idempotency_key` to retry the **same original intent**, or inspect state.

Errors extend `HumChatError`, with stable `code`, `kind`, `retryable`, optional `status`/`request_id`, and available intent metadata (`channel_id`, `message_id`, `operation_id`, `idempotency_key`). Categories: configuration, validation, authentication, permission, not-found, conflict, rate-limit, network, timeout, abort, server, protocol, unsupported-feature, session-revoked, backpressure, recovery-gap, persistence and webhook-verification. `humchat.PermissionError` and `humchat.TimeoutError` intentionally reuse the Node names; import them from `humchat` (they are not the Python built-ins). Raw server bodies and transport/provider error text are omitted. Responses are capped at 10 MiB. There is no default logger.

## Tests and examples

```powershell
.venv\Scripts\python -m pytest
.venv\Scripts\ruff check .
.venv\Scripts\mypy
```

The suite ports every Node unit, controlled-peer integration, release-policy and installed-package test. It uses synthetic identities and local HTTP/WebSocket peers; it needs no production credentials. A live acceptance runner is opt-in and reads credentials only from environment variables; see [SDK-key message acceptance](docs/SDK_KEY_MESSAGE_ACCEPTANCE.md).

Runnable examples live under `examples/`. Set `HUMCHAT_BASE_URL`, `HUMCHAT_ORG_ID`, and either `HUMCHAT_TOKEN` or `HUMCHAT_SDK_KEY` + `HUMCHAT_ACT_AS`, plus the variables documented in each example.

## Outgoing webhooks

Use `client.webhooks` to manage administrator-owned workspace-channel subscriptions on a configured server, and `verify_webhook()` to authenticate exact raw request bytes at your receiver. Read the [webhook guide](docs/WEBHOOKS.md).
