# Delivery, recovery and persistence

The default mode consumes a retained, ordered event log. With an acknowledged consumer and a durable checkpoint adapter, delivery is at least once while its cursor remains retained. Application side effects are not exactly once: a process can finish a side effect and crash before recording its input checkpoint.

This preview includes package and controlled-peer fault tests. Production deployment acceptance, representative workload tests and a sustained pilot remain **unverified**. Passing local tests is not a capacity guarantee. See [Authentication](AUTHENTICATION.md) for the credential contract and [API](API.md) for exact signatures.

## Choose a recovery mode

| Mode | Coverage and prerequisites |
| --- | --- |
| `durable` (default) | Requires server durable replay and event protocol v1. Replays every retained transition in committed order per channel. Persistent restart progress requires `consume()` plus a persistent checkpoint store. |
| `best_effort` (explicit) | Uses channel WebSockets and bounded history/state reconciliation. Unknown older threads, unseen mutations, cache eviction and restarts can leave gaps or repeat observations. It has no acknowledged consumer or durable cursor. |

If a server lacks durable capability, `await subscription.ready` raises `UnsupportedFeatureError`. The SDK does not silently downgrade. For compatibility, configure `recovery={"mode": "best_effort"}` explicitly and accept its weaker coverage.

## Acknowledged thread responder

[`examples/durable_responder.py`](../examples/durable_responder.py) is the runnable version of this pattern. Set `HUMCHAT_BASE_URL`, `HUMCHAT_ORG_ID`, `HUMCHAT_TOKEN` (or `HUMCHAT_SDK_KEY` + `HUMCHAT_ACT_AS`), `HUMCHAT_CHANNEL_ID` and `HUMCHAT_STATE_DIR`. The state directory must be private and persistent.

```python
import hashlib
import os

from humchat import FileCheckpointStore, FileOutboxStore, HumChat

checkpoints = await FileCheckpointStore.open(os.path.join(state_dir, "checkpoints.json"))
outbox = await FileOutboxStore.open(os.path.join(state_dir, "outbox.json"))
identity = await client.connect()
sub = client.realtime.subscribe(os.environ["HUMCHAT_CHANNEL_ID"], start="now")
consumer_id = "status-responder-v1"


async def consume(event, context):
    if event.kind != "message" or event.state.sender_id == identity.actor_id:
        return
    if event.state.body.strip() != "!status":
        return
    key = "reply-" + hashlib.sha256(event.event_id.encode()).hexdigest()
    await client.outbox.send(
        consumer_id=consumer_id,
        key=key,
        store=outbox,
        message={"channel_id": event.channel_id, "thread_root_id": event.state.thread_root_id or event.state.id, "content": "All systems operational."},
        signal=context.signal,
    )


# Register synchronously, before awaiting ready or any other work.
sub.consume(consume, consumer_id=consumer_id, checkpoint_store=checkpoints)
await sub.ready
```

The consumer coroutine includes the outbox write/send/receipt. Returning early or launching un-awaited work (for example `asyncio.create_task`) would acknowledge input before that work completes. No SDK can roll back an external effect that already happened.

## Starts, ordering and acknowledgements

By default one physical socket serves one canonical channel within a client; [multiplex transport](MULTIPLEX.md) shares one socket across logical channels. Multiple handles can observe a canonical channel, but only one `consume()` registration is allowed, before startup. An existing checkpoint wins over `start`. Without a checkpoint, `"now"` chooses the initial server watermark, `"earliest"` the position before the first retained event, and `EventCursor(cursor)` an explicit position. The first chosen position is persisted before callbacks begin.

Each durable transition is delivered in this order:

1. Await `event` observers.
2. Await the corresponding `message`, `message_updated`, `message_deleted` or `reaction_updated` observers.
3. Await the acknowledged consumer, if registered.
4. Atomically compare-and-set its checkpoint from the previous cursor to the event cursor.

Only then does the worker advance. A `checkpoint` notification follows a successful write. Every observer therefore needs to be safe to repeat; a failing observer can prevent the acknowledged consumer from running. Without `consume()`, the worker has only an in-memory cursor.

Ordering is per channel; different channels progress independently. An event contains the full canonical state at that transition; processing an old event by fetching today's state can erase the transition you intended to process. `event_id` is a stable event identity within its stream; include server/organization/channel scope in a shared application inbox key. Sequence values are decimal strings and are not timestamps.

Failed/timed-out delivery, failed checkpoint writes and lost checkpoint ownership block the channel without acknowledging the failed event. Automatic reconnect waits for an interrupted consumer invocation to settle before replaying it. Its original handler deadline remains active; if it ignores cancellation beyond that deadline, the channel blocks. This settlement barrier applies to the acknowledged consumer, not observational `on()` listeners or a manually created replacement client.

Managed inbox discovery also waits for interrupted consumer work and pending checkpoint storage when removing a conversation, before a later discovery can rejoin it (this is the behaviour of Node commit `44ca199`, ported here). The original handler deadline still applies; checkpoint settlement has a `retry["total_timeout_ms"]` budget. Failure blocks the watcher instead of starting a replacement over unfinished work.

After correcting a blocked failure, settle or reconcile outstanding effects, close all handles for that channel, and create a new subscription using the same consumer/store. Changing consumer identity to bypass a conflict creates a different persistence scope and can repeat work.

## Reconnect and coverage

Durable mode negotiates a ready watermark on the WebSocket and reads bounded HTTP pages from the event log. Socket events are progress hints; a five-second HTTP poll also detects a missed final hint. Replay keeps a captured upper watermark while paging, then processes later progress separately.

`recovery_complete` with `mode="durable"`, `complete=True` and `cursor` means that captured watermark was processed. It does not promise future delivery, pre-migration history, or availability after retention expires. `ready` resolves after initial recovery and deliveries complete. `resync()` schedules work and can be called inside a handler.

Transient network failures and WebSocket upgrade 408/429/502/503/504 retry using exponential full jitter and Retry-After. Reconnect continues until closed or blocked; HTTP `max_retries` is not a reconnect limit. A connection stable for 30 seconds resets the backoff. Ping/pong detects dead sockets. One failed 401 handshake can refresh; repeated denial or channel 403/404 blocks the channel. Changed credentials rotate sockets after same-principal verification. Explicit revocation ends the entire client's credential lifecycle.

SDK-key authentication uses the same shared acquisition for HTTP and every socket. Scheduled renewal failures that remain retryable enter reconnect backoff; permanent mint 401/403 is latched for the client. Revocation aborts pending renewal. Retryable acquisition failures keep a client-wide cooldown (1–2 s doubling to 15–30 s, longer `Retry-After` wins), which bounds pressure within one client, not across processes.

A server may advertise `authentication.socket_session_end_codes: ["TOKEN_EXPIRED"]`. Only with that capability does `{"type": "session_revoked", "code": "TOKEN_EXPIRED", "reason": "session expired"}` permit verified renewal. Missing, unknown or unadvertised codes remain terminal.

## Writes and uncertain outcomes

HTTP GETs and SDK send/mutation requests may retry network failures, attempt timeouts and HTTP 408/429/502/503/504. Other 5xx responses, validation failures, conflicts and permission failures are not automatically retried. HTTP 429 Retry-After also establishes a shared HTTP cooldown.

Retries preserve the serialized request, message ID/idempotency key or mutation operation ID. A timeout does not prove the server rolled back. For a manual retry after an uncertain outcome, keep the original intent and IDs (available on the error as `message_id`/`idempotency_key`/`operation_id`). Calling `send()` again without them creates a different message intent.

The outbox automates persistent send intent: it stores the exact request and generated IDs, sends it, then records the receipt before returning. Repeating the same scoped key/request reuses that intent; changed content, IDs, thread, priority or idempotency options raise `ConflictError`. Repeat the original input shape (an omitted ID is part of the stored request). `flush()` retries pending intents sequentially and stops at the first failure.

Checkpoint and outbox files are separate transactions. Safe responder order is persist/send/record the reply, then return so input can be acknowledged. Server idempotency-record retention is **unverified**.

## Retention gaps and upgrades

An expired cursor or replaced epoch raises `RecoveryGapError`, with `earliest_cursor`/`earliest_sequence` when available, and stops consumption. The SDK does not silently jump to the new floor. To resume, stop the owner, settle effects, reconcile missing work and make an explicit, application-owned checkpoint decision. Merely subscribing with `start="earliest"` does not override an existing checkpoint.

## Store ownership and limits

Persistence scope is the tuple of server origin, organization, verified actor, consumer ID and channel. Keep all five in custom adapters. Checkpoint compare-and-set must be atomic. It detects conflicting progress but does not prevent two owners from starting the same external effect; distributed workers additionally need an application lease. Bound adapter I/O yourself.

Memory stores lose progress on restart. File stores validate their document, serialize operations (an `asyncio.Lock` per store), fsync a temporary file and atomically replace the destination with `os.replace`. They hold an exclusive sibling `.lock` for their lifetime; locks are never automatically stolen. Close the client, settle application work, then close caller-owned stores. Store files contain message content and are not encrypted by the SDK; use a private directory. The 64 MiB default is per file. Network filesystems, abrupt power loss and multi-host ownership are **unverified**.

## Channels, buffers and compatibility mode

| Setting | Default / scope |
| --- | --- |
| `realtime["max_channels"]` | 50 canonical channels per client; extra admission raises `BackpressureError`, without evicting existing channels. |
| `realtime["connect_concurrency"]` | 5 simultaneous channel admissions. Durable mode releases a slot before checkpoint/replay work; best effort holds it through initial recovery. |
| `max_buffered_events`, `max_buffered_bytes` | 1,000 events / 16 MiB shared across channels, including in-flight delivery. |
| `max_payload_bytes` | 10 MiB per WebSocket frame; HTTP bodies have a separate fixed 10 MiB SDK limit. |
| `max_cached_messages`, `max_cached_bytes` | Best-effort state cache: 1,000 messages / 8 MiB per channel, with LRU eviction. |
| `reconnect_initial_delay_ms`, `reconnect_max_delay_ms` | 500 ms / 30 seconds. |
| `ping_interval_ms`, `pong_timeout_ms`, `handler_timeout_ms` | 30 / 10 / 30 seconds. |
| `recovery["max_bytes"]` | 16 MiB; per decoded page in durable mode, bounded snapshot scan in best-effort mode. |
| `recovery["max_pages"]`, `max_threads`, `max_state_refresh`, `timeout_ms` | Best-effort only: 3 pages, 20 thread roots, 50 state refreshes, 30 seconds. |

A durable page larger than configured local delivery limits blocks with a non-retryable `BackpressureError`; if another channel temporarily occupies the shared budget, the worker reports retryable backpressure and reconnects from its cursor/checkpoint.

Best-effort mode opens the socket before scanning current history, buffers live frames, then reconciles and drains. Initial history uses `state_reconciled`. Reconnect can infer creations as `message` with `source="recovery"`, `replayed=True`. A finished attempt emits both `recovery_incomplete` and `recovery_complete` with `complete=False`. Transient frames may be dropped and counted when buffers fill; overflowing message/mutation delivery blocks instead of silently losing it. Control notifications are coalesced per kind and handle.

## Outgoing webhook recovery

Webhooks use committed event snapshots and persisted per-channel delivery state. HTTP acknowledgement loss can redeliver an event. Receivers must verify raw-body signatures and persist scoped deduplication before acknowledging. See [Webhooks](WEBHOOKS.md).
