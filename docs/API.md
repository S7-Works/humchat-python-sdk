# API reference

Import public symbols from `humchat`. The package ships inline type information (`py.typed`); the docstrings and annotations in `src/humchat` are the complete type reference. Use `HumChat(...)` to obtain resources; subscriptions are returned by `client.realtime.subscribe()`. Examples below use a configured `client` from [Authentication](AUTHENTICATION.md) inside an `async` function.

All network methods are coroutines. Subscriptions, inbox watchers and their `ready`/`done` futures must be created inside a running asyncio event loop.

## Configuration and units

| Keyword | Default / requirement |
| --- | --- |
| `org_id`, `base_url` | Required. SDK-key mode requires a canonical organization UUID and HTTPS outside literal loopback origins. |
| `token_provider` or `auth={"sdk_key": ..., "act_as": ...}` | Exactly one mode. `auth` (a mapping or `SdkKeyAuth`) mints a delegated member token through HumChat; `token_provider` keeps caller-managed acquisition. See [authentication](AUTHENTICATION.md). |
| `timeout_ms` | `10_000` milliseconds per HTTP attempt. |
| `retry["max_retries"]` | `3` retries after the initial attempt; allowed range 0–10. |
| `retry["initial_delay_ms"]`, `retry["max_delay_ms"]` | `250`, `2_000`; exponential full jitter, subject to Retry-After. |
| `retry["total_timeout_ms"]` | `30_000`; total HTTP-operation budget including authentication and retries. |
| `realtime["transport"]` | `"channel"` by default; `"multiplex"` shares one socket and requires durable recovery plus server capability. See [shared connections](MULTIPLEX.md). |
| `recovery["mode"]` | `"durable"`; explicit `"best_effort"` supports older servers. |
| `transport` | Optional `httpx.AsyncBaseTransport` for HTTP (tests, or an application-managed proxy). Not used for WebSockets. Environment proxy variables are ignored. |

Unknown keyword arguments raise `ConfigurationError` (`api_key` raises `UnsupportedFeatureError`). `retry`, `realtime` and `recovery` are plain dicts (`RetryOptions`, `RealtimeOptions`, `RecoveryOptions` are their `TypedDict` types); unknown keys raise `ConfigurationError`, and `None` values mean "use the default".

Resource methods accept the keyword arguments `signal: AbortSignal | None` and `timeout_ms: int | None`. `timeout_ms` overrides the HTTP attempt timeout; it does not replace `retry["total_timeout_ms"]` with a whole-workflow deadline. `connect()` and `get_capabilities()` cap their wait by the smaller of an explicit `timeout_ms` and the total budget. For a deadline covering your full workflow, pass `AbortSignal.timeout(ms)` or wrap the call in `asyncio.timeout(...)`.

`AbortController`/`AbortSignal` mirror the JavaScript types: aborting one caller's signal fails only that caller, while shared work (for example a token acquisition other callers wait on) continues. Plain asyncio task cancellation is also honoured for your own awaits.

Message timestamps, identity expiry, read watermarks, typing timestamps, presence `last_seen` and call `created_at` are Unix **seconds**. Configuration timeouts, `TypingEvent.ttl_ms`, `retry_after_ms`, and `ConnectionEvent.next_retry_at` are **milliseconds**; `next_retry_at` is an absolute Unix-millisecond time. Durable event `sequence`, `through_sequence` and `earliest_sequence` are decimal strings, exactly as on the wire; compare them with `int(...)`, never lexically.

Identifiers are opaque, case-sensitive strings, not necessarily ULIDs. Supported characters are ASCII letters/digits and `.`, `_`, `:`, `-`; `.` and `..` alone are invalid. Message/thread/operation/idempotency IDs allow 1–128 characters, channel keys 1–1024, and actor IDs 1–256. A channel key must be in the configured organization. `consumer_id` additionally starts with a letter or digit. Send/edit text must contain non-whitespace content and fit 1 MiB of UTF-8 bytes. An emoji key is nonempty, at most 32 UTF-8 bytes, without surrounding whitespace or control characters. Invalid inputs raise `ValidationError` before the resource request.

## Client and discovery

| Method | Result |
| --- | --- |
| `await connect()` | `Identity(actor_id, org_id, email, roles, scopes, expires_at, app_id)`; verifies and binds the actor/organization. |
| `await get_capabilities()` | `SdkCapabilities`; cached with the verified session. |
| `get_config()` | Credential-free `SafeConfig` copy. |
| `await close()` / `async with HumChat(...)` | Cancels requests, subscriptions and timers; does not close caller-owned stores or revoke credentials. |
| `channels.list(team_id=None)` | `ChannelList(org_id, team_id, team_name, channels)` for one existing team workspace. |
| `conversations.list()` | `list[Conversation]`; use `channel_id` for messages. |
| `members.list()` | Organization roster `list[OrgMember(actor_id, name, email, role, status)]`. |
| `conversations.create(member_ids=[...], name=None)` | Create/reuse a conversation; creation is never automatically retried. |
| `conversations.open_direct(member_id=...)` | Find an unnamed exact peer DM or ask the server to create/reuse it. |
| `inbox.watch(handler, consumer_id=..., checkpoint_store=..., poll_interval_ms=None, max_conversations=None)` | Managed discovery and acknowledged consumption across the identity's conversations. See [direct messages](DIRECT_MESSAGES.md). |

`get_capabilities().authentication.sdk_key_user_tokens` is `SdkKeyUserTokens(endpoint, key_header, configured)` when advertised, otherwise `None`. SDK-key login bootstraps at the fixed HumChat endpoint `/api/sdk/auth/user-token` on `base_url`, then verifies this metadata after `/me`. A missing/disabled capability rejects SDK-key login; an unsupported path/header raises `ProtocolError`. The metadata never redirects the key.

`authentication.socket_session_end_codes` is a list of strings, empty on older servers. When it includes `TOKEN_EXPIRED`, that exact code on a `session_revoked` frame permits verified renewal and reconnect. Missing/unknown codes and unadvertised expiry remain terminal.

`channels.list()` leaves team selection to HumChat. For a normal member session, the server defaults to `<org_id>-home`; that workspace may not exist. Pass a known `team_id` from your application's HumChat team configuration. A missing workspace raises `NotFoundError` (404). The SDK has no team-list discovery method. `conversations.list()` discovers the member's separate DM/group conversation channels.

```python
listing = await client.channels.list(team_id=os.environ["HUMCHAT_TEAM_ID"])
for channel in listing.channels:
    print(channel.id, channel.display_name)
for conversation in await client.conversations.list():
    print(conversation.channel_id, conversation.name)
```

`Channel` contains `id`, `org_id`, `team_id`, `slug`, `display_name`, `is_private`. `Conversation` contains management `id`, messaging `channel_id`, `name`, `kind`, `member_count`, `my_role`, `members` (`ConversationMember(actor_id, role)`), `last_message_at`.

## Send, reply and mutate

| Method | Keyword input / result |
| --- | --- |
| `messages.send(...)` | `channel_id, content, id=None, thread_root_id=None, priority=None, idempotency_key=None` → `SendReceipt`. |
| `messages.send_direct(...)` | `recipient_id, content, id=None, thread_root_id=None, priority=None, idempotency_key=None` → `DirectSendReceipt(conversation, receipt)`. |
| `messages.reply(reference, ...)` | Reference is any object (or mapping) with `id`, `channel_id`, `thread_root_id`; `content, id=None, priority=None` → `SendReceipt`. |
| `messages.get_state(...)` | `channel_id, message_id` → current full `MessageState`. |
| `messages.edit(...)` | `channel_id, message_id, content, expected_revision, operation_id=None` → `MutationReceipt`. |
| `messages.delete(...)` | `channel_id, message_id, expected_revision, operation_id=None` → `MutationReceipt`. |
| `messages.set_reaction(...)` | `channel_id, message_id, emoji, present, operation_id=None` → `MutationReceipt`. |

`priority` is `"standard"` (default), `"important"` or `"urgent"`. Replying to a reply preserves `reference.thread_root_id`; replying to a root uses `reference.id`. `Message` and `MessageState` are valid references. Edit/delete require the current content revision and server permission; the SDK validates that successful edit/delete receipts belong to the bound sender. Reaction `present=True` adds your reaction and `False` removes it.

```python
listing = await client.channels.list()
channel = next(item for item in listing.channels if item.slug == "general")
sent = await client.messages.send(channel_id=channel.id, content="Build complete.", priority="important")
original = await client.messages.get_state(channel_id=channel.id, message_id=sent.id)
await client.messages.reply(original, content="Tests passed.")
await client.messages.set_reaction(channel_id=channel.id, message_id=sent.id, emoji="✅", present=True)
edited = await client.messages.edit(
    channel_id=channel.id, message_id=sent.id, expected_revision=original.revision, content="Build and tests complete."
)
# await client.messages.delete(channel_id=channel.id, message_id=sent.id, expected_revision=edited.state.revision)
```

`SendReceipt` contains `id`, `channel_id`, `status="accepted"`, `created_at`, `idempotency_key`, `attachment_count`, `attachment_manifest_blake3`. `MutationReceipt` contains `schema=1`, `operation_id`, `actor_id`, `request_digest`, `state`. Generated message and operation IDs are monotonic ULIDs. A send defaults its idempotency key to its message ID. Automatic retries retain the exact serialized intent; a new call generates new IDs unless you supply the originals. See [reliability](RELIABILITY.md) before adding application retries.

`MessageState` includes `schema=1`, `id`, `org_id`, `channel_id`, `sender_id`, `created_at`, `thread_root_id`, `priority`, `original_body`, `body`, `revision`, `updated_at`, `deleted`, `reaction_revision`, `reactions` (emoji → sorted actor IDs), `attachments` (`MessageAttachment`). Uploading/sending attachments, pinning, forwarding, restore/undo and arbitrary raw requests are not exposed.

## History and durable events

| Method | Behavior |
| --- | --- |
| `messages.history(channel_id=, thread_root_id=None, cursor=None, limit=None)` | One `HistoryPage` (schema 2) with full current states and `next_cursor`. Limit 1–200. |
| `messages.iterate_history(...)` | Async generator of **pages**, newer pages first; each page's messages are ascending. |
| `events.sync(channel_id=, after=, through=None, limit=None)` | One forward `EventPage` from the durable log; limit 1–200. |

```python
async for page in client.messages.iterate_history(channel_id=channel_id, limit=100):
    for state in page.messages:
        print(state.id, state.deleted)
```

For manual event paging, obtain `after` from an application checkpoint or a received durable envelope. Keep the first page's `through_cursor` fixed while continuing with `next_cursor`:

```python
following, through = after, None
while following is not None:
    page = await client.events.sync(channel_id=channel_id, after=following, through=through, limit=100)
    through = through or page.through_cursor
    for event in page.events:
        print(event.sequence, event.kind, event.state.id)
    following = page.next_cursor
```

`EventPage` contains `schema=1`, `org_id`, `channel_id`, `epoch`, `events`, `after_cursor`, `through_cursor`, `through_sequence`, `next_cursor`. `DurableEvent` contains the same scope plus `event_id`, `actor_id`, `sequence`, `cursor`, `kind` and full `state`.

## Subscriptions

| Method / property | Behavior |
| --- | --- |
| `realtime.subscribe(channel_id, start=None)` | Returns a handle immediately. Durable start is `"now"` (default), `"earliest"`, or `EventCursor(cursor)`; saved checkpoints take precedence. |
| `realtime.subscribe_many(channel_ids)` | `list[SubscriptionResult]` with either `subscription` or `error` set. |
| `realtime.get_stats()` | `RealtimeStats`. |
| `await subscription.ready` | Initial startup/recovery completion. Each access returns a shielded future, so cancelling one awaiter does not affect others. |
| `subscription.status` | `idle`, `connecting`, `syncing`, `live`, `backoff`, `blocked`, `closed`. |
| `subscription.on(name, listener)` / `off(name, listener)` | Add/remove a sync or async listener; return the handle. Unknown names raise `ValidationError`. |
| `subscription.consume(handler, consumer_id=..., checkpoint_store=...)` | One acknowledged durable consumer per shared channel; register synchronously before startup. `handler(event, context)` may be sync or async. |
| `await subscription.typing()` | Sends a transient signal while live. |
| `await subscription.mark_read(through=...)` | Sends an explicit Unix-second read watermark while live. |
| `await subscription.resync()` | Schedules replay/reconciliation; observe `recovery_complete`. |
| `await subscription.close()` | Detaches a handle; the last handle removes its channel registration. |

| Event | Payload |
| --- | --- |
| `event` | `DurableEvent`. |
| `checkpoint` | `CheckpointEvent(channel_id, consumer_id, cursor, sequence)` after a persisted acknowledgement. |
| `message` | `MessageEvent(message, source, replayed)`; source is `live` or `recovery`. |
| `message_updated`, `message_deleted`, `reaction_updated` | `StateEvent(state, source)`. |
| `state_reconciled` | `StateEvent(state, source="recovery")`; best-effort snapshot reconciliation. |
| `typing`, `presence`, `receipt`, `call_invite` | `TypingEvent`, `PresenceEvent`, `ReceiptEvent`, `CallInviteEvent`; transient. |
| `connection` | `ConnectionEvent(channel_id, state, attempt, next_retry_at)`. |
| `resync` | `ResyncEvent(channel_id, reason)`. |
| `recovery_complete` | `RecoveryCoverage`: durable has `complete=True` and `cursor`; best effort has `complete=False`. |
| `recovery_incomplete` | `RecoveryCoverage` with `limitations` (best effort). |
| `session_revoked`, `auth_required` | `SessionRevokedEvent(channel_id, reason)`, `AuthRequiredEvent(channel_id, error)`. |
| `error` | A `HumChatError`. |
| `handler_error` | `HandlerErrorEvent(event, error)`; event name can be `"consume"`. |
| `unknown_event` | `UnknownEvent(channel_id, type)`; bounded name, no raw frame. |

Every listener receives its own deep copy of the payload. Live `Message.content` can be a snippet when `is_long` is true; fetch `get_state()` for current full text. Durable consumers already receive full historical text in `event.state.body`.

## Persistence and outbox

`MemoryCheckpointStore()` and `MemoryOutboxStore()` are process-local. `await FileCheckpointStore.open(path, max_bytes=None)` and `await FileOutboxStore.open(path, max_bytes=None)` persist to separate files (default 64 MiB, 1 KiB–1 GiB). Call each store's `close()` explicitly. The on-disk format matches the Node SDK.

`outbox.send(consumer_id=, key=, store=, message={...}, idempotency_key=None)` persists exact send intent before network work and the accepted receipt before returning. `message` is a mapping with `channel_id`, `content` and optional `id`, `thread_root_id`, `priority`. `outbox.flush(consumer_id=, channel_id=, store=)` retries that scope's pending intents sequentially and stops at the first failure.

Custom `CheckpointStore` adapters implement `async load(scope)`, atomic `async compare_and_set(scope, expected_cursor, next_cursor)`, and `async close()`. Custom `OutboxStore` adapters implement `prepare(scope, key, message, *, idempotency_key=None)`, `mark_sent`, `list_pending` and `close`. `PersistenceScope(server_origin, org_id, actor_id, consumer_id, channel_id)`: all five parts matter.

## Error categories

All errors extend `HumChatError`. They expose `code`, `kind`, `retryable` and optional context: `status`, `request_id`, `channel_id`, `message_id`, `operation_id`, `idempotency_key`, `retry_after_ms`, `earliest_cursor`, `earliest_sequence` (`None` when unavailable). Catch by class or `code`, not message text.

| Class / code | Meaning |
| --- | --- |
| `ConfigurationError` / `CONFIGURATION` | Invalid client/store setup. |
| `ValidationError` / `VALIDATION` | Invalid input, or HTTP 400/422. |
| `AuthenticationError` / `AUTHENTICATION` | Token/provider/identity failure, or HTTP 401. |
| `SessionRevokedError` / `SESSION_REVOKED` | Client credential lifecycle was explicitly revoked. |
| `PermissionError` / `PERMISSION`; `NotFoundError` / `NOT_FOUND` | HTTP 403 or 404. |
| `ConflictError` / `CONFLICT` | HTTP 409, changed intent, stale revision or checkpoint ownership conflict. |
| `RateLimitError` / `RATE_LIMIT` | HTTP 429. |
| `NetworkError` / `NETWORK`; `TimeoutError` / `TIMEOUT`; `AbortError` / `ABORT` | Transport failure, deadline or cancellation. |
| `ServerError` / `SERVER` | HTTP 5xx. |
| `ProtocolError` / `PROTOCOL` | Invalid/mismatched wire data or failed application delivery. |
| `UnsupportedFeatureError` / `UNSUPPORTED_FEATURE` | Missing protocol/capability or unsupported API-key mode. |
| `BackpressureError` / `BACKPRESSURE` | Channel, queue or delivery budget exceeded. Inspect `retryable`. |
| `RecoveryGapError` / `RECOVERY_GAP` | Event cursor expired or stream epoch changed. |
| `PersistenceError` / `PERSISTENCE` | Store/lock/format/write failure. |
| `WebhookVerificationError` / `WEBHOOK_VERIFICATION` | Any webhook authentication, timestamp or schema failure. |

`humchat.PermissionError` and `humchat.TimeoutError` shadow the Python built-ins of the same name inside modules that import them; import them explicitly (`from humchat import TimeoutError as HumChatTimeout`) if you also catch the built-ins.

## Webhook management and verification

`client.webhooks` provides `create(id=, url=, channel_ids=)`, `list()`, `get(id)`, `pause(id)`, `resume(id)`, `delete(id)`, `rotate_secret(id)`, `deliveries(id, status=None, limit=None, before=None)` and `replay(id, delivery_id)`. `verify_webhook(raw_body, signature_header, secrets, ...)` returns a verified `WebhookEnvelope` with a normalized `DurableEvent`, or raises `WebhookVerificationError`. Management mutations are single-attempt, including authentication failures. See the [webhook guide](WEBHOOKS.md).
