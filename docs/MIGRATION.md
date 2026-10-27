# Migration

## From the Node SDK

`humchat` 0.1.0a1 mirrors `@humchat/node` 0.1.0-alpha.8: the same server endpoints, headers, validation rules, retry/backoff policy, realtime state machines and persistence file format. Code ports mechanically with these conventions:

| Node | Python |
| --- | --- |
| `new HumChat({ orgId, baseUrl, ... })` | `HumChat(org_id=..., base_url=..., ...)`; also an async context manager. |
| camelCase option and field names (`totalTimeoutMs`, `channelId`) | snake_case (`total_timeout_ms`, `channel_id`). Wire values (`"durable"`, `"multiplex"`) are unchanged except `bestEffort` → `"best_effort"`. |
| `timeout` | `timeout_ms` (client and per call). |
| `auth: { sdkKey, actAs }` | `auth={"sdk_key": ..., "act_as": ...}` or `SdkKeyAuth(sdk_key=..., act_as=...)`. |
| `tokenProvider: async ctx => ({ token, expiresAt })` | `token_provider=` an async *or* sync callable returning `TokenResult(token, expires_at)` or a mapping. |
| `fetch:` transport injection | `transport=` an `httpx.AsyncBaseTransport` (for example `httpx.MockTransport` in tests). |
| Input objects, e.g. `messages.send({ channelId, content }, { idempotencyKey, signal })` | Keyword arguments: `messages.send(channel_id=..., content=..., idempotency_key=..., signal=...)`. |
| `messages.reply(ref, { content })` | `messages.reply(ref, content=...)`. |
| `events.sync({ channelId, after })` | `events.sync(channel_id=..., after=...)`. |
| `realtime.subscribe(id, { start: { cursor } })` | `realtime.subscribe(id, start=EventCursor(cursor))`. |
| `sub.consume(fn, { consumerId, checkpointStore })` | `sub.consume(fn, consumer_id=..., checkpoint_store=...)`. |
| `sub.markRead({ through })` | `sub.mark_read(through=...)`. |
| `inbox.watch(fn, { consumerId, checkpointStore, pollIntervalMs })` | `inbox.watch(fn, consumer_id=..., checkpoint_store=..., poll_interval_ms=...)`. |
| `outbox.send({ consumerId, key, store, message: { channelId, ... } })` | `outbox.send(consumer_id=..., key=..., store=..., message={"channel_id": ..., ...})`. |
| `store.compareAndSet(...)`, `store.markSent(...)`, `store.listPending(...)` | `compare_and_set`, `mark_sent`, `list_pending`. |
| `FileCheckpointStore.open(path, { maxBytes })` | `await FileCheckpointStore.open(path, max_bytes=...)`. |
| Event names `messageUpdated`, `recoveryComplete`, `handlerError`, ... | `message_updated`, `recovery_complete`, `handler_error`, ... |
| Plain payload objects | Dataclasses (`MessageEvent`, `StateEvent`, `ConnectionEvent`, `RecoveryCoverage`, ...). |
| `Promise` (`sub.ready`, `inbox.done`) | awaitable `asyncio.Future`s (shielded per access). |
| `AbortController` / `AbortSignal` | `humchat.AbortController` / `humchat.AbortSignal` with the same semantics; `AbortSignal.timeout(ms)`. asyncio cancellation also works. |
| `verifyWebhook(raw, header, secrets, { expectedOrgId, now })` | `verify_webhook(raw, header, secrets, expected_org_id=..., now=...)`. |
| `getConfig()`, `getCapabilities()`, `getStats()` | `get_config()`, `get_capabilities()`, `get_stats()`. |
| Error fields `requestId`, `retryAfterMs`, `earliestCursor` | `request_id`, `retry_after_ms`, `earliest_cursor`. |

Behavioural differences that are inherent to the platform:

- Methods take keyword arguments, so an unknown argument raises Python's `TypeError` rather than the Node SDK's `ValidationError` for an unknown input field. Unknown **client configuration** keywords still raise `ConfigurationError` (and `api_key` raises `UnsupportedFeatureError`), as in Node.
- `realtime.subscribe()`, `inbox.watch()` and subscription/watch futures require a running asyncio event loop.
- `humchat.PermissionError` and `humchat.TimeoutError` keep the Node names and therefore shadow Python built-ins where imported unqualified.
- The SDK-key browser refusal maps to refusing `sys.platform == "emscripten"` (Pyodide); there is no other browser runtime for Python.
- WebSocket text frames are UTF-8 decoded by the `websockets` library; an invalid UTF-8 text frame closes the socket (retryable network failure) instead of being reported as a protocol error.

Checkpoint and outbox **files are interchangeable** between the two SDKs (same JSON layout and scope hashes), so a worker can move from Node to Python without a format migration. Stop the Node owner (its lock must be released), keep the same base URL, organization, actor, consumer ID and channel, then start the Python worker on the same files.

## Version progression

| Version | Notes |
| --- | --- |
| `0.1.0a1` | First Python release. Feature parity with `@humchat/node` 0.1.0-alpha.8 (HTTP resources, SDK-key and token-provider auth, best-effort/durable/multiplex realtime, inbox, outbox, persistence, webhooks). Not published to a package index. |

## Moving a listener to durable consumption

1. Verify event protocol v1/durable capability on every server writer sharing the database.
2. Choose a stable consumer ID and a persistent checkpoint store with the full server/org/actor/consumer/channel scope. Use the outbox or an application inbox for effects that can repeat.
3. Decide the first start position: `"now"`, retained `"earliest"`, or a known `EventCursor`. History cursors and best-effort watermarks cannot be converted into a durable checkpoint.
4. Stop the old listener and settle its work before assigning the same channel to the new owner. Register `consume()` synchronously after `subscribe()`, then await `ready`.
5. Test reconnect, restart, uncertain writes, failed callbacks and a retained-cursor gap before accepting traffic.

Do not keep a business action in a `message` listener and also add it to `consume()`: both deliveries run for the same durable creation. Use `event.kind == "message"` to select creations, `event.state.body` for historical full content, and `event.state.thread_root_id or event.state.id` when replying.

## Delegated member login

Token-provider applications can keep their configuration. To let the SDK obtain and renew a member token, replace `token_provider` with `auth={"sdk_key": ..., "act_as": ...}`. Choose exactly one; omit the other keyword entirely. The switch needs no checkpoint or outbox conversion, but changing `act_as` changes the actor and therefore its checkpoint scope; do not copy another member's cursors.
