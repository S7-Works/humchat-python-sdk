# Troubleshooting

Start with the error class/`code`, HTTP `status` when available, `subscription.status` and `client.realtime.get_stats()`. SDK errors omit raw response bodies, tokens and provider messages. Log a bounded selection of fields; do not log provider results, request headers, token-bearing configuration or message/store contents by default.

```python
from humchat import HumChatError


def report(error: BaseException) -> None:
    if isinstance(error, HumChatError):
        print({"code": error.code, "status": error.status, "retryable": error.retryable,
               "request_id": error.request_id, "channel_id": error.channel_id, "message_id": error.message_id})
    else:
        print("Unexpected application error")

# subscription.on("error", report)
# subscription.on("handler_error", lambda event: report(event.error))
```

## Setup and access

| Symptom | Check / action |
| --- | --- |
| `CONFIGURATION` at construction | Required `org_id`, HTTP(S) origin-only `base_url`, an authentication mode, supported keyword names and positive integer limits. SDK-key mode requires HTTPS outside literal loopback development origins. |
| `VALIDATION` for authentication options | Use exactly one of `auth` or `token_provider`; omit the other keyword entirely. SDK-key mode validates key/email shape and a canonical org UUID. |
| `UNSUPPORTED_FEATURE` with `auth` | The login endpoint is absent (404/405) or the SDK-key capability is missing/disabled on the server. |
| Mint 401 / 403 | Invalid/revoked key or refused delegated login. Latched for this client; fix credentials and construct a new client. |
| Mint 502 / 503 | Login is unavailable. Bounded retries and a client-wide cooldown apply. |
| `UNSUPPORTED_FEATURE` with `api_key` | Bot API-key exchange is unsupported. Use `auth=` or an authorized `token_provider`. |
| `AUTHENTICATION` from a static environment token | The example does not renew tokens. Implement renewal inside the provider. Expiry values are Unix seconds. |
| `AUTHENTICATION` after refresh | Replacement actor/org must match the already-bound client. Create a new client for a different principal. |
| `UNSUPPORTED_FEATURE` during `connect()` | Server lacks capability discovery or send v1/state v1/history v2. |
| `UNSUPPORTED_FEATURE` on subscription readiness | Upgrade to event protocol v1/durable replay, or explicitly choose best-effort mode. |
| `VALIDATION` on a channel | Use `Channel.id` or `Conversation.channel_id`; it must be a canonical key in the configured organization. |
| `NOT_FOUND` / 404 from `channels.list()` | Supply a known team ID with `channels.list(team_id=...)`. Use `conversations.list()` for DM/group channels. |
| 403/404 | Confirm channel/team membership and the exact routing key. Retrying does not create access. |
| `SESSION_REVOKED` | All work on this client is blocked. Re-establish credentials and construct a new client. |
| `RuntimeError: no running event loop` | `realtime.subscribe()`, `inbox.watch()` and their futures need a running asyncio loop; call them from inside `async` code. |

## Writes, errors and repeated work

| Symptom | Check / action |
| --- | --- |
| Timeout/network error after sending | Acceptance may already have committed. Preserve the original `message_id`, `idempotency_key` and exact intent for retry, or resume the persisted outbox entry. |
| 409 / `CONFLICT` on mutation | Fetch current state; decide a new action with a new operation ID. |
| `CONFLICT` from outbox | That scoped key already belongs to a different normalized request. Repeat the original input or use a new key for a new action. |
| `CONFLICT` after a consumer callback | Checkpoint ownership may have been lost. Stop duplicate owners and reconcile side effects. |
| 429 / `RATE_LIMIT` | Observe Retry-After and the shared HTTP cooldown; reduce concurrency rather than retrying faster. |
| `PROTOCOL` | Check SDK/server protocol compatibility and response scope. A failed handler also uses this category; inspect `handler_error`. |
| Duplicate consumer callback | Delivery is at least once. Deduplicate with an application inbox or the send outbox. |

## Subscriptions and handlers

Register listeners and the one `consume()` callback synchronously after `subscribe()`. Awaiting another operation first lets startup begin and causes late consumer registration to fail. Extra handles may observe, but must use the same `start`.

| Symptom | Check / action |
| --- | --- |
| `handler_error` and `blocked` | Await all intended work, respect `context.signal` and finish within `handler_timeout_ms`. Repair the failure, close all channel handles, then resubscribe using the existing checkpoint. |
| Reconnect waits after cancellation | The acknowledged callback may still be running; its original deadline eventually blocks it. |
| `BACKPRESSURE` when subscribing | Default admission is 50 channels. Close unused handles or split channels across workers. |
| Retryable durable `BACKPRESSURE` | Another channel temporarily occupies the shared delivery budget; the worker reconnects from its cursor. |
| Non-retryable durable `BACKPRESSURE` | A page exceeds local configured limits; review buffer/page budgets. |
| `recovery_complete` with `complete=False` | Expected best-effort semantics. |
| `typing()` / `mark_read()` fails | The channel must be live; `through` is a non-negative integer in Unix seconds. |

## Retention gaps

For `RECOVERY_GAP`, record the affected scope and old/earliest cursor privately, stop the consumer, and reconcile missing business work. History provides current snapshots only. An existing checkpoint overrides `start`; any reset must be an explicit application decision with concurrent owners stopped.

## File stores

| Symptom | Check / action |
| --- | --- |
| `PERSISTENCE`: locked store | Find the owner from the sibling `.lock` record. Remove a stale lock only after confirming that exact process is gone. |
| Invalid format/corruption | Stop writers, preserve the original file, and recover from a verified backup. |
| Size budget reached | Default is 64 MiB per store; completed outbox entries are retained. Plan archival or a database adapter. |
| Write/rename/permission failure | Check a private writable local directory, free space and exclusive ownership. |

## Reporting a reproducible issue

Provide the exact SDK version (`humchat.__version__`), Python/OS version, protocol capability flags, error category/status, bounded statistics, and whether the failure used durable or best-effort mode. Include a minimal reproduction with placeholder credentials and synthetic messages. Use the [issue tracker](https://github.com/S7-Works/humchat-python-sdk/issues) for nonsensitive bugs; see [Support](SUPPORT.md) and [SECURITY.md](../SECURITY.md) for security concerns.
