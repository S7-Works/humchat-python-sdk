# Changelog

## 0.1.0a1 - 2026-10-27 (review preview; not published)

First release of the HumChat Python SDK, a port of `@humchat/node` 0.1.0-alpha.8 (including the unreleased Node fix for inbox rejoin settlement, commit `44ca199`). The package is not published to a package index (`Private :: Do Not Upload`) and remains `UNLICENSED`.

- Async-first `HumChat` client (Python 3.10+, `httpx` + `websockets`), with `py.typed` inline types and an async context manager.
- Mutually exclusive `auth={"sdk_key", "act_as"}` (delegated SDK-key login through HumChat) and `token_provider` authentication, with the Node SDK's validation, HTTPS requirement outside literal loopback origins, single-flight cached acquisition, proactive renewal, one coordinated 401 refresh, terminal mint 401/403, shared outage cooldown and capability-gated `TOKEN_EXPIRED` socket recovery.
- HTTP transport with bounded retries/full jitter, Retry-After and 429 cooldown, total operation deadlines, no redirects, no environment proxies, 10 MiB response cap, credential-safe request-ID redaction and sanitized typed errors.
- Resources: channels, conversations (create / open-direct), members, messages (send, send-direct, reply, get-state, history and page iteration, edit, delete, set-reaction), durable events sync, outbox, webhooks management, managed inbox.
- Realtime: best-effort per-channel recovery, durable ordered event-log consumption with acknowledged checkpoints, and the optional multiplexed `/ws/events` transport, with the Node SDK's reconnect, liveness, backpressure, settlement-barrier and inbox rejoin semantics.
- Persistence: memory and file checkpoint/outbox stores. File stores use the same on-disk format as the Node SDK.
- `verify_webhook()` for signed outgoing webhooks.
- `AbortController`/`AbortSignal` with JavaScript semantics alongside native asyncio cancellation.
- Test suite ports every Node unit, integration, release-policy and package test; docs and examples adapted to Python.
