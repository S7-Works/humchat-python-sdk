# Captured HumChat wire contract

`fixtures/http-ws.json` was captured from a local HumChat server using mock authentication and synthetic messages. The fixture records raw wire names and values before the SDK's camelCase mapping.

The `provenance` object records the capture context. These local samples document specific wire responses; they do not certify production deployments or every server version.

| Sample | What it records |
| --- | --- |
| `identity`, `channels`, `conversation` | `/me`, team-scoped channel discovery, and DM creation with its canonical routing key. |
| `sendRequest`, `sendReceipt`, `message` | Incoming root send, detailed ACK, and bare WebSocket message. |
| `replyRequest`, `reply`, `legacyAck` | Threaded response and legacy JSON ACK string. |
| `mutationRequest`, `mutationReceipt`, `reaction` | Desired-state reaction and authoritative receipt/event state. |
| `history`, `threadHistory` | Version 2 top-level and thread history pages. |
| `typing`, `receipt` | Typing and Unix-second read-horizon events. |
| `edit`, `delete`, `state` | Versioned edit, deletion, and final message tombstone. |

This fixture is not a transcript of every request or a replacement for server acceptance tests. Validate retries, conflicts, authorization and channel separation against the deployment you intend to use.

No live samples were captured for `presence`, `resync`, `session_revoked`, `call_invite`, forward/undo/restore, attachments, or oversized histories. Their presence in source does not make them runtime-certified here. Future decoder tests should include separate, explicitly source-derived edge-case fixtures rather than relabeling invented data as captured traffic.

SDK unit tests consume this fixture through `npm test`. Regeneration requires the separate HumChat server harness and a disposable mock-authentication database; it cannot be performed from this repository alone. The npm tarball excludes this directory. Keep future captures limited to synthetic identities and content, without credentials.
