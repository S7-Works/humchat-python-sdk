# SDK-key direct-message acceptance

This opt-in runner checks two real organization members through the configured HumChat server. It sends one labeled direct message, then one labeled reply in that message's thread. Both messages stay in HumChat; the runner does not delete them or mark them read. It verifies inbox delivery and reads both messages back through both members' history APIs. A passing result verifies SDK and server behavior; a person must still check the HumChat app UI.

## Before running

`pytest` exercises this runner only against synthetic loopback peers (`tests/integration/test_sdk_key_message_acceptance_runner.py`); it never invokes the live test. Running it explicitly against a real server changes real conversation history and may notify the chosen members.

Have the organization owner designate two distinct, active test members, and obtain permission to send the test messages and to sign in as the recipient for its automated reply. Do not choose a recipient automatically from the roster. The SDK key must belong to their organization and allow delegated login for both members.

Configure these environment variables through your private process environment or secret manager. Do not put keys in command arguments, source files, logs, screenshots, or this document.

| Variable | Required value |
| --- | --- |
| `HUMCHAT_BASE_URL` | Explicit HumChat origin (HTTPS); no path, credentials, query, or fragment |
| `HUMCHAT_ORG_ID` | The designated organization's UUID |
| `HUMCHAT_SDK_KEY` | Its private `hsk_` credential |
| `HUMCHAT_ACT_AS` | Designated sender's member email |
| `HUMCHAT_TEST_RECIPIENT_EMAIL` | Designated recipient's different member email |
| `HUMCHAT_ACCEPTANCE_SEND` | Exactly `true`, enabling the two-identity test: two persistent messages, possible DM creation, recipient login and its thread reply |
| `HUMCHAT_ACCEPTANCE_TIMEOUT_MS` | Optional total deadline, 10000–600000 milliseconds; default 180000 |

All HTTP and socket traffic goes through `HUMCHAT_BASE_URL`, using `auth={"sdk_key": ..., "act_as": ...}`.

## Run once

From the repository root, after supplying the private environment:

```sh
python tests/acceptance/sdk_key_direct_messaging.py
```

Do not pass arguments; the script refuses them so credentials never reach the command line. It prints one sanitized JSON report and exits 0 only when every acceptance check passes. It writes no evidence or checkpoint files.

## Checks and visible changes

Before any conversation creation or send, the runner verifies both minted identities, organization/email binding, messaging capabilities, organization rosters, conversation membership and inbox readiness. Each roster must contain exactly one matching active entry for each designated member with the verified actor IDs.

It starts one durable inbox watcher per member with in-memory checkpoints; unrelated messages are discarded and never printed. Then:

1. The sender calls `messages.send_direct()` with the recipient's verified actor ID (reusing an exact two-person DM or creating one). The content starts with `[HumChat SDK acceptance <random run label>] Test 1/2`.
2. The recipient inbox must deliver that exact root message.
3. Both members' streams must be `live` before the recipient calls `messages.reply()` once (`Test 2/2`, in the first message's thread).
4. The sender inbox must deliver the exact reply, and root and thread histories must return the correct message for both identities.

Each send has a fixed per-run message ID and idempotency key; transport retries reuse them. There is no automatic second run, cleanup, reaction, typing notification, read receipt or administrative mutation.

## Report and interpretation

The JSON contains only pass/fail, fixed check names, sanitized error category/status, send-attempt and accepted-message counts, delivery counts by source, and history-match counts (snake_case keys). It omits keys, tokens, raw errors, request IDs, member IDs, emails, message IDs, bodies and conversation identifiers.

On failure, inspect `failure.code`, optional HTTP status and completed checks. A failed run may already have created the DM or one or both messages; a new invocation uses a new run label, so inspect the conversation before repeating. Afterwards, confirm both messages in the recipient's and sender's HumChat app and record that UI observation separately.
