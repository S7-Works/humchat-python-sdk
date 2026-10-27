# Runnable backend examples

Use Python 3.10 or newer. Install a reviewed wheel into your backend's virtual environment (`pip install ./humchat_sdk-0.1.0a1-py3-none-any.whl`), copy this directory (it ships in the sdist), and run the scripts from inside it so `config.py` is importable. The package is not published to a package index and licensing remains pending.

Every value comes from the process environment. Nothing is printed except IDs, counts and SDK error codes; tokens, SDK keys, signing secrets and server response bodies are never printed. There is no default server: always supply your own `HUMCHAT_BASE_URL`.

## Authentication

`config.py` supports exactly one mode:

- `HUMCHAT_TOKEN`: a user-session token your application obtains. A static environment token cannot renew itself; for unattended use replace the static `token_provider` with your renewal flow.
- `HUMCHAT_SDK_KEY` plus `HUMCHAT_ACT_AS`: delegated SDK-key login. The SDK mints, verifies and renews short-lived member tokens itself. See [authentication](../docs/AUTHENTICATION.md).

`send_and_list.py` is deliberately self-contained and uses `HUMCHAT_TOKEN` only.

## Environment

| Variable | Used by | Meaning |
| --- | --- | --- |
| `HUMCHAT_BASE_URL` | Messaging examples | HTTP(S) server origin, with no path or query. |
| `HUMCHAT_ORG_ID` | All | Organization for this verified session (a UUID in SDK-key mode). |
| `HUMCHAT_TOKEN` or `HUMCHAT_SDK_KEY` + `HUMCHAT_ACT_AS` | Messaging examples | Credentials, see above. |
| `HUMCHAT_CHANNEL_ID` | Send, react, single-channel responders, smoke send | Canonical channel routing key, including for a DM. |
| `HUMCHAT_TEAM_ID` | Channel lists | Optional workspace team filter. |
| `HUMCHAT_MESSAGE_ID` | React | Existing message in the selected channel. |
| `HUMCHAT_SEND_MESSAGE` | `send_and_list.py` | Optional message text; it sends only when this variable is present. |
| `HUMCHAT_CHANNEL_IDS` | Multi-channel and multiplex workers, webhook receiver | Comma-separated canonical IDs; at most 50 distinct channels. |
| `HUMCHAT_STATE_DIR` | Durable responders, DM responder | Private persistent directory owned by this process (checkpoint and outbox files). |
| `HUMCHAT_CONSUMER_ID` | Durable responders, DM responder | Optional stable worker identity. Defaults to `status-responder-v1`, `multi-channel-status-v1` or `multiplex-status-v1`. |
| `HUMCHAT_START` | Durable responders | `now` by default; `earliest` replays retained historical commands on first use. A saved checkpoint takes precedence. |
| `HUMCHAT_RUN_FOR_MS` | Durable responders, DM responder, webhook receiver | Optional bounded demo duration, followed by a clean shutdown. |

## Commands

Run send/react/responder commands only where their writes are intended.

| Command | Result |
| --- | --- |
| `python list_channels.py` | Prints channel IDs and display names. |
| `python send.py "Hello from my backend"` | Sends one message; prints its message ID and `accepted` status. |
| `python react.py "✅"` | Sets the reaction present; prints operation ID and reaction revision. |
| `python send_and_list.py` | Prints a JSON channel list; with `HUMCHAT_SEND_MESSAGE` also sends it. |
| `python thread_responder.py` | Older-server, best-effort example. Replies to live `!status` messages in their thread, ignoring its own messages. Downtime commands can be missed. |
| `python durable_responder.py` | Durable single-channel `!status` responder using checkpoint and outbox files. |
| `python multi_channel_worker.py` | Durable responder across `HUMCHAT_CHANNEL_IDS`; one socket per distinct channel. |
| `python multiplex_worker.py` | Same durable responder over one physical socket; requires the server multiplex capability, see [shared connections](../docs/MULTIPLEX.md). |
| `python dm_responder.py` | Identity-wide watcher over every DM and group conversation; replies to `!status`. Optional `HUMCHAT_RECIPIENT_ID` sends one persisted greeting. See [direct messages](../docs/DIRECT_MESSAGES.md). |
| `python webhook_receiver.py` | Signed webhook receiver on `127.0.0.1` (port `HUMCHAT_WEBHOOK_PORT`, default 43140) backed by `webhook_inbox.py`. |

The durable workers share `durable_worker.py`. They print JSON `ready`, `reply` and `stopped` records, reply with `All systems operational.` and keep an existing thread root. A terminal channel error stops the worker with a non-zero exit; transient failures use the SDK's reconnect behaviour.

## Durable restart and shutdown

Use a server that advertises durable event protocol v1. Keep the same origin, organization, actor, consumer ID, channel IDs and state directory across restarts. The worker flushes pending outbox entries before subscribing. Its handler returns only after the reply receipt is saved; the SDK then advances that input's checkpoint. Processing is at least once, and stable outbox keys keep retries bound to the same outgoing intent.

Ctrl+C/SIGTERM and `HUMCHAT_RUN_FOR_MS` abort outstanding SDK requests, close sockets, wait for active handlers and release the file-store locks. An interrupted input stays unacknowledged and is replayed on restart. One process owns each state directory. A crash can leave `.lock` files: verify the previous process has exited before removing them manually. Do not discard checkpoint/outbox data to bypass a retention gap; decide explicitly how the application recovers.

## Signed webhook receiver

`webhook_receiver.py` accepts signed events at `POST /humchat/webhook` on loopback and commits a deduplicated local inbox (`webhook_inbox.py`) before acknowledging with `204`. It needs no user-session token. Set `HUMCHAT_ORG_ID`, `HUMCHAT_WEBHOOK_SUBSCRIPTION_ID`, `HUMCHAT_CHANNEL_IDS`, `HUMCHAT_WEBHOOK_SECRET`, optionally `HUMCHAT_WEBHOOK_PREVIOUS_SECRET` during rotation, and a private `HUMCHAT_WEBHOOK_STATE_DIR`; see [webhooks](../docs/WEBHOOKS.md). Put a public HTTPS reverse proxy in front for an external destination. This is a single-process inbox example, not a distributed business transaction.

## Quickstart

```python
import asyncio
import os

from humchat import HumChat


async def main() -> None:
    async with HumChat(
        org_id=os.environ["HUMCHAT_ORG_ID"],
        base_url=os.environ["HUMCHAT_BASE_URL"],
        auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]},
    ) as client:
        channels = await client.channels.list()
        print([channel.id for channel in channels.channels])
        receipt = await client.messages.send(channel_id=os.environ["HUMCHAT_CHANNEL_ID"], content="Hello from Python")
        print(receipt.id, receipt.status)


asyncio.run(main())
```

The package tests install the built wheel into a fresh virtual environment, type-check consumers against it, and run real requests against controlled loopback HTTP and WebSocket peers with synthetic credentials. They make no production HumChat requests.
