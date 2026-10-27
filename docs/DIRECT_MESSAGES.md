# Direct messages and the managed inbox

The SDK acts as the user returned by `client.connect()`. Messages to that identity are normal HumChat conversations; messages sent by the SDK use that same identity. The server decides which member an SDK key may sign in as and which conversations that member may access. This feature does not create a bot or bypass organization and conversation permissions. The server must expose `/api/sdk/capabilities` and the roster/conversation routes. Receiving also requires durable event protocol v1.

## Sign in through HumChat

```python
import os

from humchat import HumChat

client = HumChat(
    base_url=os.environ["HUMCHAT_BASE_URL"],
    org_id=os.environ["HUMCHAT_ORG_ID"],
    auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]},
)
await client.connect()
# Use the send and inbox examples below; close the client during shutdown.
```

Exchange and automatic renewal go only through HumChat. A caller-supplied `token_provider` remains an alternative; do not combine it with `auth`. See [authentication](AUTHENTICATION.md) and the [login guide](SDK_KEY_LOGIN_GUIDE.md).

## Find a recipient and send

```python
members = await client.members.list()
recipient = next((m for m in members if m.email == "colleague@example.com" and m.status == "active"), None)
if recipient is None:
    raise SystemExit("Choose an accessible active organization member")
result = await client.messages.send_direct(recipient_id=recipient.actor_id, content="Build completed.")
print(result.conversation.channel_id, result.receipt.id)
```

`members.list()` returns `list[OrgMember(actor_id, name, email, role, status)]` from the authenticated organization's roster. Guest visibility and creation permissions are decided by the server. The SDK relies on its verified, organization-bound session and rejects a mismatched optional `org_uuid` if the server includes one. Names and email addresses are display/search data; use the exact `actor_id` to address a recipient.

`conversations.open_direct(member_id=...)` first finds an unnamed conversation with exactly the caller and that member, then calls `conversations.create(member_ids=[member_id])` if necessary. It rejects self-DMs. `create(member_ids=[...], name=None)` also supports group conversations; the caller is always included by the server. The returned `Conversation.channel_id` is the canonical routing key used by all message/replay APIs; the management `id` is not a routing key.

Creation has no idempotency-key contract. The SDK makes only one POST attempt, including after a 401; an ambiguous failure can leave a conversation created on the server. Reconcile with `conversations.list()`/`open_direct()` before trying again. Send retries use the normal stable message ID and idempotency key; supply both when preserving intent across separate application attempts, or open the DM and use the persistent [outbox](RELIABILITY.md). `send_direct` and `open_direct` bound their combined HTTP steps by `retry["total_timeout_ms"]`.

The recipient reads these messages through HumChat's normal conversation/history APIs. An already-open app must refresh or discover new conversations to show a newly created DM.

## Watch incoming conversations

```python
from humchat import FileCheckpointStore

checkpoints = await FileCheckpointStore.open("./private-state/inbox.json")
me = await client.connect()


async def handle(event, context):
    if event.kind != "message" or event.state.sender_id == me.actor_id:
        return
    # Await your durable business processing here; observe context.signal when cancelled.
    await process_incoming(event, signal=context.signal)


inbox = client.inbox.watch(handle, consumer_id="my-inbox-v1", checkpoint_store=checkpoints)
try:
    await inbox.ready
    await inbox.done
finally:
    await inbox.close()
    await client.close()
    # Settle any application work before closing caller-owned stores.
    await checkpoints.close()
```

The watcher polls `conversations.list()` every 5000 ms after the preceding discovery finishes. It includes both direct and group conversations the identity belongs to, including its own sent messages and historical mutations. It does not watch arbitrary workspace channels. `poll_interval_ms` can be changed (minimum 100 ms), and `refresh()` performs an immediate, coalesced discovery request.

Each newly discovered channel registers an acknowledged consumer with `start="earliest"`. A saved checkpoint takes precedence. This recovers a retained message committed **before** discovery; the ordinary subscription default of `"now"` would skip it. On first use, existing conversations also replay their retained histories, potentially including old commands. Choose a new consumer's business cutover deliberately and make external effects idempotent. Persisted retention gaps block instead of skipping data.

| Handle / option | Behavior |
| --- | --- |
| `await inbox.ready` | Initial capability check, discovery and consumer registration completed; per-channel replay may still be running. Raises on initial discovery/admission failures. |
| `await inbox.done` | Raises on any terminal discovery, channel, checkpoint or handler failure; returns on explicit close. Always observe it. |
| `status`, `error` | `starting`, `watching`, `blocked` or `closed`, plus the terminal error when present. |
| `last_channel_error` | Most recent channel error, including transient reconnect failures. |
| `get_stats()` | `InboxStats(conversations, channels=[InboxChannelStats(channel_id, status)])`. |
| `refresh()` | Coalesces with an existing discovery; errors are also reflected by `done`. |
| `max_conversations` | Defaults to `realtime["max_channels"]` (50). Exceeding it blocks the watcher; nothing is silently omitted. |
| `close()` | Cancels discovery/timers and closes owned handles. `client.close()` also closes the watcher. Caller-owned stores remain open. |

There is one active watcher per client and one acknowledged consumer per channel. Do not mix the watcher with independently consumed subscriptions to the same conversations. Callbacks are serialized per channel; different channels can process concurrently. Optional multiplex transport shares one socket while preserving per-channel checkpoints.

A conversation missing from a later successful list has its owned subscription closed. Discovery waits for that channel's interrupted consumer and any pending checkpoint operation to settle before it can automatically rejoin the conversation, retaining its checkpoint. The consumer keeps its original `realtime["handler_timeout_ms"]` deadline; pending checkpoint settlement is bounded by `retry["total_timeout_ms"]`. A missed deadline or checkpoint failure blocks the watcher and raises from `done`. Explicit watcher/client close cancels this wait promptly; it cannot stop an application's pending coroutine.

A channel-level denial or discovery failure after the normal bounded HTTP retries stops the whole watcher and raises from `done`; inspect the error, close the watcher, and explicitly restart after resolving it. Transient channel disconnections keep the normal reconnect/replay behavior.

Run [dm_responder.py](../examples/dm_responder.py) for a persistent `!status` responder that filters its own messages, replies in the same thread and saves outgoing intent before acknowledging input. See [example setup](../examples/README.md).
