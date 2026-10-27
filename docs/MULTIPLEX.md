# One connection for multiple channels

An opt-in transport carries progress notifications for multiple channels on one physical WebSocket. Message sends still use HTTP. Each channel keeps its own durable event log, checkpoint, callback order and failure state; there is no global cross-channel sequence.

## Configure

```python
import asyncio
import os

from humchat import HumChat, TokenResult


async def token_provider(context):
    return TokenResult(token=os.environ["HUMCHAT_TOKEN"])


client = HumChat(
    org_id=os.environ["HUMCHAT_ORG_ID"],
    base_url=os.environ["HUMCHAT_BASE_URL"],
    token_provider=token_provider,
    recovery={"mode": "durable"},
    realtime={"transport": "multiplex", "max_channels": 50, "connect_concurrency": 5},
)
results = client.realtime.subscribe_many(["canonical-channel-a", "canonical-channel-b"])
for result in results:
    if result.error is not None:
        raise result.error
    result.subscription.on("error", lambda error: print(error.code))
    result.subscription.on("message", lambda event: print(event.message.id))
await asyncio.gather(*(result.subscription.ready for result in results))
print(client.realtime.get_stats().active_sockets)  # 1 while connected
# At application shutdown: await client.close()
```

Replace the sample channel IDs with accessible canonical routing keys. The environment token is static; implement renewal in `token_provider` (or use `auth=`) for unattended operation. For acknowledged processing, register `consume()` synchronously before awaiting startup, using the [reliability guide](RELIABILITY.md). The runnable [multiplex worker](../examples/multiplex_worker.py) combines this transport with file checkpoints and a send outbox.

## Requirements and migration

The server must advertise `messaging.multiplexed_events.protocol_versions: [1]` and a `max_channels` limit. `get_capabilities()` exposes this as `messaging.multiplexed_events` (`MultiplexedEvents(protocol_versions, max_channels)`), or `None` when absent. The SDK default remains 50 distinct channels; raise `max_channels` explicitly after testing your workload.

`realtime["transport"]` defaults to `"channel"`, using one socket per canonical channel. `"multiplex"` requires durable recovery. Missing capability fails with `UnsupportedFeatureError`; combining it with `"best_effort"` is rejected at construction. The SDK never silently falls back. Ensure every serving instance and proxy supports authenticated upgrades to `/ws/events` before enabling it.

To change transport, stop the old client, settle outstanding effects, then construct a new client with the new setting. Keep the origin, organization, actor, consumer ID, channel keys and persistence stores unchanged. No cursor, checkpoint or outbox format migration is needed.

## Behavior and failures

| Situation | Behavior |
| --- | --- |
| Multiple handles for one channel | Share one logical registration; one acknowledged consumer is allowed. Closing the last handle removes that registration. |
| Dynamic subscribe/unsubscribe | Uses correlated registration IDs; stale frames cannot be delivered to a replacement registration. |
| Connection loss | One reconnect/backoff/heartbeat cycle serves the client. Each channel resumes from its last processed cursor, using the persisted checkpoint when configured. |
| Credential renewal | A verified same-actor/same-organization replacement rotates the shared connection once. Explicit session revocation blocks the client. |
| Channel denial or durable delivery/consumer handler failure | Blocks that channel; other authorized channels continue. |
| Whole-stream authentication or malformed protocol failure | Affects all registrations sharing that stream. |
| Missed notification | Periodic HTTP replay checks catch committed transitions. A notification is a hint, not an acknowledgement. |

`connect_concurrency` bounds logical channel admission on the shared socket. `active_sockets` reports physical sockets; `channels` and `subscriptions` report logical channels and handles. Typing/read signals carry channel correlation; received typing/presence/receipt/call notifications remain ephemeral. Durable message/edit/delete/reaction bodies come from authenticated `/api/events/sync`, not the shared socket. The at-least-once effects, retention-gap behavior and cancellation limits in [Reliability](RELIABILITY.md) still apply.

## Deployment limits

Multiplexing reduces physical WebSocket count. It does not remove per-channel HTTP replay, authorization work or checkpoint storage. Do not interpret local controlled-peer measurements as production capacity.
