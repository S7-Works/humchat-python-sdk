# Outgoing webhooks

The server can deliver administrator-managed outgoing events as signed HTTP POSTs, and `verify_webhook()` authenticates them at your receiver without maintaining a WebSocket worker. Sending messages still uses the normal authenticated SDK.

## Server setup and authorization

The server needs durable event storage and a stable webhook encryption key configured by its operator; missing configuration disables the capability. Subscriptions are **organization-owned grants**, created and managed by an authenticated organization/workspace administrator. They survive the creator's logout. Pausing or deleting the subscription revokes future delivery; already-transmitted requests cannot be recalled.

Each subscription selects 1–50 existing workspace channels in one organization; DMs/conversations are not supported. URLs and channel sets are immutable: create a replacement subscription to change them. New grants capture each channel's committed head and start **paused**. Install the returned signing secret at the receiver, then explicitly resume.

`(await client.get_capabilities()).messaging.outgoing_webhooks` is `None` on unsupported/unconfigured servers; otherwise `OutgoingWebhooks(schema_versions, max_channels, workspace_channels_only)`.

## Register and manage a destination

```python
import uuid

webhook_id = str(uuid.uuid4())  # Persist before requesting creation.
created = await client.webhooks.create(id=webhook_id, url="https://your-backend.example/humchat/webhook", channel_ids=[channel_id])
# Save created.signing_secret in your secret manager immediately, configure the receiver, then:
await client.webhooks.resume(webhook_id)
subscription = await client.webhooks.get(webhook_id)
await client.webhooks.pause(webhook_id)
await client.webhooks.resume(webhook_id)
page = await client.webhooks.deliveries(webhook_id, status="dead", limit=20)
if page.deliveries:
    await client.webhooks.replay(webhook_id, page.deliveries[0].id)
```

| Method | Result |
| --- | --- |
| `webhooks.create(id=, url=, channel_ids=)` | `WebhookSecretResult(subscription, signing_secret)` |
| `webhooks.list()` | `list[WebhookSubscription]` |
| `webhooks.get(id)` | `WebhookSubscription` |
| `webhooks.pause(id)` / `resume(id)` | Updated subscription |
| `webhooks.rotate_secret(id)` | `WebhookSecretResult` |
| `webhooks.delete(id)` | `WebhookDeletion(id, deleted=True)` |
| `webhooks.deliveries(id, status=None, limit=None, before=None)` | `WebhookDeliveryPage(deliveries, next_before)`; limit 1–100 |
| `webhooks.replay(id, delivery_id)` | Updated `WebhookDelivery` |

Management mutations are single-attempt: no HTTP retry or automatic 401 replay. A timeout may still mean the operation committed; reconcile with the persisted ID and `get()`/delivery inspection. Creation and rotation reveal a signing secret only in their successful response (`repr()` of the result redacts it).

## Verify before parsing or acknowledging

The signature header is `X-HumChat-Signature: t=<Unix seconds>,v1=<hex HMAC>`; during rotation it carries a second `v1`. The signed bytes are the ASCII timestamp, a period, and the exact HTTP request body. The key is the 32 decoded bytes of `whsec_<64 lowercase hex>`.

```python
from humchat import WebhookVerificationError, verify_webhook

try:
    envelope = verify_webhook(
        raw_body,                 # the exact bytes received, before any JSON parsing
        signature_header,
        [current_secret, previous_secret],  # one or two secrets
        expected_org_id=org_id,
        expected_subscription_id=subscription_id,
        expected_channel_id=channel_id,
    )
except WebhookVerificationError:
    ...  # respond 401/400 without detail
# Atomically persist a deduplicated inbox record (org + subscription + channel + event_id) before responding 2xx.
```

`verify_webhook()` validates the HMAC with constant-time comparison, timestamp freshness (300-second default `tolerance_seconds`, at most 3600), the envelope schema and nested event scope. Invalid, tampered or oversized payloads raise `WebhookVerificationError` without exposing payload or secret data. Enforce the size limit while reading the request too (`max_body_bytes` can lower the 8 MiB hard limit). Keep receiver clocks synchronized.

The verified `WebhookEnvelope` contains `schema`, `subscription_id`, `delivery_id`, `org_id`, `channel_id`, `event_id`, `attempt`, `sent_at` and `event`, the same normalized `DurableEvent` used by durable WebSocket consumers.

## Delivery, retries and recovery

- Delivery is at least once. Persist deduplication and business changes atomically.
- A subscription/channel has one ordered outstanding delivery; other channels continue independently.
- A delivery ID and scoped event identity remain stable across retries and replay; `sent_at`, the signature and `attempt` change per attempt.
- Only 2xx acknowledges delivery. Network failures, 408, 429 and 5xx retry with jitter within bounded windows; other responses become dead letters. Redirects are never followed.
- A dead letter or retention gap stops that channel; fix the receiver and replay, or explicitly create a new subscription.

Secret rotation keeps the previous signing secret for a short grace period, signing each attempt with both keys. Install the new secret before the grace period ends and keep the previous verification key during the transition.

## Receiver example

[`examples/webhook_receiver.py`](../examples/webhook_receiver.py) is a minimal standard-library receiver bound to `127.0.0.1` that verifies `POST /humchat/webhook` and stores a scoped deduplicated inbox record before returning success. Set `HUMCHAT_WEBHOOK_SECRET`, `HUMCHAT_ORG_ID`, `HUMCHAT_WEBHOOK_SUBSCRIPTION_ID`, comma-separated `HUMCHAT_CHANNEL_IDS` and a private `HUMCHAT_WEBHOOK_STATE_DIR`; `HUMCHAT_WEBHOOK_PREVIOUS_SECRET` is optional and `HUMCHAT_WEBHOOK_PORT` defaults to 43140. Put an HTTPS reverse proxy in front of it for a public destination, and use database-backed atomic inbox processing for distributed receivers.
