"""Synthetic webhook fixtures (port of test/unit/webhooks.fixtures.mjs)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

org = "webhook-org"
channel = org + ":team:general"
actor = "webhook-actor"
subscription_id = "11111111-1111-4111-8111-111111111111"
delivery_id = "22222222-2222-4222-8222-222222222222"
secret = "whsec_" + "11" * 32
previous_secret = "whsec_" + "22" * 32
timestamp = 1_792_370_346
epoch = "33333333-3333-4333-8333-333333333333"


def _cursor() -> str:
    raw = json.dumps({"schema": 1, "org_uuid": org, "channel_id": channel, "epoch": epoch, "sequence": "1"}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def envelope(**overrides: Any) -> dict[str, Any]:
    return {
        "schema": 1,
        "subscription_id": subscription_id,
        "delivery_id": delivery_id,
        "org_uuid": org,
        "channel_id": channel,
        "event_id": epoch + ":1",
        "attempt": 1,
        "sent_at": timestamp,
        "event": {
            "type": "event",
            "schema": 1,
            "org_uuid": org,
            "channel_id": channel,
            "epoch": epoch,
            "sequence": "1",
            "event_id": epoch + ":1",
            "cursor": _cursor(),
            "actor_id": actor,
            "kind": "message",
            "state": {
                "schema": 1,
                "org_uuid": org,
                "channel_id": channel,
                "message_id": "webhook-message",
                "sender_id": actor,
                "created_at": timestamp,
                "thread_root_id": None,
                "priority": "standard",
                "original_body": "Hello webhook",
                "body": "Hello webhook",
                "revision": 1,
                "updated_at": timestamp,
                "deleted": False,
                "reaction_revision": 0,
                "reactions": {},
                "attachments": [],
            },
        },
        **overrides,
    }


def dumps(value: Any, indent: int | None = None) -> str:
    """``JSON.stringify`` (compact, non-ASCII preserved)."""
    if indent is not None:
        return json.dumps(value, ensure_ascii=False, indent=indent)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def signature(raw: str | bytes, keys: list[str] | None = None, sent_at: int = timestamp) -> str:
    body = raw.encode("utf-8") if isinstance(raw, str) else raw
    header = "t=" + str(sent_at)
    for key in keys or [secret]:
        digest = hmac.new(bytes.fromhex(key[6:]), str(sent_at).encode() + b"." + body, hashlib.sha256).hexdigest()
        header += ",v1=" + digest
    return header


def subscription(**overrides: Any) -> dict[str, Any]:
    return {
        "id": subscription_id,
        "org_uuid": org,
        "url": "https://receiver.example.invalid/humchat",
        "status": "active",
        "secret_version": 1,
        "previous_secret_expires_at": None,
        "created_by": actor,
        "created_at": timestamp,
        "updated_at": timestamp,
        "channels": [{"channel_id": channel, "epoch": epoch, "sequence": "1", "status": "active", "last_error": None}],
        **overrides,
    }


def delivery(**overrides: Any) -> dict[str, Any]:
    return {
        "id": delivery_id,
        "subscription_id": subscription_id,
        "channel_id": channel,
        "epoch": epoch,
        "sequence": "1",
        "event_id": epoch + ":1",
        "status": "dead",
        "attempts": 8,
        "next_attempt_at": timestamp,
        "created_at": timestamp,
        "updated_at": timestamp,
        "last_status": 503,
        "last_error": "HTTP_REJECTED",
        **overrides,
    }


capabilities: dict[str, Any] = {
    "sdk_protocol_version": 1,
    "authentication": {
        "user_sessions": True,
        "service_identities": False,
        "api_key_exchange": False,
        "app_binding_required": False,
        "mock_authentication": True,
    },
    "messaging": {
        "send_receipt_versions": [1],
        "message_state_versions": [1],
        "history_versions": [2],
        "durable_replay": True,
        "event_protocol_versions": [1],
        "websocket_scope": "channel",
        "outgoing_webhooks": {"schema_versions": [1], "max_channels": 50, "workspace_channels_only": True},
    },
}
