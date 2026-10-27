from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Sequence
from typing import Any

from .. import _clock
from .._util import is_safe_int, json_loads, utf8_len
from .._wire import Invalid, check, identity, literal, obj, required, safe_int, string
from ..errors import WebhookVerificationError
from ..realtime.durable_protocol import parse_event
from .types import WebhookEnvelope

WEBHOOK_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
WEBHOOK_SECRET = re.compile(r"^whsec_[0-9a-f]{64}$")
_SIGNATURE = re.compile(r"^t=(0|[1-9][0-9]{0,10}),v1=([0-9a-f]{64})(?:,v1=([0-9a-f]{64}))?$")
_MAX_BODY = 8 * 1024 * 1024


def webhook_uuid(value: Any) -> str:
    check(isinstance(value, str) and WEBHOOK_UUID.fullmatch(value) is not None)
    return value


def webhook_secret(value: Any) -> str:
    check(isinstance(value, str) and WEBHOOK_SECRET.fullmatch(value) is not None)
    return value


def _time(value: Any) -> int:
    return safe_int(value, 0, 99_999_999_999)


def _envelope(raw: Any) -> dict[str, Any]:
    v = obj(raw)
    literal(required(v, "schema"), 1)
    check("event" in v)
    return {
        "subscription_id": webhook_uuid(required(v, "subscription_id")),
        "delivery_id": webhook_uuid(required(v, "delivery_id")),
        "org_uuid": identity(required(v, "org_uuid")),
        "channel_id": identity(required(v, "channel_id"), 1024),
        "event_id": string(required(v, "event_id"), 1, 160),
        "attempt": safe_int(required(v, "attempt"), 1, 8),
        "sent_at": _time(required(v, "sent_at")),
        "event": v["event"],
    }


def verify_webhook(
    raw_body: bytes | bytearray | memoryview | str,
    signature_header: str,
    secrets: str | Sequence[str],
    *,
    expected_org_id: str | None = None,
    expected_subscription_id: str | None = None,
    expected_channel_id: str | None = None,
    tolerance_seconds: int | None = None,
    now: int | None = None,
    max_body_bytes: int | None = None,
) -> WebhookEnvelope:
    """Authenticate the exact received bytes before parsing; never pass a JSON-parsed object.

    ``secrets`` may hold the current and previous signing secret during rotation.
    ``tolerance_seconds`` defaults to 300 (maximum 3600). ``now`` is Unix seconds for
    deterministic tests. ``max_body_bytes`` may lower the hard 8 MiB limit, never raise it.
    Every failure raises the same :class:`WebhookVerificationError`.
    """
    try:
        if expected_org_id is not None:
            identity(expected_org_id)
        if expected_subscription_id is not None:
            webhook_uuid(expected_subscription_id)
        if expected_channel_id is not None:
            identity(expected_channel_id, 1024)
        if tolerance_seconds is not None:
            safe_int(tolerance_seconds, 0, 3600)
        if now is not None:
            _time(now)
        if max_body_bytes is not None:
            safe_int(max_body_bytes, 1, _MAX_BODY)
        keys = [secrets] if isinstance(secrets, str) else list(secrets) if isinstance(secrets, (list, tuple)) else None
        check(keys is not None and 1 <= len(keys) <= 2)
        assert keys is not None
        for key in keys:
            webhook_secret(key)
        check(len(set(keys)) == len(keys) and isinstance(signature_header, str) and len(signature_header) <= 256)
        signature = _SIGNATURE.fullmatch(signature_header)
        check(signature is not None)
        assert signature is not None
        timestamp = int(signature.group(1))
        current = int(_clock.now_ms() // 1000) if now is None else int(now)
        check(is_safe_int(current) and 0 <= current <= 99_999_999_999)
        check(abs(current - timestamp) <= (300 if tolerance_seconds is None else tolerance_seconds))
        if isinstance(raw_body, str):
            size = utf8_len(raw_body)
            # Buffer.from(string) encodes lone surrogates as U+FFFD; match those bytes.
            body = raw_body.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace").encode("utf-8")
        elif isinstance(raw_body, (bytes, bytearray, memoryview)):
            body = bytes(raw_body)  # Own the bytes used by both HMAC and parsing.
            size = len(body)
        else:
            raise Invalid
        check(1 <= size <= (_MAX_BODY if max_body_bytes is None else max_body_bytes))
        candidates = [bytes.fromhex(signature.group(2))]
        if signature.group(3):
            candidates.append(bytes.fromhex(signature.group(3)))
        matched = False
        for key in keys:
            digest = hmac.new(bytes.fromhex(key[6:]), signature.group(1).encode("ascii") + b"." + body, hashlib.sha256).digest()
            for candidate in candidates:
                matched |= hmac.compare_digest(digest, candidate)
        check(matched)
        envelope = _envelope(json_loads(body))
        check(
            envelope["sent_at"] == timestamp
            and envelope["channel_id"].startswith(envelope["org_uuid"] + ":")
            and (expected_org_id is None or expected_org_id == envelope["org_uuid"])
            and (expected_subscription_id is None or expected_subscription_id == envelope["subscription_id"])
            and (expected_channel_id is None or expected_channel_id == envelope["channel_id"])
        )
        event = parse_event(envelope["event"], envelope["org_uuid"], envelope["channel_id"])
        check(event.event_id == envelope["event_id"])
        return WebhookEnvelope(
            schema=1,
            subscription_id=envelope["subscription_id"],
            delivery_id=envelope["delivery_id"],
            org_id=envelope["org_uuid"],
            channel_id=envelope["channel_id"],
            event_id=envelope["event_id"],
            attempt=envelope["attempt"],
            sent_at=envelope["sent_at"],
            event=event,
        )
    except Exception:
        raise WebhookVerificationError() from None
