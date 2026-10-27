"""Port of test/unit/webhook-verification.test.mjs."""

from __future__ import annotations

import datetime
import hashlib
import hmac
import math
from collections.abc import Callable
from typing import Any

import pytest

from humchat import WebhookVerificationError, verify_webhook
from webhook_support import channel, delivery_id, dumps, envelope, org, previous_secret, secret, signature, subscription_id, timestamp

OPTIONS: dict[str, Any] = {
    "now": timestamp,
    "expected_org_id": org,
    "expected_channel_id": channel,
    "expected_subscription_id": subscription_id,
}


def invalid(work: Callable[[], Any]) -> None:
    with pytest.raises(WebhookVerificationError) as caught:
        work()
    error = caught.value
    assert error.code == "WEBHOOK_VERIFICATION"
    assert error.message == "Webhook verification failed"
    assert error.__cause__ is None
    assert secret not in repr(error) and secret not in str(error)


def test_webhook_verification_authenticates_exact_bytes_and_returns_normalized_scoped_durable_data() -> None:
    value = envelope()
    value["event"]["state"]["body"] = value["event"]["state"]["original_body"] = "Unicode webhook: café 🌍"
    raw = dumps(value)
    header = signature(raw)
    for body in [raw, raw.encode("utf-8"), bytearray(raw.encode("utf-8"))]:
        verified = verify_webhook(body, header, secret, **OPTIONS)
        assert verified.subscription_id == subscription_id
        assert verified.delivery_id == delivery_id
        assert verified.org_id == org
        assert verified.channel_id == channel
        assert verified.sent_at == timestamp
        assert verified.event.state.body == value["event"]["state"]["body"]
        assert verified.event.state.id == "webhook-message"
        assert verified.event.event_id == verified.event_id
        assert verified.event.sequence == "1"
    padded = b"padding" + raw.encode("utf-8") + b"tail"
    assert verify_webhook(memoryview(padded)[7:-4], header, secret, **OPTIONS).event_id == value["event_id"]


def test_rotation_verifies_either_of_two_signatures_with_either_accepted_current_previous_key() -> None:
    raw = dumps(envelope())
    dual = signature(raw, [secret, previous_secret])
    for keys in [secret, previous_secret, [secret, previous_secret], [previous_secret, secret]]:
        assert verify_webhook(raw, dual, keys, **OPTIONS).attempt == 1
    assert verify_webhook(raw, signature(raw, [previous_secret]), [secret, previous_secret], **OPTIONS).attempt == 1
    invalid(lambda: verify_webhook(raw, signature(raw, ["whsec_" + "33" * 32]), [secret, previous_secret], **OPTIONS))
    literal_key = "t=" + str(timestamp) + ",v1=" + hmac.new(secret.encode(), (str(timestamp) + "." + raw).encode(), hashlib.sha256).hexdigest()
    invalid(lambda: verify_webhook(raw, literal_key, secret, **OPTIONS))


def test_raw_body_tampering_normalization_and_invalid_utf8_fail_before_becoming_events() -> None:
    raw = dumps(envelope())
    header = signature(raw)
    for body in [raw + " ", raw.replace("Hello", "Changed"), dumps(envelope(), indent=2)]:
        invalid(lambda body=body: verify_webhook(body, header, secret, **OPTIONS))
    bad_bodies: list[str | bytes] = [bytes([0xFF, 0xFE]), "{bad json", "null", "[]"]
    for bad in bad_bodies:
        invalid(lambda bad=bad: verify_webhook(bad, signature(bad), secret, **OPTIONS))
    invalid(lambda: verify_webhook(envelope(), header, secret, **OPTIONS))  # type: ignore[arg-type]


def test_timestamp_tolerance_is_symmetric_and_strict_at_the_configured_boundary() -> None:
    for delta in [-300, 300]:
        raw = dumps(envelope(sent_at=timestamp + delta))
        assert verify_webhook(raw, signature(raw, [secret], timestamp + delta), secret, **OPTIONS).sent_at == timestamp + delta
    for delta in [-301, 301]:
        raw = dumps(envelope(sent_at=timestamp + delta))
        invalid(lambda raw=raw, delta=delta: verify_webhook(raw, signature(raw, [secret], timestamp + delta), secret, **OPTIONS))
    raw = dumps(envelope())
    assert verify_webhook(raw, signature(raw), secret, **{**OPTIONS, "tolerance_seconds": 0}).attempt == 1
    invalid(lambda: verify_webhook(raw, signature(raw), secret, **{**OPTIONS, "now": timestamp + 1, "tolerance_seconds": 0}))


def test_duplicate_timestamps_malformed_or_excessive_signatures_invalid_secrets_and_options_fail_generically() -> None:
    raw = dumps(envelope())
    header = signature(raw)
    digest = header.split("v1=")[1]
    ts = str(timestamp)
    for invalid_header in [
        None,
        "",
        header + ",t=" + ts,
        header + ",v1=" + digest + ",v1=" + digest,
        "t=0" + ts + ",v1=" + digest,
        "t=+123,v1=" + digest,
        "t=NaN,v1=" + digest,
        "t=" + ts + ",v1=" + digest.upper(),
        "t=" + ts + ",v1=" + digest[1:],
        "t=" + ts + ",v2=" + digest,
        header + ",other=value",
        " " + header,
    ]:
        invalid(lambda h=invalid_header: verify_webhook(raw, h, secret, **OPTIONS))  # type: ignore[arg-type]
    for keys in ["", "whsec_" + "GG" * 32, "whsec_" + "11" * 31, [], [secret, secret], [secret, previous_secret, secret], {}]:
        invalid(lambda keys=keys: verify_webhook(raw, header, keys, **OPTIONS))  # type: ignore[arg-type]
    for extra in [
        {"now": datetime.datetime.now()},
        {"now": math.inf},
        {"now": "1792370346"},
        {"now": -1},
        {"tolerance_seconds": -1},
        {"tolerance_seconds": 3601},
        {"max_body_bytes": 0},
        {"max_body_bytes": 8 * 1024 * 1024 + 1},
    ]:
        invalid(lambda extra=extra: verify_webhook(raw, header, secret, **{**OPTIONS, **extra}))
    # Python deviation: an unknown keyword is a TypeError at the call site, before verification.
    with pytest.raises(TypeError):
        verify_webhook(raw, header, secret, **{**OPTIONS, "unknown": True})


def test_body_size_limits_are_measured_in_bytes_before_allocation_parsing() -> None:
    raw = dumps(envelope())
    header = signature(raw)
    size = len(raw.encode("utf-8"))
    assert verify_webhook(raw, header, secret, **{**OPTIONS, "max_body_bytes": size}).attempt == 1
    invalid(lambda: verify_webhook(raw, header, secret, **{**OPTIONS, "max_body_bytes": size - 1}))
    invalid(lambda: verify_webhook(bytes(8 * 1024 * 1024 + 1), header, secret, **OPTIONS))
    invalid(lambda: verify_webhook("", header, secret, **OPTIONS))


def _set(path: list[str], value: Any) -> Callable[[dict[str, Any]], None]:
    def apply(target: dict[str, Any]) -> None:
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return apply


def _increment_sent_at(value: dict[str, Any]) -> None:
    value["sent_at"] += 1


def test_valid_signatures_cannot_cross_expected_installation_scope_or_contradict_their_nested_event() -> None:
    raw = dumps(envelope())
    header = signature(raw)
    for extra in [{"expected_org_id": "other-org"}, {"expected_subscription_id": delivery_id}, {"expected_channel_id": org + ":team:other"}]:
        invalid(lambda extra=extra: verify_webhook(raw, header, secret, **{**OPTIONS, **extra}))
    corruptions: list[Callable[[dict[str, Any]], None]] = [
        _set(["schema"], 2),
        _set(["attempt"], 0),
        _set(["attempt"], 9),
        _increment_sent_at,
        _set(["subscription_id"], "not-a-uuid"),
        _set(["delivery_id"], "not-a-uuid"),
        _set(["org_uuid"], "other-org"),
        _set(["event_id"], "other:1"),
        _set(["event", "org_uuid"], "other-org"),
        _set(["event", "channel_id"], org + ":team:other"),
        _set(["event", "state", "channel_id"], org + ":team:other"),
        _set(["event", "sequence"], "2"),
        _set(["event", "state", "revision"], 2),
    ]
    for corrupt in corruptions:
        value = envelope()
        corrupt(value)
        body = dumps(value)
        invalid(lambda body=body: verify_webhook(body, signature(body), secret, now=timestamp))
