"""Port of test/unit/config.test.mjs."""

from __future__ import annotations

import math
from typing import Any

import pytest

from helpers import MockServer, json_response, rejects, text_response
from humchat import ConfigurationError, HumChat, ProtocolError, UnsupportedFeatureError

CAPABILITIES = {
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
        "durable_replay": False,
        "websocket_scope": "channel",
    },
}


async def provider(_: Any) -> dict[str, str]:
    return {"token": "fake-config-secret"}


def config(**extra: Any) -> dict[str, Any]:
    return {"org_id": "org-mock-1", "base_url": "http://127.0.0.1:43125", "token_provider": provider, **extra}


async def test_construction_is_lazy_and_safe_config_has_no_credentials_or_mutable_defaults() -> None:
    calls = 0

    def handler(_: Any) -> Any:
        nonlocal calls
        calls += 1
        raise RuntimeError("unexpected IO")

    client = HumChat(**config(transport=MockServer(handler).transport))
    safe = client.get_config()
    assert safe.timeout_ms == 10_000
    assert safe.retry["max_retries"] == 3
    assert safe.retry["total_timeout_ms"] == 30_000
    assert safe.realtime["transport"] == "channel"
    safe.retry["max_retries"] = 8
    assert client.get_config().retry["max_retries"] == 3
    assert "secret" not in repr(safe)
    assert "secret" not in repr(client)
    assert not hasattr(safe, "token_provider")
    assert calls == 0
    await client.close()


INVALID = [
        {"org_id": ""},
        {"org_id": " org "},
        {"base_url": None},
        {"base_url": "not a URL"},
        {"base_url": "file:///tmp/a"},
        {"base_url": "https://user:secret@example.test"},
        {"base_url": "https://example.test/api"},
        {"base_url": "https://example.test/?token=secret"},
        {"base_url": "https://example.test/#secret"},
        {"timeout_ms": 0},
        {"timeout_ms": math.inf},
        {"token_provider": None},
        {"transport": False},
        {"retry": None},
        {"retry": {"max_retries": -1}},
        {"retry": {"max_retries": 11}},
        {"retry": {"total_timeout_ms": math.nan}},
        {"retry": {"misspelled": 1}},
        {"recovery": {"mode": "unknown"}},
        {"realtime": {"max_channels": 0}},
        {"realtime": {"transport": "auto"}},
]


def test_invalid_or_unsupported_config_fails_before_fetching() -> None:
    for extra in INVALID:
        with pytest.raises(ConfigurationError):
            HumChat(**config(**extra))
    with pytest.raises(UnsupportedFeatureError):
        HumChat(**config(api_key="fake-key"))
    with pytest.raises(UnsupportedFeatureError):
        HumChat(**config(recovery={"mode": "best_effort"}, realtime={"transport": "multiplex"}))


async def test_identity_and_capabilities_are_copied_additive_server_fields_are_harmless() -> None:
    def handler(request: Any) -> Any:
        if request.path == "/me":
            return json_response(
                {
                    "user_uuid": "user-mock-123",
                    "org": "org-mock-1",
                    "email": "test@example.invalid",
                    "roles": ["admin"],
                    "scopes": [],
                    "expires_at": None,
                    "app_uuid": None,
                    "future": "field",
                }
            )
        return json_response({**CAPABILITIES, "future": "field"})

    client = HumChat(**config(transport=MockServer(handler).transport))
    identity = await client.connect()
    identity.actor_id = "different"
    identity.roles.append("invented")
    assert (await client.connect()).actor_id == "user-mock-123"
    assert (await client.connect()).roles == ["admin"]
    caps = await client.get_capabilities()
    caps.messaging.send_receipt_versions.append(99)
    assert (await client.get_capabilities()).messaging.send_receipt_versions == [1]
    assert caps.authentication.service_identities is False
    assert caps.messaging.multiplexed_events is None
    await client.close()


async def test_missing_and_incompatible_capability_negotiation_fails_explicitly() -> None:
    cases: list[tuple[Any, type]] = [
        (None, UnsupportedFeatureError),
        ({**CAPABILITIES, "messaging": {**CAPABILITIES["messaging"], "history_versions": [1]}}, UnsupportedFeatureError),
        ({"sdk_protocol_version": 1}, ProtocolError),
    ]
    for caps, expected in cases:

        def handler(request: Any, caps: Any = caps) -> Any:
            if request.path == "/me":
                return json_response({"user_uuid": "user-mock-123", "org": "org-mock-1", "email": "test@example.invalid", "roles": []})
            return text_response("not found", 404) if caps is None else json_response(caps)

        client = HumChat(**config(transport=MockServer(handler).transport))
        await rejects(client.connect(), expected)
        await client.close()
