"""Port of test/unit/sdk-key-config.test.mjs. Synthetic fixtures only; configuration never performs IO."""

from __future__ import annotations

import builtins
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest

from helpers import MockServer, Recorded, json_response, text_response
from humchat import ConfigurationError, HumChat, UnsupportedFeatureError, ValidationError
from humchat._util import parse_url

ORG_ID = "01234567-89ab-cdef-0123-456789abcdef"
SDK_KEY = "hsk_synthetic_phase2_config_secret"
ACT_AS = "synthetic.actor@example.invalid"
TOKEN = "synthetic-phase2-provider-token"


async def token_provider(_: Any) -> dict[str, str]:
    return {"token": TOKEN}


def auth(**extra: Any) -> dict[str, Any]:
    return {"sdk_key": SDK_KEY, "act_as": ACT_AS, **extra}


def options(**extra: Any) -> dict[str, Any]:
    return {"org_id": ORG_ID, "base_url": "https://humchat.example.invalid", "auth": auth(), **extra}


def observable(value: Any) -> str:
    """Python counterparts of ``inspect(value, {showHidden})`` and ``JSON.stringify(value)``."""
    parts = [repr(value), str(value)]
    try:
        parts.append(repr(vars(value)))
    except TypeError:
        pass
    if isinstance(value, BaseException):
        parts.append(repr(value.args))
        parts.append(repr(value.__cause__))
    return "\n".join(parts)


def assert_no_credentials(value: str, credentials: list[str] | None = None) -> None:
    for credential in credentials if credentials is not None else [SDK_KEY, ACT_AS]:
        assert credential not in value, "Credential must not appear in an observable representation"


@pytest.fixture
async def clients() -> AsyncIterator[list[HumChat]]:
    created: list[HumChat] = []
    yield created
    for client in created:
        await client.close()


def test_sdk_key_auth_and_token_provider_are_mutually_exclusive_including_explicit_none_fields() -> None:
    for extra in [
        {"token_provider": token_provider},
        {"token_provider": None},
        {"token_provider": token_provider, "auth": None},
    ]:
        with pytest.raises(ValidationError):
            HumChat(**options(**extra))
    with pytest.raises(ValidationError):
        HumChat(org_id=ORG_ID, base_url="https://humchat.example.invalid", token_provider=token_provider, auth=None)
    with pytest.raises(ConfigurationError):
        HumChat(org_id=ORG_ID, base_url="https://humchat.example.invalid")
    with pytest.raises(ConfigurationError):
        HumChat(org_id=ORG_ID, base_url="https://humchat.example.invalid", token_provider=None)


def test_top_level_api_key_remains_explicitly_unsupported_in_either_authentication_mode() -> None:
    for api_key in ["synthetic-unsupported-api-key", None]:
        with pytest.raises(UnsupportedFeatureError):
            HumChat(**options(api_key=api_key))
        with pytest.raises(UnsupportedFeatureError):
            HumChat(org_id="legacy-org", base_url="http://example.invalid", token_provider=token_provider, api_key=api_key)


def test_sdk_key_auth_rejects_malformed_nested_configuration_and_unknown_fields_without_echoing_credentials() -> None:
    for nested in [
        None,
        False,
        42,
        "synthetic-invalid-auth",
        [],
        {},
        {"sdk_key": SDK_KEY},
        {"act_as": ACT_AS},
        auth(sdk_key=None),
        auth(act_as=None),
        auth(api_key="synthetic-wrong-field"),
        auth(token_provider=token_provider),
        auth(misspelled=True),
    ]:
        with pytest.raises(ValidationError) as caught:
            HumChat(**options(auth=nested))
        assert_no_credentials(observable(caught.value))


async def test_sdk_keys_require_a_nonempty_header_safe_base64url_suffix_and_at_most_128_total_characters(clients: list[HumChat]) -> None:
    for key in ["hsk_a", "hsk_Aa0_-", "hsk_" + "a" * 124]:
        client = HumChat(**options(auth=auth(sdk_key=key)))
        clients.append(client)
        assert client.get_config().authentication == "sdk_key"
    for key in [
        None,
        17,
        "",
        "hsk_",
        "HSK_synthetic",
        "synthetic",
        "hsk_" + "a" * 125,
        f" {SDK_KEY}",
        f"{SDK_KEY} ",
        "hsk_one two",
        "hsk_one\ttwo",
        "hsk_one\r\ntwo",
        "hsk_one\u0000two",
        "hsk_one\u007ftwo",
        "hsk_one+two",
        "hsk_one/two",
        "hsk_one=two",
        "hsk_one.two",
        "hsk_é",
    ]:
        with pytest.raises(ValidationError) as caught:
            HumChat(**options(auth=auth(sdk_key=key)))
        if isinstance(key, str) and len(key) > 10:
            assert_no_credentials(observable(caught.value), [key, ACT_AS])


async def test_sdk_key_actor_emails_accept_server_normalization_and_enforce_shape_and_a_254_byte_limit(clients: list[HumChat]) -> None:
    maximum = "a" * 242 + "@example.com"
    assert len(maximum.encode("utf-8")) == 254
    for email in [ACT_AS, " \tSYNTHETIC.Actor@EXAMPLE.INVALID\r\n", maximum, "ÄCTOR@example.invalid"]:
        client = HumChat(**options(auth=auth(act_as=email)))
        clients.append(client)
        assert client.get_config().authentication == "sdk_key"
        normalized = "".join(ch.lower() if "A" <= ch <= "Z" else ch for ch in email.strip())
        assert_no_credentials(observable(client), [email, normalized])
    for email in [
        None,
        42,
        "",
        " \t ",
        "missing-at.example.invalid",
        "@example.invalid",
        "actor@",
        "actor@@example.invalid",
        "actor@localhost",
        "actor@.invalid",
        "actor@example.",
        "actor@example..invalid",
        "actor name@example.invalid",
        "actor@exam ple.invalid",
        "actor\nname@example.invalid",
        "actor\u0000@example.invalid",
        "actor\u007f@example.invalid",
        "actor name@example.invalid",
        "actor\u0085name@example.invalid",
        "actor\u009fname@example.invalid",
        "a" + maximum,
        "é" * 122 + "@example.com",
    ]:
        with pytest.raises(ValidationError) as caught:
            HumChat(**options(auth=auth(act_as=email)))
        assert_no_credentials(observable(caught.value), [SDK_KEY])
        if isinstance(email, str) and len(email) > 10:
            assert_no_credentials(observable(caught.value), [email])


async def test_sdk_key_organizations_require_canonical_uuid_structure_normalize_case_and_allow_any_uuid_version(clients: list[HumChat]) -> None:
    for value in [ORG_ID, ORG_ID.upper(), "00000000-0000-0000-0000-000000000000"]:
        client = HumChat(**options(org_id=value))
        clients.append(client)
        assert client.get_config().org_id == value.lower()
    for value in [
        None,
        123,
        "",
        "legacy-org",
        f" {ORG_ID}",
        f"{ORG_ID} ",
        f"{ORG_ID}\n",
        ORG_ID.replace("-", ""),
        f"{{{ORG_ID}}}",
        f"urn:uuid:{ORG_ID}",
        "g1234567-89ab-cdef-0123-456789abcdef",
    ]:
        with pytest.raises(ValidationError):
            HumChat(**options(org_id=value))


async def test_sdk_key_auth_requires_https_except_the_three_literal_loopback_hosts(clients: list[HumChat]) -> None:
    for base_url in [
        "https://humchat.example.invalid",
        "http://localhost",
        "http://localhost:43123/",
        "http://127.0.0.1:43123",
        "http://[::1]:43123/",
    ]:
        client = HumChat(**options(base_url=base_url))
        clients.append(client)
        assert client.get_config().base_url == parse_url(base_url).origin
    for base_url in [
        "http://example.invalid",
        "http://192.168.1.2",
        "http://0.0.0.0",
        "http://127.0.0.2",
        "http://localhost.example.invalid",
        "http://localhost.",
        "http://127.1",
        "http://2130706433",
        "http://0x7f000001",
        "http://0177.0.0.1",
        "http://[0:0:0:0:0:0:0:1]",
        "http://[::ffff:127.0.0.1]",
    ]:
        with pytest.raises(ConfigurationError):
            HumChat(**options(base_url=base_url))


def test_sdk_key_auth_retains_the_existing_origin_only_url_restrictions() -> None:
    for base_url in [
        None,
        "not-a-url",
        "file:///synthetic",
        "https://user:synthetic@example.invalid",
        "https://example.invalid/api",
        "https://example.invalid/?key=synthetic",
        "https://example.invalid/#synthetic",
    ]:
        with pytest.raises(ConfigurationError):
            HumChat(**options(base_url=base_url))


def test_sdk_key_auth_rejects_browser_runtimes_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    # Python counterpart of the window/document/worker globals: a Pyodide/emscripten runtime.
    calls = 0

    def handler(_: Recorded) -> Any:
        nonlocal calls
        calls += 1
        raise RuntimeError("Unexpected browser fetch")

    monkeypatch.setattr(sys, "platform", "emscripten")
    with pytest.raises(UnsupportedFeatureError):
        HumChat(**options(transport=MockServer(handler).transport))
    assert calls == 0


async def test_a_browser_like_global_does_not_turn_the_backend_runtime_into_a_browser(monkeypatch: pytest.MonkeyPatch, clients: list[HumChat]) -> None:
    monkeypatch.setattr(builtins, "navigator", {"userAgent": "Node.js/22"}, raising=False)
    monkeypatch.setattr(builtins, "window", {}, raising=False)
    client = HumChat(**options())
    clients.append(client)
    assert client.get_config().authentication == "sdk_key"


async def test_sdk_key_construction_is_lazy_and_credentials_are_absent_from_public_config_and_inspection() -> None:
    calls = 0

    def handler(_: Recorded) -> Any:
        nonlocal calls
        calls += 1
        raise RuntimeError("Unexpected configuration fetch")

    input_auth = auth(act_as=" SYNTHETIC.Actor@EXAMPLE.INVALID ")
    original = dict(input_auth)
    client = HumChat(**options(auth=input_auth, transport=MockServer(handler).transport))
    safe = client.get_config()
    assert safe.authentication == "sdk_key"
    assert safe.org_id == ORG_ID
    assert safe.timeout_ms == 10_000
    assert safe.retry["max_retries"] == 3
    assert input_auth == original, "Normalization must not modify caller-owned options"
    for field in ["auth", "sdk_key", "act_as", "token_provider"]:
        assert not hasattr(safe, field)
    for field in ["auth", "sdk_key", "act_as"]:
        assert not hasattr(client.realtime.config, field)
    assert client.realtime.config.token_provider is None
    secrets = [SDK_KEY, ACT_AS, original["act_as"], original["act_as"].strip()]
    for value in [safe, client, client.realtime.config]:
        assert_no_credentials(observable(value), secrets)
    safe.retry["max_retries"] = 8
    safe.realtime["max_channels"] = 1
    safe.recovery["max_pages"] = 1
    assert client.get_config().retry["max_retries"] == 3
    assert client.get_config().realtime["max_channels"] == 50
    assert client.get_config().recovery["max_pages"] == 3
    assert calls == 0
    await client.close()
    assert calls == 0


async def test_sdk_key_configuration_stays_lazy_until_the_first_operation_attempts_the_humchat_exchange(clients: list[HumChat]) -> None:
    calls = 0

    def handler(request: Recorded) -> Any:
        nonlocal calls
        calls += 1
        assert request.path == "/api/sdk/auth/user-token"
        assert request.headers.get("X-HumChat-Sdk-Key") == SDK_KEY
        assert "authorization" not in request.headers
        return text_response("Synthetic unavailable endpoint", 404)

    client = HumChat(**options(retry={"max_retries": 0}, transport=MockServer(handler).transport))
    clients.append(client)
    assert calls == 0
    with pytest.raises(UnsupportedFeatureError) as caught:
        await client.connect()
    assert_no_credentials(observable(caught.value))
    assert calls == 1


async def test_token_provider_preserves_opaque_organization_ids_http_origins_laziness_and_normal_authenticated_requests(
    clients: list[HumChat],
) -> None:
    legacy_org = "org-mock-1"
    acquisitions = 0
    paths: list[str] = []

    async def provider(context: Any) -> dict[str, str]:
        nonlocal acquisitions
        acquisitions += 1
        assert context.reason == "initial"
        return {"token": TOKEN}

    def handler(request: Recorded) -> Any:
        paths.append(request.path)
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        assert "x-humchat-sdk-key" not in request.headers
        if request.path == "/me":
            return json_response({"user_uuid": "synthetic-actor", "org": legacy_org, "email": ACT_AS, "roles": ["member"]})
        if request.path == "/api/sdk/capabilities":
            return json_response(
                {
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
            )
        assert request.path == "/api/org/members"
        return json_response([])

    client = HumChat(org_id=legacy_org, base_url="http://humchat.example.invalid", token_provider=provider, transport=MockServer(handler).transport)
    clients.append(client)
    assert client.get_config().authentication == "token_provider"
    assert client.get_config().org_id == legacy_org
    assert acquisitions == 0
    assert paths == []
    assert (await client.connect()).actor_id == "synthetic-actor"
    assert (await client.get_capabilities()).protocol_version == 1
    assert await client.members.list() == []
    assert acquisitions == 1
    assert paths == ["/me", "/api/sdk/capabilities", "/api/org/members"]
