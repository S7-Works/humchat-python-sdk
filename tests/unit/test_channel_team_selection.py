"""Port of test/unit/channel-team-selection.test.mjs."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from helpers import MockServer, Recorded, json_response, rejects, text_response
from humchat import HumChat, NotFoundError, ProtocolError

ORG_ID = "01234567-89ab-cdef-0123-456789abcdef"
TEAM_ID = "team:Operations.v2"
ACTOR_ID = "synthetic-channel-reader"
EMAIL = "synthetic.channel.reader@example.invalid"
TOKEN = "synthetic-channel-reader-token"
SDK_KEY = "hsk_synthetic_channel_reader_key"
CAPABILITIES = {
    "sdk_protocol_version": 1,
    "authentication": {
        "user_sessions": True,
        "service_identities": False,
        "api_key_exchange": False,
        "app_binding_required": False,
        "mock_authentication": False,
        "sdk_key_user_tokens": {"endpoint": "/api/sdk/auth/user-token", "key_header": "X-HumChat-Sdk-Key", "configured": True},
    },
    "messaging": {
        "send_receipt_versions": [1],
        "message_state_versions": [1],
        "history_versions": [2],
        "durable_replay": False,
        "websocket_scope": "channel",
    },
}


def response_for_team(selected: str = TEAM_ID) -> dict[str, Any]:
    return {
        "org_uuid": ORG_ID,
        "team_uuid": selected,
        "team_name": "Synthetic team",
        "channels": [
            {
                "channel_id": f"{ORG_ID}:{selected}:general",
                "org_uuid": ORG_ID,
                "team_uuid": selected,
                "slug": "general",
                "display_name": "General",
                "is_private": False,
            }
        ],
    }


@pytest.fixture
async def clients() -> AsyncIterator[list[HumChat]]:
    made: list[HumChat] = []
    yield made
    for client in made:
        await client.close()


def fixture(made: list[HumChat], authentication: str, handle_channels: Callable[[Recorded], Any]) -> tuple[HumChat, MockServer]:
    def handler(request: Recorded) -> Any:
        if request.path == "/api/sdk/auth/user-token":
            assert authentication == "sdk_key"
            assert request.method == "POST"
            assert request.headers.get("x-humchat-sdk-key") == SDK_KEY
            assert "authorization" not in request.headers
            return json_response({"access_token": TOKEN, "token_type": "Bearer", "expires_in": 900})
        assert request.method == "GET"
        assert request.headers.get("authorization") == f"Bearer {TOKEN}"
        assert "x-humchat-sdk-key" not in request.headers
        if request.path == "/me":
            return json_response(
                {
                    "user_uuid": ACTOR_ID,
                    "id": 1,
                    "org": ORG_ID,
                    "email": EMAIL,
                    "roles": ["member"],
                    "scopes": [],
                    "expires_at": int(time.time()) + 900,
                    "app_uuid": None,
                }
            )
        if request.path == "/api/sdk/capabilities":
            return json_response(CAPABILITIES)
        assert request.path == "/api/workspace/channels", "Channel lookup must not call fallback routes"
        return handle_channels(request)

    server = MockServer(handler)

    async def provider(_: Any) -> dict[str, str]:
        return {"token": TOKEN}

    auth: dict[str, Any] = {"auth": {"sdk_key": SDK_KEY, "act_as": EMAIL}} if authentication == "sdk_key" else {"token_provider": provider}
    client = HumChat(
        org_id=ORG_ID,
        base_url="https://humchat.example.invalid",
        retry={"max_retries": 2, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 5_000},
        transport=server.transport,
        **auth,
    )
    made.append(client)
    return client, server


AUTHENTICATIONS = ["token_provider", "sdk_key"]


@pytest.mark.parametrize("authentication", AUTHENTICATIONS)
async def test_channel_lookup_sends_the_exact_selected_team_as_one_encoded_query(clients: list[HumChat], authentication: str) -> None:
    def channels(request: Recorded) -> Any:
        assert list(request.url.params.multi_items()) == [("team_uuid", TEAM_ID)]
        assert request.url.query == b"team_uuid=team%3AOperations.v2"
        return json_response(response_for_team())

    client, server = fixture(clients, authentication, channels)
    result = await client.channels.list(team_id=TEAM_ID)
    assert result.org_id == ORG_ID
    assert result.team_id == TEAM_ID
    assert result.channels[0].team_id == TEAM_ID
    assert result.channels[0].id == f"{ORG_ID}:{TEAM_ID}:general"
    assert server.count("/api/workspace/channels") == 1


@pytest.mark.parametrize("authentication", AUTHENTICATIONS)
async def test_omitted_team_remains_server_selected_without_client_discovery(clients: list[HumChat], authentication: str) -> None:
    server_selected = f"{ORG_ID}-home"

    def channels(request: Recorded) -> Any:
        assert request.url.query == b""
        return json_response(response_for_team(server_selected))

    client, server = fixture(clients, authentication, channels)
    assert (await client.channels.list()).team_id == server_selected
    assert server.count("/api/workspace/channels") == 1


@pytest.mark.parametrize("authentication", AUTHENTICATIONS)
async def test_missing_workspace_remains_not_found_with_no_retry_or_fallback_writes(clients: list[HumChat], authentication: str) -> None:
    private_body = "Synthetic workspace lookup detail must stay private"

    def channels(request: Recorded) -> Any:
        assert request.url.query == b""
        return text_response(private_body, 404)

    client, server = fixture(clients, authentication, channels)
    error = await rejects(client.channels.list(), NotFoundError)
    assert isinstance(error, NotFoundError)
    assert error.status == 404
    assert error.retryable is False
    assert private_body not in str(error)
    expected = (["POST /api/sdk/auth/user-token"] if authentication == "sdk_key" else []) + [
        "GET /me",
        "GET /api/sdk/capabilities",
        "GET /api/workspace/channels",
    ]
    assert [f"{request.method} {request.path}" for request in server.requests] == expected


def _org(value: dict[str, Any]) -> None:
    value["org_uuid"] = "other-org"


def _team(value: dict[str, Any]) -> None:
    value["team_uuid"] = "other-team"
    value["channels"] = []


def _channel_org(value: dict[str, Any]) -> None:
    value["channels"][0]["org_uuid"] = "other-org"


def _channel_team(value: dict[str, Any]) -> None:
    value["channels"][0]["team_uuid"] = "other-team"


@pytest.mark.parametrize(
    "change",
    [_org, _team, _channel_org, _channel_team],
    ids=["top-level organization differs", "selected team differs", "channel organization differs", "channel team differs"],
)
async def test_channel_lookup_rejects_scope_drift(clients: list[HumChat], change: Callable[[dict[str, Any]], None]) -> None:
    invalid = response_for_team()
    change(invalid)
    client, server = fixture(clients, "sdk_key", lambda _request: json_response(invalid))
    await rejects(client.channels.list(team_id=TEAM_ID), ProtocolError)
    assert server.count("/api/workspace/channels") == 1
