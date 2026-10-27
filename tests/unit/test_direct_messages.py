"""Port of test/unit/direct-messages.test.mjs."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from helpers import MockServer, Recorded, json_response, rejects
from humchat import HumChat, OrgMember

ORG = "dm-org"
ACTOR = "dm-self"
PEER = "dm-peer"
IDENTITY = {"user_uuid": ACTOR, "org": ORG, "id": 1, "email": "self@example.invalid", "roles": ["member"], "scopes": [], "expires_at": None, "app_uuid": None}
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
        "durable_replay": True,
        "event_protocol_versions": [1],
        "websocket_scope": "channel",
    },
}


def conversation(identifier: str = "existing", **changes: Any) -> dict[str, Any]:
    return {
        "id": identifier,
        "channel_key": f"{ORG}:conv:{identifier}",
        "name": None,
        "kind": "dm",
        "member_count": 2,
        "my_role": "member",
        "members": [{"user_uuid": ACTOR, "role": "member"}, {"user_uuid": PEER, "role": "member"}],
        "last_message_at": None,
        **changes,
    }


async def provider(_: Any) -> dict[str, str]:
    return {"token": "synthetic-dm-token"}


@pytest.fixture
async def clients() -> AsyncIterator[list[HumChat]]:
    made: list[HumChat] = []
    yield made
    for client in made:
        await client.close()


def client(made: list[HumChat], route: Callable[[Recorded], Any]) -> tuple[HumChat, MockServer]:
    def handler(request: Recorded) -> Any:
        if request.path == "/me":
            return json_response(IDENTITY)
        if request.path == "/api/sdk/capabilities":
            return json_response(CAPABILITIES)
        return route(request)

    server = MockServer(handler)
    sdk = HumChat(
        org_id=ORG,
        base_url="http://127.0.0.1:43123",
        token_provider=provider,
        retry={"max_retries": 3, "initial_delay_ms": 1, "max_delay_ms": 2, "total_timeout_ms": 500},
        timeout_ms=200,
        transport=server.transport,
    )
    made.append(sdk)
    return sdk, server


async def test_members_normalize_the_roster_and_reject_duplicates_malformed_rows_and_foreign_scope(clients: list[HumChat]) -> None:
    rows: list[dict[str, Any]] = [{"user_uuid": PEER, "name": "Peer", "email": "peer@example.invalid", "role": "member", "status": "active"}]

    def route(request: Recorded) -> Any:
        assert request.path == "/api/org/members"
        return json_response(rows)

    sdk, _ = client(clients, route)
    assert await sdk.members.list() == [OrgMember(actor_id=PEER, name="Peer", email="peer@example.invalid", role="member", status="active")]
    rows = [*rows, *rows]
    await rejects(sdk.members.list(), "PROTOCOL")
    rows = [{**rows[0], "org_uuid": "foreign"}]
    await rejects(sdk.members.list(), "PROTOCOL")
    rows = [{"user_uuid": PEER}]
    await rejects(sdk.members.list(), "PROTOCOL")


async def test_open_direct_reuses_only_an_unnamed_exact_peer_pair_and_send_direct_uses_its_routing_key(clients: list[HumChat]) -> None:
    sent: list[Any] = []

    def route(request: Recorded) -> Any:
        if request.path == "/api/conversations":
            assert request.method == "GET"
            other = conversation("other", members=[{"user_uuid": ACTOR, "role": "member"}, {"user_uuid": "other", "role": "member"}])
            return json_response([conversation("named", name="Separate group"), other, conversation()])
        assert request.path == "/api/messages/send"
        value = request.json()
        sent.append(value)
        assert request.headers.get("X-Idempotency-Key") == "request-key"
        return json_response({"status": "ACK", "message_id": value["id"], "created_at": 1000, "attachment_count": 0, "attachment_manifest_blake3": "0" * 64})

    sdk, server = client(clients, route)
    result = await sdk.messages.send_direct(recipient_id=PEER, content="hello", id="message-one", idempotency_key="request-key")
    assert result.conversation.id == "existing"
    assert result.receipt.id == "message-one"
    assert sent[0]["channel_id"] == f"{ORG}:conv:existing"
    assert len([r for r in server.requests if r.path == "/api/conversations" and r.method == "POST"]) == 0


async def test_open_direct_creates_once_with_the_peer_and_validates_the_returned_participant_set(clients: list[HumChat]) -> None:
    returned = conversation("new")
    bodies: list[Any] = []

    def route(request: Recorded) -> Any:
        if request.method == "GET":
            return json_response([])
        bodies.append(json.loads(request.body or b"null"))
        return json_response(returned)

    sdk, _ = client(clients, route)
    assert (await sdk.conversations.open_direct(member_id=PEER)).channel_id == f"{ORG}:conv:new"
    assert bodies == [{"member_uuids": [PEER], "name": None}]
    returned = conversation("new", members=[{"user_uuid": ACTOR, "role": "member"}, {"user_uuid": "stranger", "role": "member"}])
    await rejects(sdk.conversations.create(member_ids=[PEER]), "PROTOCOL")


async def test_creation_never_retries_an_ambiguous_transport_failure_503_or_401(clients: list[HumChat]) -> None:
    for status in (None, 503, 401):
        posts = 0

        def route(request: Recorded, status: int | None = status) -> Any:
            nonlocal posts
            if request.method == "GET":
                return json_response([])
            posts += 1
            if status is None:
                raise RuntimeError("Synthetic fetch failure")
            return json_response({}, status)

        sdk, _ = client(clients, route)
        await rejects(sdk.conversations.open_direct(member_id=PEER))
        assert posts == 1


async def test_invalid_dm_intent_is_rejected_before_creation_and_caller_cannot_direct_message_itself(clients: list[HumChat]) -> None:
    def route(_: Recorded) -> Any:
        raise RuntimeError("No data requests expected")

    sdk, server = client(clients, route)
    await rejects(sdk.messages.send_direct(recipient_id=PEER, content=""), "VALIDATION")
    await rejects(sdk.conversations.create(member_ids=[PEER, PEER]), "VALIDATION")
    assert len(server.requests) == 0
    await rejects(sdk.conversations.open_direct(member_id=ACTOR), "VALIDATION")
    assert server.count("/api/conversations") == 0


async def test_direct_send_preserves_explicit_capability_negotiation_with_no_legacy_fallback(clients: list[HumChat]) -> None:
    def handler(request: Recorded) -> Any:
        return json_response(IDENTITY) if request.path == "/me" else json_response({}, 404)

    server = MockServer(handler)
    sdk = HumChat(org_id=ORG, base_url="http://127.0.0.1:43123", token_provider=provider, transport=server.transport)
    clients.append(sdk)
    await rejects(sdk.messages.send_direct(recipient_id=PEER, content="hello"), "UNSUPPORTED_FEATURE")
    assert server.paths() == ["/me", "/api/sdk/capabilities"]
