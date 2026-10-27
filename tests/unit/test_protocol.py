"""Port of test/unit/protocol.test.mjs."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable
from typing import Any

import pytest

from helpers import SAMPLES, MockServer, Recorded, json_response, rejects
from humchat import HumChat, MessageReference, ProtocolError, ValidationError

ORG_ID = SAMPLES["identity"]["org"]
CHANNEL_ID = SAMPLES["sendRequest"]["channel_id"]
STATE = SAMPLES["state"]
ULID = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
MAX_SAFE = 9_007_199_254_740_991
CAPS = {
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
    return {"token": "fixture-token"}


def client(handler: Callable[[Recorded], Any], identity: dict[str, Any] | None = None) -> HumChat:
    me = identity or SAMPLES["identity"]

    def route(request: Recorded) -> Any:
        if request.path == "/me":
            value = me
        elif request.path == "/api/sdk/capabilities":
            value = CAPS
        else:
            value = handler(request)
        return json_response(value)

    return HumChat(
        org_id=ORG_ID,
        base_url="https://sdk.example.test",
        token_provider=provider,
        retry={"max_retries": 0},
        transport=MockServer(route).transport,
    )


def cursor(message: dict[str, Any], thread: str | None = None) -> str:
    return json.dumps(
        {
            "schema": 2,
            "org_uuid": ORG_ID,
            "channel_id": CHANNEL_ID,
            "thread_root_id": thread,
            "created_at": message["created_at"],
            "message_id": message["message_id"],
        },
        separators=(",", ":"),
    ).encode().hex()


async def test_captured_channels_dms_and_versioned_state_normalize_into_public_fields() -> None:
    def handler(request: Recorded) -> Any:
        if request.path == "/api/workspace/channels":
            return {**SAMPLES["channels"], "future_field": True}
        if request.path == "/api/conversations":
            return [SAMPLES["conversation"]]
        return {**STATE, "future_field": {"added": True}}

    sdk = client(handler)
    listed = await sdk.channels.list(team_id=SAMPLES["channels"]["team_uuid"])
    assert listed.channels[0].id == SAMPLES["channels"]["channels"][0]["channel_id"]
    assert listed.team_id == SAMPLES["channels"]["team_uuid"]
    [dm] = await sdk.conversations.list()
    assert dm.channel_id == f"{ORG_ID}:conv:{dm.id}"
    assert dm.id != dm.channel_id
    message = await sdk.messages.get_state(channel_id=CHANNEL_ID, message_id=STATE["message_id"])
    assert message.body == STATE["body"]
    assert message.original_body == STATE["original_body"]
    assert message.reaction_revision == STATE["reaction_revision"]
    assert message.created_at == STATE["created_at"]
    assert message.deleted is True
    await sdk.close()


def _set(**changes: Any) -> Callable[[dict[str, Any]], None]:
    def modify(value: dict[str, Any]) -> None:
        for key, item in changes.items():
            value[key] = item

    return modify


def _self_thread(value: dict[str, Any]) -> None:
    value["thread_root_id"] = value["message_id"]


@pytest.mark.parametrize(
    "modify",
    [
        _set(org_uuid="other-org"),
        _set(channel_id=f"{ORG_ID}:other"),
        _set(message_id="other-message"),
        _set(schema=7),
        _set(revision=MAX_SAFE + 1),
        _set(reaction_revision=MAX_SAFE + 1),
        _self_thread,
        _set(reactions={"yes": ["a", "a"]}),
        _set(revision=1, deleted=False),
    ],
    ids=[
        "different organization",
        "different channel",
        "different message",
        "unknown schema",
        "unsafe revision",
        "unsafe reaction revision",
        "self-referential thread",
        "duplicate reaction actors",
        "initial revision with changed content",
    ],
)
async def test_rejects_state_with(modify: Callable[[dict[str, Any]], None]) -> None:
    invalid = copy.deepcopy(STATE)
    modify(invalid)
    sdk = client(lambda _request: invalid)
    await rejects(sdk.messages.get_state(channel_id=CHANNEL_ID, message_id=STATE["message_id"]), ProtocolError)
    await sdk.close()


async def test_channel_list_and_dm_response_cannot_cross_organization_boundaries() -> None:
    def handler(request: Recorded) -> Any:
        if request.path == "/api/conversations":
            return [{**SAMPLES["conversation"], "channel_key": f"other-org:conv:{SAMPLES['conversation']['id']}"}]
        return {**SAMPLES["channels"], "channels": [{**SAMPLES["channels"]["channels"][0], "org_uuid": "other-org"}]}

    sdk = client(handler)
    await rejects(sdk.channels.list(), ProtocolError)
    await rejects(sdk.conversations.list(), ProtocolError)
    await sdk.close()


async def test_send_allocates_ulid_and_idempotency_once_reply_preserves_an_existing_thread_root() -> None:
    requests: list[tuple[Any, Any]] = []

    def handler(request: Recorded) -> Any:
        body = request.json()
        requests.append((body, request.headers))
        return {**SAMPLES["sendReceipt"], "message_id": body["id"]}

    sdk = client(handler)
    sent = await sdk.messages.send(channel_id=CHANNEL_ID, content="hello")
    assert ULID.fullmatch(sent.id)
    assert sent.idempotency_key == sent.id
    assert requests[0][1].get("x-idempotency-key") == sent.id
    assert requests[0][1].get("x-zlack-message-receipt") == "v1"
    assert requests[0][0]["priority"] == "standard"
    assert requests[0][0]["thread_root_id"] is None
    await sdk.messages.reply(
        MessageReference(id="existing-reply", channel_id=CHANNEL_ID, thread_root_id="root"),
        content="reply",
        id="known-id",
        priority="important",
        idempotency_key="stable-key",
    )
    assert requests[1][0]["thread_root_id"] == "root"
    assert requests[1][0]["id"] == "known-id"
    assert requests[1][0]["priority"] == "important"
    assert requests[1][1].get("x-idempotency-key") == "stable-key"
    await sdk.close()


async def test_malformed_send_acknowledgments_retain_generated_intent_for_reconciliation() -> None:
    sdk = client(lambda _request: "ACK")
    error = await rejects(sdk.messages.send(channel_id=CHANNEL_ID, content="hello"), ProtocolError)
    assert isinstance(error, ProtocolError)
    assert error.message_id is not None and ULID.fullmatch(error.message_id)
    assert error.idempotency_key == error.message_id
    assert error.channel_id == CHANNEL_ID
    await sdk.close()


async def test_invalid_local_content_revisions_reaction_size_and_foreign_routing_fail_before_io() -> None:
    calls = 0

    def handler(_: Recorded) -> Any:
        nonlocal calls
        calls += 1
        raise RuntimeError("unexpected request")

    sdk = client(handler)
    await rejects(sdk.messages.send(channel_id=CHANNEL_ID, content="  "), ValidationError)
    await rejects(sdk.messages.send(channel_id=CHANNEL_ID, content="a" * 1_048_577), ValidationError)
    await rejects(sdk.messages.send(channel_id="other-org:channel", content="hello"), ValidationError)
    await rejects(sdk.messages.edit(channel_id=CHANNEL_ID, message_id="id", content="x", expected_revision=1.5), ValidationError)  # type: ignore[arg-type]
    await rejects(sdk.messages.set_reaction(channel_id=CHANNEL_ID, message_id="id", emoji="😀" * 9, present=True), ValidationError)
    await rejects(sdk.messages.history(channel_id=CHANNEL_ID, limit=201), ValidationError)
    assert calls == 0
    await sdk.close()


async def test_reaction_mutation_uses_strict_wire_fields_and_validates_receipt_actor_and_reflected_intent() -> None:
    fixture = SAMPLES["mutationReceipt"]
    wrong_actor = False
    wrong_intent = False

    def handler(request: Recorded) -> Any:
        body = request.json()
        assert list(body.keys()) == ["operation_id", "channel_id", "message_id", "mutation"]
        assert body["mutation"] == {"kind": "set_reaction", "emoji": "✅", "present": True}
        return {
            **fixture,
            "operation_id": body["operation_id"],
            "actor_id": "another-actor" if wrong_actor else fixture["actor_id"],
            "state": {**fixture["state"], "reactions": {}} if wrong_intent else fixture["state"],
        }

    sdk = client(handler, {**SAMPLES["identity"], "user_uuid": fixture["actor_id"]})
    params = {"channel_id": CHANNEL_ID, "message_id": fixture["state"]["message_id"], "emoji": "✅", "present": True}
    receipt = await sdk.messages.set_reaction(**params)
    assert receipt.actor_id == fixture["actor_id"]
    assert ULID.fullmatch(receipt.operation_id)
    wrong_actor = True
    await rejects(sdk.messages.set_reaction(**params), ProtocolError)
    wrong_actor = False
    wrong_intent = True
    await rejects(sdk.messages.set_reaction(**params), ProtocolError)
    await sdk.close()


async def test_history_iterates_older_pages_while_retaining_ascending_row_order_and_scoped_cursor() -> None:
    base = SAMPLES["history"]["messages"][0]
    older = {**base, "message_id": "older", "created_at": base["created_at"] - 1, "updated_at": base["created_at"] - 1}
    following = cursor(base)
    calls = 0

    def handler(request: Recorded) -> Any:
        nonlocal calls
        assert request.headers.get("x-zlack-message-state") == "v2"
        assert request.url.params.get("channel_id") == CHANNEL_ID
        assert "since_id" not in request.url.params
        calls += 1
        if calls == 1:
            return {**SAMPLES["history"], "messages": [base], "next_cursor": following}
        assert request.url.params.get("native_cursor") == following
        return {**SAMPLES["history"], "messages": [older], "next_cursor": None}

    sdk = client(handler)
    pages = [page async for page in sdk.messages.iterate_history(channel_id=CHANNEL_ID, limit=1)]
    assert [page.messages[0].id for page in pages] == [base["message_id"], "older"]
    assert calls == 2
    await sdk.close()


async def test_history_rejects_wrong_thread_scope_duplicate_rows_and_fabricated_cursor_boundaries() -> None:
    base = SAMPLES["history"]["messages"][0]
    invalid: Any = {**SAMPLES["history"], "messages": [base, base]}
    sdk = client(lambda _request: invalid)
    await rejects(sdk.messages.history(channel_id=CHANNEL_ID), ProtocolError)
    invalid = {**SAMPLES["threadHistory"]}
    await rejects(sdk.messages.history(channel_id=CHANNEL_ID), ProtocolError)
    invalid = {**SAMPLES["history"], "next_cursor": cursor({**base, "message_id": "skipped"})}
    await rejects(sdk.messages.history(channel_id=CHANNEL_ID), ProtocolError)
    invalid = {**SAMPLES["history"], "next_cursor": cursor(base)}

    async def exhaust() -> None:
        async for _ in sdk.messages.iterate_history(channel_id=CHANNEL_ID):
            pass

    await rejects(exhaust(), ProtocolError)
    await rejects(sdk.messages.history(channel_id=CHANNEL_ID, thread_root_id="wrong-thread", cursor=cursor(base)), ValidationError)
    await sdk.close()
