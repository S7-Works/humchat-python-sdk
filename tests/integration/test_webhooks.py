"""Port of test/integration/webhooks.test.mjs."""

from __future__ import annotations

import copy
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict
from typing import Any

import httpx
import pytest

from helpers import MockServer, Recorded, json_response, rejects
from humchat import HumChat, ProtocolError, UnsupportedFeatureError, ValidationError
from webhook_support import (
    actor,
    capabilities,
    channel,
    delivery,
    delivery_id,
    epoch,
    org,
    previous_secret,
    secret,
    subscription,
    subscription_id,
    timestamp,
)

CREATE_INPUT: dict[str, Any] = {"id": subscription_id, "url": "https://receiver.example.invalid/humchat", "channel_ids": [channel]}


def result(value: Any) -> dict[str, Any]:
    return {"schema": 1, "subscription": value}


class Fixture:
    def __init__(self, handler: Callable[[Recorded], httpx.Response], caps: Any = None) -> None:
        self.reasons: list[str] = []
        self.data: list[Recorded] = []

        def route(request: Recorded) -> httpx.Response:
            if request.path == "/me":
                return json_response({"user_uuid": actor, "org": org, "email": "webhook@example.invalid", "roles": ["admin"]})
            if request.path == "/api/sdk/capabilities":
                return json_response(caps if caps is not None else capabilities)
            self.data.append(request)
            return handler(request)

        self.server = MockServer(route)

        async def provider(context: Any) -> dict[str, str]:
            self.reasons.append(context.reason)
            return {"token": "synthetic-webhook-admin"}

        self.client = HumChat(
            org_id=org,
            base_url="http://127.0.0.1:43123",
            token_provider=provider,
            retry={"max_retries": 2, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 2000},
            transport=self.server.transport,
        )

    @property
    def requests(self) -> list[Recorded]:
        return self.server.requests


@pytest.fixture
async def fixtures() -> AsyncIterator[Callable[..., Fixture]]:
    created: list[Fixture] = []

    def make(handler: Callable[[Recorded], httpx.Response], caps: Any = None) -> Fixture:
        fixture = Fixture(handler, caps)
        created.append(fixture)
        return fixture

    yield make
    for fixture in created:
        await fixture.client.close()


def _no_secret(value: Any) -> bool:
    return secret not in repr(value) and secret not in str(value) and secret not in repr(getattr(value, "__dict__", {}))


async def test_webhook_management_maps_all_routes_methods_one_time_secrets_and_delivery_views(fixtures: Any) -> None:
    calls: list[Recorded] = []

    def handler(request: Recorded) -> httpx.Response:
        calls.append(request)
        path = request.path
        if path == "/api/webhooks" and request.method == "POST":
            assert request.json() == {"id": subscription_id, "url": CREATE_INPUT["url"], "channel_ids": [channel]}
            return json_response({**result(subscription(status="paused")), "signing_secret": secret})
        if path == "/api/webhooks":
            return json_response({"schema": 1, "org_uuid": org, "subscriptions": [subscription()]})
        if request.method == "DELETE":
            return json_response({"schema": 1, "id": subscription_id, "deleted": True})
        if path.endswith("/pause"):
            return json_response(result(subscription(status="paused")))
        if path.endswith("/resume"):
            return json_response(result(subscription()))
        if path.endswith("/rotate-secret"):
            return json_response({**result(subscription(secret_version=2, previous_secret_expires_at=timestamp + 300)), "signing_secret": previous_secret})
        if path.endswith("/replay"):
            return json_response({"schema": 1, "delivery": delivery(status="pending", attempts=0)})
        if path.endswith("/deliveries"):
            params = request.url.params
            assert params.get("status") == "dead"
            assert params.get("limit") == "10"
            assert params.get("before") == subscription_id
            return json_response({"schema": 1, "org_uuid": org, "deliveries": [delivery()], "next_before": delivery_id})
        return json_response({**result(subscription()), "signing_secret": secret})  # Public read must strip an unexpected secret.

    client = fixtures(handler).client
    created = await client.webhooks.create(**CREATE_INPUT)
    assert created.signing_secret == secret
    assert created.subscription.org_id == org
    assert created.subscription.status == "paused"
    assert created.subscription.channels[0].channel_id == channel
    caps = (await client.get_capabilities()).messaging.outgoing_webhooks
    assert caps is not None
    assert asdict(caps) == {"schema_versions": [1], "max_channels": 50, "workspace_channels_only": True}
    assert (await client.webhooks.list())[0].id == subscription_id
    fetched = await client.webhooks.get(subscription_id)
    assert fetched.id == subscription_id
    assert secret not in repr(asdict(fetched))
    assert (await client.webhooks.pause(subscription_id)).status == "paused"
    assert (await client.webhooks.resume(subscription_id)).status == "active"
    rotated = await client.webhooks.rotate_secret(subscription_id)
    assert rotated.signing_secret == previous_secret
    assert rotated.subscription.secret_version == 2
    page = await client.webhooks.deliveries(subscription_id, status="dead", limit=10, before=subscription_id)
    assert page.next_before == delivery_id
    assert page.deliveries[0].event_id == epoch + ":1"
    assert page.deliveries[0].last_status == 503
    assert (await client.webhooks.replay(subscription_id, delivery_id)).status == "pending"
    deleted = await client.webhooks.delete(subscription_id)
    assert (deleted.id, deleted.deleted) == (subscription_id, True)
    assert len(calls) == 9
    for call in calls:
        assert call.headers.get("authorization") == "Bearer synthetic-webhook-admin"


MUTATIONS: list[Callable[[HumChat], Any]] = [
    lambda client: client.webhooks.create(**CREATE_INPUT),
    lambda client: client.webhooks.pause(subscription_id),
    lambda client: client.webhooks.resume(subscription_id),
    lambda client: client.webhooks.rotate_secret(subscription_id),
    lambda client: client.webhooks.delete(subscription_id),
    lambda client: client.webhooks.replay(subscription_id, delivery_id),
]


# Node runs these as 18 subtests of one parent test; pytest flattens them into 18 tests.
@pytest.mark.parametrize("status", [401, 503, "network"])
@pytest.mark.parametrize("index", range(len(MUTATIONS)))
async def test_webhook_mutations_never_automatically_repeat_on_network_retryable_http_or_unauthorized_results(fixtures: Any, status: Any, index: int) -> None:
    writes = 0

    def handler(_: Recorded) -> httpx.Response:
        nonlocal writes
        writes += 1
        if status == "network":
            raise RuntimeError("Never copy this raw transport body")
        return json_response({"message": "Never copy this server body"}, status)

    fixture = fixtures(handler)
    await rejects(MUTATIONS[index](fixture.client), lambda error: error.code in ("AUTHENTICATION", "SERVER", "NETWORK"))
    assert writes == 1
    assert fixture.reasons == ["initial"]


async def test_an_ambiguous_create_can_be_reconciled_by_caller_id_and_a_new_secret_is_obtained_only_through_rotation(fixtures: Any) -> None:
    state = {"exists": False, "create_calls": 0, "rotations": 0}

    def handler(request: Recorded) -> httpx.Response:
        if request.path == "/api/webhooks" and request.method == "POST":
            state["exists"] = True
            state["create_calls"] += 1
            raise RuntimeError("Lost committed response")
        assert state["exists"]
        assert subscription_id in request.path
        if request.path.endswith("/rotate-secret"):
            state["rotations"] += 1
            return json_response({**result(subscription(secret_version=2)), "signing_secret": previous_secret})
        return json_response(result(subscription()))

    client = fixtures(handler).client
    await rejects(client.webhooks.create(**CREATE_INPUT), lambda error: error.code == "NETWORK")
    known = await client.webhooks.get(subscription_id)
    assert known.id == subscription_id
    assert not hasattr(known, "signing_secret")
    assert (await client.webhooks.rotate_secret(subscription_id)).signing_secret == previous_secret
    assert state["create_calls"] == 1
    assert state["rotations"] == 1


async def test_webhook_reads_retain_bounded_retry_and_management_authorization_errors_remain_typed(fixtures: Any) -> None:
    reads = 0

    def handler(_: Recorded) -> httpx.Response:
        nonlocal reads
        reads += 1
        return json_response({}, 503) if reads == 1 else json_response(result(subscription()))

    client = fixtures(handler).client
    assert (await client.webhooks.get(subscription_id)).id == subscription_id
    assert reads == 2
    for status, expected in [(403, "PERMISSION"), (404, "NOT_FOUND"), (409, "CONFLICT")]:
        failed = fixtures(lambda _, status=status: json_response({"code": "WEBHOOK_ROTATION_PENDING", "message": secret}, status)).client
        await rejects(failed.webhooks.rotate_secret(subscription_id), lambda error, expected=expected: error.code == expected and _no_secret(error))


def _never(_: Recorded) -> httpx.Response:
    raise AssertionError("No management request expected")


async def test_unsupported_capabilities_and_malformed_local_grants_fail_before_management_traffic(fixtures: Any) -> None:
    legacy = copy.deepcopy(capabilities)
    del legacy["messaging"]["outgoing_webhooks"]
    old = fixtures(_never, legacy)
    await rejects(old.client.webhooks.list(), UnsupportedFeatureError)
    assert (await old.client.get_capabilities()).messaging.outgoing_webhooks is None
    assert old.data == []
    valid = fixtures(_never)
    for value in [
        {**CREATE_INPUT, "id": "not-uuid"},
        {**CREATE_INPUT, "channel_ids": []},
        {**CREATE_INPUT, "channel_ids": [channel, channel]},
        {**CREATE_INPUT, "channel_ids": ["other-org:team:channel"]},
        {**CREATE_INPUT, "url": "https://user:secret@example.invalid/"},
        {**CREATE_INPUT, "url": "https://example.invalid/#fragment"},
        {**CREATE_INPUT, "url": "file:///private"},
        {**CREATE_INPUT, "url": " https://example.invalid/"},
        {**CREATE_INPUT, "url": "https://example.invalid/a\nb"},
        {**CREATE_INPUT, "url": "https://example.invalid/" + "x" * 2048},
    ]:
        await rejects(valid.client.webhooks.create(**value), ValidationError)
    await rejects(valid.client.webhooks.deliveries(subscription_id, limit=101), ValidationError)
    await rejects(valid.client.webhooks.replay(subscription_id, "../escape"), ValidationError)
    assert len(valid.requests) == 0
    limited_caps = copy.deepcopy(capabilities)
    limited_caps["messaging"]["outgoing_webhooks"]["max_channels"] = 1
    limited = fixtures(_never, limited_caps)
    await rejects(limited.client.webhooks.create(**{**CREATE_INPUT, "channel_ids": [channel, org + ":team:second"]}), ValidationError)
    assert limited.data == []


def _channel(channel_id: str, epoch_value: str, sequence: str = "0") -> dict[str, Any]:
    return {"channel_id": channel_id, "epoch": epoch_value, "sequence": sequence, "status": "active", "last_error": None}


async def test_management_responses_cannot_forge_organization_grant_delivery_or_pagination_scope(fixtures: Any) -> None:
    cases: list[tuple[Callable[[HumChat], Any], Any]] = [
        (lambda client: client.webhooks.get(subscription_id), result(subscription(org_uuid="other"))),
        (lambda client: client.webhooks.get(subscription_id), result(subscription(id=delivery_id))),
        (lambda client: client.webhooks.get(subscription_id), result(subscription(channels=[_channel(channel, "not-a-uuid")]))),
        (lambda client: client.webhooks.list(), {"schema": 1, "org_uuid": "other", "subscriptions": []}),
        (
            lambda client: client.webhooks.create(**CREATE_INPUT),
            {**result(subscription(channels=[_channel(org + ":team:other", epoch)])), "signing_secret": secret},
        ),
        (lambda client: client.webhooks.create(**CREATE_INPUT), {**result(subscription()), "signing_secret": "invalid-secret"}),
        (lambda client: client.webhooks.create(**CREATE_INPUT), {**result(subscription(status="active")), "signing_secret": secret}),
        (lambda client: client.webhooks.pause(subscription_id), result(subscription())),
        (lambda client: client.webhooks.delete(subscription_id), {"schema": 1, "id": delivery_id, "deleted": True}),
        (
            lambda client: client.webhooks.deliveries(subscription_id),
            {"schema": 1, "org_uuid": org, "deliveries": [delivery(channel_id="other:team:channel")], "next_before": None},
        ),
        (
            lambda client: client.webhooks.deliveries(subscription_id),
            {"schema": 1, "org_uuid": org, "deliveries": [delivery()], "next_before": subscription_id},
        ),
        (lambda client: client.webhooks.replay(subscription_id, delivery_id), {"schema": 1, "delivery": delivery(subscription_id=delivery_id)}),
        (
            lambda client: client.webhooks.replay(subscription_id, delivery_id),
            {"schema": 1, "delivery": delivery(epoch="not-a-uuid", event_id="not-a-uuid:1")},
        ),
        (lambda client: client.webhooks.replay(subscription_id, delivery_id), {"schema": 1, "delivery": delivery(event_id="epoch:99")}),
    ]
    for method, response in cases:
        client = fixtures(lambda _, response=response: json_response(response)).client
        await rejects(method(client), ProtocolError)
