"""Port of test/unit/transport.test.mjs."""

from __future__ import annotations

import asyncio
import time
import traceback
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

from helpers import Deferred, FakeClock, MockServer, Recorded, code, json_response, rejects, text_response, until
from humchat import AbortController, HumChat, HumChatError, TokenPrevious

ORG = "org-mock-1"
ACTOR = "user-mock-123"
CHANNEL_ID = f"{ORG}:{ORG}-home:sdk-test"
MARKER = "SYNTHETIC_CREDENTIAL_MUST_NOT_ESCAPE"
DIGEST = "d53d18c23212ea7b6300594bb89bce60218f6eff2b9d628b8cc42d3e79bbd5ab"
IDENTITY = {"user_uuid": ACTOR, "id": 1, "org": ORG, "email": "sdk@example.test", "roles": ["member"], "scopes": [], "expires_at": None, "app_uuid": None}
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
CHANNELS = {
    "org_uuid": ORG,
    "team_uuid": f"{ORG}-home",
    "team_name": "SDK Test",
    "channels": [
        {"channel_id": CHANNEL_ID, "org_uuid": ORG, "team_uuid": f"{ORG}-home", "slug": "sdk-test", "display_name": "SDK Test", "is_private": False}
    ],
}


def secret_free(error: BaseException, *secrets: str) -> None:
    """No credential or untrusted text in the message, repr, attributes or formatted traceback."""
    rendered = "".join(traceback.format_exception(error)) + repr(error) + repr(vars(error))
    for secret in secrets or (MARKER,):
        assert secret not in rendered


def ack(request: Recorded) -> httpx.Response:
    body = request.json()
    return json_response({"status": "ACK", "message_id": body["id"], "created_at": 1792370346, "attachment_count": 0, "attachment_manifest_blake3": DIGEST})


async def never() -> Any:
    await asyncio.Event().wait()


async def default_provider(_: Any) -> dict[str, str]:
    return {"token": "synthetic-token-a"}


@pytest.fixture
async def make_client() -> AsyncIterator[Callable[..., tuple[HumChat, MockServer]]]:
    made: list[HumChat] = []

    def make(handler: Callable[[Recorded], Any] | None = None, **overrides: Any) -> tuple[HumChat, MockServer]:
        route = handler or (lambda _request: json_response(CHANNELS))
        custom = overrides.pop("handler_all", None)

        def dispatch(request: Recorded) -> Any:
            if custom is not None:
                return custom(request)
            if request.path == "/me":
                return json_response(IDENTITY)
            if request.path == "/api/sdk/capabilities":
                return json_response(CAPABILITIES)
            return route(request)

        server = MockServer(dispatch)
        options: dict[str, Any] = {
            "org_id": ORG,
            "base_url": "http://127.0.0.1:43123",
            "token_provider": default_provider,
            "transport": server.transport,
            "timeout_ms": 500,
            "retry": {"max_retries": 0, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000},
            **overrides,
        }
        client = HumChat(**options)
        made.append(client)
        return client, server

    yield make
    for client in made:
        await client.close()


async def test_concurrent_connect_uses_one_provider_call_and_one_verified_handshake(make_client: Any) -> None:
    gate = Deferred()
    calls = 0

    async def provider(context: Any) -> dict[str, str]:
        nonlocal calls
        calls += 1
        assert context.reason == "initial"
        assert context.previous is None
        await gate
        return {"token": "synthetic-token-a"}

    client, server = make_client(token_provider=provider)
    first = asyncio.ensure_future(client.connect())
    second = asyncio.ensure_future(client.connect())
    # Windows loops before Python 3.13 can wake a 10 ms sleep early; wait for the first
    # call explicitly, then give a competing acquisition time to (wrongly) start.
    await until(lambda: calls >= 1, "provider called")
    await asyncio.sleep(0.03)
    assert calls == 1
    gate.resolve()
    one, two = await asyncio.gather(first, second)
    assert one.actor_id == ACTOR
    assert one == two
    assert server.count("/me") == 1
    one.roles.append("changed-by-caller")
    assert (await client.connect()).roles == ["member"]
    cap = await client.get_capabilities()
    cap.messaging.history_versions.clear()
    assert (await client.get_capabilities()).messaging.history_versions == [2]


async def test_one_cancelled_connect_waiter_does_not_cancel_another_shared_handshake(make_client: Any) -> None:
    gate = Deferred()
    started = Deferred()
    provider_signal: Any = None

    async def provider(context: Any) -> dict[str, str]:
        nonlocal provider_signal
        provider_signal = context.signal
        started.resolve()
        await gate
        return {"token": "synthetic-token-a"}

    client, _ = make_client(token_provider=provider)
    cancel = AbortController()
    cancelled = asyncio.ensure_future(client.connect(signal=cancel.signal))
    survivor = asyncio.ensure_future(client.connect())
    await started
    cancel.abort()
    await rejects(cancelled, code("ABORT"))
    assert provider_signal.aborted is False
    gate.resolve()
    assert (await survivor).actor_id == ACTOR


async def test_concurrent_401_responses_refresh_once_without_exposing_the_previous_token(make_client: Any) -> None:
    contexts: list[Any] = []

    async def provider(context: Any) -> dict[str, str]:
        contexts.append(context)
        if len(contexts) > 1:
            await asyncio.sleep(0.015)
        return {"token": "synthetic-token-a" if len(contexts) == 1 else "synthetic-token-b"}

    def route(request: Recorded) -> Any:
        if request.headers.get("authorization") == "Bearer synthetic-token-a":
            return json_response({"error": MARKER}, 401)
        return json_response(CHANNELS)

    client, server = make_client(route, token_provider=provider)
    await client.connect()
    await asyncio.gather(client.channels.list(), client.channels.list())
    assert len(contexts) == 2
    assert contexts[1].reason == "unauthorized"
    assert contexts[1].previous == TokenPrevious(expires_at=None)
    assert not hasattr(contexts[1].previous, "token")
    assert server.count("/me") == 2
    assert server.count("/api/workspace/channels") == 4


async def test_a_second_401_fails_the_operation_instead_of_refreshing_forever(make_client: Any) -> None:
    calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"token": f"synthetic-token-{calls}"}

    client, server = make_client(lambda _r: json_response({"error": MARKER}, 401), token_provider=provider)
    await rejects(client.channels.list(), code("AUTHENTICATION"))
    assert calls == 2
    assert server.count("/api/workspace/channels") == 2


async def test_an_approaching_expiry_refreshes_proactively_with_metadata_only_previous_context(make_client: Any, monkeypatch: Any) -> None:
    clock = FakeClock(monkeypatch, now=2_000_000_000_000, timers=False)
    contexts: list[Any] = []

    async def provider(context: Any) -> dict[str, Any]:
        contexts.append(context)
        return {"token": f"synthetic-token-{len(contexts)}", "expires_at": int(clock.now / 1000) + 120}

    client, _ = make_client(token_provider=provider)
    await client.connect()
    clock.now += 61_000
    await client.channels.list()
    assert len(contexts) == 2
    assert contexts[1].reason == "expiring"
    assert contexts[1].previous == TokenPrevious(expires_at=2_000_000_120)


async def test_a_refreshed_actor_cannot_receive_a_replay_intended_for_the_original_actor(make_client: Any) -> None:
    provider_calls = 0
    data_requests = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return {"token": f"synthetic-token-{provider_calls}"}

    def handler_all(request: Recorded) -> Any:
        nonlocal data_requests
        if request.path == "/me":
            return json_response({**IDENTITY, "user_uuid": ACTOR if provider_calls == 1 else "different-actor"})
        if request.path == "/api/sdk/capabilities":
            return json_response(CAPABILITIES)
        data_requests += 1
        return json_response({}, 401)

    client, _ = make_client(token_provider=provider, handler_all=handler_all)
    await rejects(client.channels.list(), code("AUTHENTICATION"))
    await rejects(client.channels.list(), code("AUTHENTICATION"))
    assert data_requests == 1
    assert provider_calls == 2


async def test_a_different_organization_is_rejected_before_a_data_request(make_client: Any) -> None:
    client, server = make_client(handler_all=lambda _r: json_response({**IDENTITY, "org": "other-organization"}))
    await rejects(client.channels.list(), code("AUTHENTICATION"))
    assert server.paths() == ["/me"]


async def test_shared_authentication_failures_do_not_overwrite_another_operations_intent(make_client: Any) -> None:
    client, _ = make_client(handler_all=lambda _r: json_response({**IDENTITY, "org": "other-organization"}))
    first = await rejects(
        client.messages.send(channel_id=CHANNEL_ID, id="first-message", content="Synthetic first", idempotency_key="first-key"), code("AUTHENTICATION")
    )
    second = await rejects(
        client.messages.send(channel_id=CHANNEL_ID, id="second-message", content="Synthetic second", idempotency_key="second-key"),
        code("AUTHENTICATION"),
    )
    assert isinstance(first, HumChatError) and isinstance(second, HumChatError)
    assert first is not second
    assert first.message_id == "first-message"
    assert first.idempotency_key == "first-key"
    assert second.message_id == "second-message"
    assert second.idempotency_key == "second-key"


async def test_provider_failures_are_sanitized_and_bounded_even_when_the_provider_ignores_cancellation(make_client: Any) -> None:
    async def throwing_provider(_: Any) -> Any:
        raise RuntimeError(MARKER)

    throwing, _ = make_client(token_provider=throwing_provider)
    error = await rejects(throwing.connect(), code("AUTHENTICATION"))
    assert error.__cause__ is None
    secret_free(error)

    async def hanging_provider(_: Any) -> Any:
        return await never()

    hanging, _ = make_client(token_provider=hanging_provider, retry={"max_retries": 0, "total_timeout_ms": 30})
    start = time.perf_counter()
    await rejects(hanging.connect(), code("TIMEOUT"))
    assert time.perf_counter() - start < 0.5


async def test_request_abort_cancels_a_fetch_that_ignores_its_signal(make_client: Any) -> None:
    started = Deferred()

    async def route(_: Recorded) -> Any:
        started.resolve()
        return await never()

    client, _ = make_client(route)
    await client.connect()
    cancel = AbortController()
    work = asyncio.ensure_future(client.channels.list(signal=cancel.signal))
    await started
    cancel.abort()
    await rejects(work, code("ABORT"))


class StalledStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.cancelled = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        await never()
        yield b""

    async def aclose(self) -> None:
        self.cancelled = True


async def test_attempt_timeout_includes_a_stalled_response_body_and_cancels_its_reader(make_client: Any) -> None:
    stream = StalledStream()
    client, _ = make_client(lambda _r: httpx.Response(200, stream=stream), timeout_ms=25)
    await client.connect()
    await rejects(client.channels.list(), code("TIMEOUT"))
    assert stream.cancelled is True


async def test_total_deadline_interrupts_a_long_retry_after_instead_of_scheduling_more_requests(make_client: Any) -> None:
    calls = 0

    def route(_: Recorded) -> Any:
        nonlocal calls
        calls += 1
        return json_response({}, 429, {"retry-after": "10"})

    client, _ = make_client(route, retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 50})
    await client.connect()
    start = time.perf_counter()
    await rejects(client.channels.list(), code("TIMEOUT"))
    assert calls == 1
    assert time.perf_counter() - start < 0.5


async def test_408_429_502_503_and_504_responses_retry_a_safe_read(make_client: Any) -> None:
    for status in (408, 429, 502, 503, 504):
        attempts = 0

        def route(_: Recorded, status: int = status) -> Any:
            nonlocal attempts
            attempts += 1
            return json_response({}, status) if attempts == 1 else json_response(CHANNELS)

        client, _ = make_client(route, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
        await client.channels.list()
        assert attempts == 2, f"status {status}"


async def test_403_404_422_and_500_fail_without_implicit_retries(make_client: Any) -> None:
    for status, expected in ((403, "PERMISSION"), (404, "NOT_FOUND"), (422, "VALIDATION"), (500, "SERVER")):
        attempts = 0

        def route(_: Recorded, status: int = status) -> Any:
            nonlocal attempts
            attempts += 1
            return json_response({}, status)

        client, _ = make_client(route, retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
        await rejects(client.channels.list(), code(expected))
        assert attempts == 1, f"status {status}"


async def test_retry_after_is_honored_before_a_rate_limited_request_is_retried(make_client: Any) -> None:
    times: list[float] = []

    def route(_: Recorded) -> Any:
        times.append(time.perf_counter())
        return json_response({}, 429, {"retry-after": "0.04"}) if len(times) == 1 else json_response(CHANNELS)

    client, _ = make_client(route, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    await client.channels.list()
    assert len(times) == 2
    assert (times[1] - times[0]) * 1000 >= 30, "Retry must honor the server cooldown"


async def test_idempotent_send_retries_preserve_the_complete_body_and_intent_headers(make_client: Any) -> None:
    sends: list[Recorded] = []

    def route(request: Recorded) -> Any:
        sends.append(request)
        if len(sends) == 1:
            raise RuntimeError(MARKER)
        return ack(request)

    client, _ = make_client(route, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    receipt = await client.messages.send(channel_id=CHANNEL_ID, content="Synthetic test message")
    assert len(sends) == 2
    assert sends[0].body == sends[1].body
    assert sends[0].headers.get("x-idempotency-key")
    assert sends[0].headers.get("x-idempotency-key") == sends[1].headers.get("x-idempotency-key")
    assert sends[0].headers.get("x-zlack-message-receipt") == "v1"
    assert receipt.id == sends[0].json()["id"]


async def test_a_401_send_replay_preserves_its_message_id_body_and_idempotency_key_after_refresh(make_client: Any) -> None:
    sends: list[Recorded] = []
    provider_calls = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return {"token": f"synthetic-token-{provider_calls}"}

    def route(request: Recorded) -> Any:
        sends.append(request)
        return json_response({}, 401) if len(sends) == 1 else ack(request)

    client, _ = make_client(route, token_provider=provider)
    await client.messages.send(channel_id=CHANNEL_ID, content="Synthetic reply")
    assert provider_calls == 2
    assert len(sends) == 2
    assert sends[0].body == sends[1].body
    assert sends[0].headers.get("x-idempotency-key") == sends[1].headers.get("x-idempotency-key")
    assert sends[0].headers.get("authorization") != sends[1].headers.get("authorization")


async def test_exhausted_network_and_timeout_sends_retain_exact_recoverable_intent_metadata(make_client: Any) -> None:
    for kind in ("NETWORK", "TIMEOUT"):
        sends: list[Recorded] = []

        async def route(request: Recorded, kind: str = kind, sends: list[Recorded] = sends) -> Any:
            sends.append(request)
            if kind == "NETWORK":
                raise RuntimeError(MARKER)
            return await never()

        # Node uses a 20 ms attempt timeout; it also bounds the /me and capability requests,
        # which can exceed 20 ms on a loaded CI runner. 200 ms keeps both attempts in budget.
        client, _ = make_client(route, timeout_ms=200, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
        error = await rejects(
            client.messages.send(channel_id=CHANNEL_ID, content="Synthetic send", id="failure-intent", idempotency_key="failure-key"), code(kind)
        )
        assert isinstance(error, HumChatError)
        assert error.message_id == "failure-intent"
        assert error.channel_id == CHANNEL_ID
        assert error.idempotency_key == "failure-key"
        assert error.__cause__ is None
        secret_free(error)
        assert len(sends) == 2
        assert sends[0].body == sends[1].body


def reaction_state() -> dict[str, Any]:
    return {
        "schema": 1,
        "org_uuid": ORG,
        "channel_id": CHANNEL_ID,
        "message_id": "react-message",
        "sender_id": ACTOR,
        "created_at": 1792370346,
        "thread_root_id": None,
        "priority": "standard",
        "original_body": "Synthetic root",
        "body": "Synthetic root",
        "revision": 1,
        "updated_at": 1792370346,
        "deleted": False,
        "reaction_revision": 1,
        "reactions": {"👍": [ACTOR]},
        "attachments": [],
    }


async def test_reaction_retries_preserve_the_operation_id_and_exact_set_intent(make_client: Any) -> None:
    operations: list[Recorded] = []

    def route(request: Recorded) -> Any:
        operations.append(request)
        if len(operations) == 1:
            return json_response({}, 503)
        body = request.json()
        return json_response({"schema": 1, "operation_id": body["operation_id"], "actor_id": ACTOR, "request_digest": DIGEST, "state": reaction_state()})

    client, _ = make_client(route, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    receipt = await client.messages.set_reaction(channel_id=CHANNEL_ID, message_id="react-message", emoji="👍", present=True)
    assert len(operations) == 2
    assert operations[0].body == operations[1].body
    body = operations[0].json()
    assert body["operation_id"]
    assert body["mutation"] == {"kind": "set_reaction", "emoji": "👍", "present": True}
    assert receipt.operation_id == body["operation_id"]


async def test_mutation_acquisition_and_response_share_one_total_operation_deadline(make_client: Any, monkeypatch: Any) -> None:
    # Move the operation clock explicitly: a loaded host may spend the entire real deadline
    # before a sleeping provider resumes, never attempting the mutation.
    clock = FakeClock(monkeypatch, now=1_800_000_000_000)
    provider_started, provider_gate = Deferred(), Deferred()
    mutations: list[Recorded] = []

    async def route(request: Recorded) -> Any:
        mutations.append(request)
        return await never()  # The response deliberately ignores cancellation.

    async def provider(_: Any) -> dict[str, str]:
        provider_started.resolve()
        await provider_gate
        return {"token": "synthetic-token-a"}

    client, _ = make_client(route, token_provider=provider, retry={"max_retries": 0, "total_timeout_ms": 200})
    operation = asyncio.ensure_future(
        client.messages.set_reaction(channel_id=CHANNEL_ID, message_id="deadline-message", emoji="👍", present=True, operation_id="deadline-operation")
    )
    await provider_started
    clock.tick(120)
    provider_gate.resolve()
    await clock.drain()
    assert len(mutations) == 1, "Deadline must span both authentication and the attempted mutation"
    assert not operation.done()
    clock.tick(79)
    await clock.drain()
    assert not operation.done(), "The operation must remain pending before its original deadline"
    assert mutations[0].cancelled is False
    clock.tick(1)
    await clock.drain()
    assert operation.done(), "Authentication must consume the same 200 ms deadline as the mutation response"
    assert mutations[0].cancelled is True, "The original deadline must cancel the active mutation request"
    error = await rejects(operation, code("TIMEOUT"))
    assert isinstance(error, HumChatError)
    assert error.operation_id == "deadline-operation"


async def test_409_is_not_retried_and_failures_preserve_caller_intent_without_server_body_leakage(make_client: Any) -> None:
    attempts = 0

    def route(_: Recorded) -> Any:
        nonlocal attempts
        attempts += 1
        return json_response({"error": MARKER}, 409, {"x-request-id": "safe-request-1"})

    client, _ = make_client(route, retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    error = await rejects(
        client.messages.send(channel_id=CHANNEL_ID, id="intent-message-1", content="Synthetic test message", idempotency_key="intent-key-1"),
        code("CONFLICT"),
    )
    assert isinstance(error, HumChatError)
    assert error.status == 409
    assert error.message_id == "intent-message-1"
    assert error.idempotency_key == "intent-key-1"
    assert error.request_id == "safe-request-1"
    secret_free(error)
    assert attempts == 1


async def test_malformed_json_and_unsafe_request_ids_never_copy_response_text_into_errors(make_client: Any) -> None:
    malformed, _ = make_client(lambda _r: text_response("not-json-" + MARKER))
    error = await rejects(malformed.channels.list(), code("PROTOCOL"))
    assert error.__cause__ is None
    secret_free(error)
    unsafe_id, _ = make_client(lambda _r: json_response({"error": MARKER}, 500, {"x-request-id": "Bearer " + MARKER}))
    error = await rejects(unsafe_id.channels.list(), code("SERVER"))
    assert isinstance(error, HumChatError)
    assert error.request_id is None
    secret_free(error)


class FailingStream(httpx.AsyncByteStream):
    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise self.error
        yield b""  # pragma: no cover


async def test_custom_fetch_and_response_stream_errors_are_sanitized_even_when_they_use_sdk_error_classes(make_client: Any) -> None:
    for boundary in ("fetch", "body"):

        def route(_: Recorded, boundary: str = boundary) -> Any:
            sensitive = HumChatError(MARKER, "NETWORK", cause=RuntimeError(MARKER), retryable=True)
            if boundary == "fetch":
                raise sensitive
            return httpx.Response(200, stream=FailingStream(sensitive))

        client, _ = make_client(route)
        error = await rejects(client.channels.list(), code("NETWORK"))
        assert error.__cause__ is None
        secret_free(error)


async def test_an_error_request_id_cannot_expose_the_active_short_opaque_token(make_client: Any) -> None:
    client, _ = make_client(lambda _r: json_response({}, 500, {"x-request-id": "prefix-synthetic-token-a-suffix"}))
    error = await rejects(client.channels.list(), code("SERVER"))
    assert isinstance(error, HumChatError)
    assert error.request_id is None
    secret_free(error, "synthetic-token-a")


async def test_all_requests_disable_redirects_and_credentials_stay_out_of_urls_and_safe_config(make_client: Any) -> None:
    client, server = make_client()
    await client.channels.list()
    # Python adaptation: fetch's per-request ``redirect: 'error'`` is the client-wide
    # ``follow_redirects=False`` setting plus redirect statuses mapped to NetworkError.
    assert client._http._client is not None and client._http._client.follow_redirects is False
    for request in server.requests:
        assert request.headers.get("authorization") == "Bearer synthetic-token-a"
        assert "synthetic-token" not in str(request.url)
    config = client.get_config()
    assert not hasattr(config, "token_provider")
    assert not hasattr(config, "transport")
    assert "synthetic-token" not in repr(config)


async def test_close_cancels_outstanding_fetch_and_refuses_later_work_without_another_request(make_client: Any) -> None:
    started = Deferred()

    async def route(_: Recorded) -> Any:
        started.resolve()
        return await never()

    client, server = make_client(route)
    await client.connect()
    pending = asyncio.ensure_future(client.channels.list())
    await started
    await client.close()
    await rejects(pending, code("ABORT"))
    count = len(server.requests)
    await rejects(client.channels.list(), code("ABORT"))
    assert len(server.requests) == count
