"""Port of test/unit/sdk-key-auth.test.mjs.

Synthetic secrets only: no production credentials or customer identities belong here.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from aiohttp import web

from helpers import Deferred, FakeClock, LoopbackPeer, MockServer, Recorded, json_response, text_response, until
from humchat import AbortController, HumChat, HumChatError, _clock

ORG_ID = "01234567-89ab-cdef-0123-456789abcdef"
ACTOR_ID = "synthetic-sdk-key-actor"
# Concatenated so a release scan for long hsk_ literals never matches a synthetic fixture.
SDK_KEY = "hsk_" + "SYNTHETIC_MINT_SECRET_NEVER_EXPOSE"
ACT_AS = "synthetic.actor@example.invalid"
TOKEN = "SYNTHETIC_MINTED_TOKEN_NEVER_EXPOSE"
MARKER = "SYNTHETIC_UNTRUSTED_BODY_NEVER_EXPOSE"
MINT_PATH = "/api/sdk/auth/user-token"
KEY_CAPABILITY = {"endpoint": MINT_PATH, "key_header": "X-HumChat-Sdk-Key", "configured": True}
IDENTITY = {"user_uuid": ACTOR_ID, "org": ORG_ID, "email": ACT_AS, "roles": ["member"], "expires_at": None}
CAPABILITIES: dict[str, Any] = {
    "sdk_protocol_version": 1,
    "authentication": {
        "user_sessions": True,
        "service_identities": False,
        "api_key_exchange": False,
        "app_binding_required": False,
        "mock_authentication": False,
        "sdk_key_user_tokens": KEY_CAPABILITY,
    },
    "messaging": {
        "send_receipt_versions": [1],
        "message_state_versions": [1],
        "history_versions": [2],
        "durable_replay": False,
        "websocket_scope": "channel",
    },
}
MINT_RESULT = {"access_token": TOKEN, "token_type": "Bearer", "expires_in": 900}


def exposed(error: BaseException) -> str:
    """Everything an application could log: repr/str/attributes and the formatted traceback chain."""
    return "\n".join(
        [repr(error), str(error), repr(vars(error)), repr(error.args), "".join(traceback.format_exception(type(error), error, error.__traceback__))]
    )


def secret_free(value: str) -> None:
    for secret in (SDK_KEY, TOKEN, MARKER):
        assert secret not in value, "Credentials and untrusted response text must stay private"


def error_code(expected: str, status: int | None = None) -> Callable[[BaseException], bool]:
    def check(error: BaseException) -> bool:
        assert isinstance(error, HumChatError), repr(error)
        assert error.code == expected, f"{error.code} != {expected}"
        if status is not None:
            assert error.status == status
        assert error.__cause__ is None
        secret_free(exposed(error))
        return True

    return check


async def rejects(awaitable: Any, check: Callable[[BaseException], bool]) -> BaseException:
    try:
        await awaitable
    except HumChatError as error:
        assert check(error)
        return error
    raise AssertionError("Expected the operation to fail")


@dataclass
class Fixture:
    client: HumChat
    server: MockServer

    @property
    def requests(self) -> list[Recorded]:
        return self.server.requests

    def paths(self) -> list[str]:
        return self.server.paths()


@pytest.fixture
async def clients() -> AsyncIterator[list[HumChat]]:
    created: list[HumChat] = []
    yield created
    for client in created:
        await client.close()


@pytest.fixture
def fixture(clients: list[HumChat]) -> Callable[..., Fixture]:
    def build(handlers: dict[str, Callable[..., Any]] | None = None, **options: Any) -> Fixture:
        handlers = handlers or {}
        server: MockServer

        def handle(request: Recorded) -> Any:
            if request.path in handlers:
                return handlers[request.path](request, server.requests)
            if request.path == MINT_PATH:
                return json_response(MINT_RESULT)
            if request.path == "/me":
                return json_response(IDENTITY)
            if request.path == "/api/sdk/capabilities":
                return json_response(CAPABILITIES)
            assert request.path == "/api/org/members", "Unexpected authenticated path"
            return json_response([])

        server = MockServer(handle)
        config: dict[str, Any] = {
            "org_id": ORG_ID,
            "base_url": "https://humchat.example.invalid",
            "auth": {"sdk_key": SDK_KEY, "act_as": ACT_AS},
            "transport": server.transport,
            "timeout_ms": 500,
            "retry": {"max_retries": 0, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1_000},
            **options,
        }
        client = HumChat(**config)
        clients.append(client)
        return Fixture(client, server)

    return build


def ok(value: Any, status: int = 200, headers: dict[str, str] | None = None) -> Callable[..., httpx.Response]:
    return lambda *_: json_response(value, status, headers)


class StalledStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes] | None = None) -> None:
        self.chunks = chunks or []
        self.cancelled = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        await asyncio.get_running_loop().create_future()

    async def aclose(self) -> None:
        self.cancelled = True


async def test_sdk_key_exchange_uses_only_humchat_then_verifies_identity_and_capabilities_before_a_resource_call(fixture: Any) -> None:
    f = fixture()
    assert f.requests == []
    assert await f.client.members.list() == []
    assert f.paths() == [MINT_PATH, "/me", "/api/sdk/capabilities", "/api/org/members"]
    mint = f.requests[0]
    assert mint.method == "POST"
    assert mint.headers.get("x-humchat-sdk-key") == SDK_KEY
    assert "authorization" not in mint.headers
    assert mint.headers.get("content-type", "").startswith("application/json")
    assert mint.json() == {"org_uuid": ORG_ID, "email": ACT_AS, "expires_in": 900}
    for request in f.requests:
        assert f"{request.url.scheme}://{request.url.host}" == "https://humchat.example.invalid"
        assert request.url.query == b""
        if request is not mint:
            assert request.headers.get("authorization") == f"Bearer {TOKEN}"
            assert "x-humchat-sdk-key" not in request.headers
            assert request.body is None
    # fetch(redirect: 'error') equivalent: the shared client never follows redirects.
    assert f.client._http._http().follow_redirects is False
    assert (await f.client.connect()).actor_id == ACTOR_ID
    caps = await f.client.get_capabilities()
    assert caps.authentication.sdk_key_user_tokens is not None
    assert (caps.authentication.sdk_key_user_tokens.endpoint, caps.authentication.sdk_key_user_tokens.key_header) == (MINT_PATH, "X-HumChat-Sdk-Key")
    assert caps.authentication.sdk_key_user_tokens.configured is True
    caps.authentication.sdk_key_user_tokens.configured = False
    refreshed = (await f.client.get_capabilities()).authentication.sdk_key_user_tokens
    assert refreshed is not None and refreshed.configured is True
    assert len(f.requests) == 4, "Verified sessions must be reused"
    secret_free(repr(f.client) + repr(vars(f.client)) + repr(f.client.get_config()))


async def test_sdk_key_credentials_are_snapshotted_and_normalized_without_mutating_the_caller_object(fixture: Any) -> None:
    auth = {"sdk_key": SDK_KEY, "act_as": " SYNTHETIC.Actor@EXAMPLE.INVALID "}
    original = dict(auth)
    f = fixture(auth=auth, org_id=ORG_ID.upper())
    assert auth == original
    auth["sdk_key"] = "hsk_mutated_after_construction"
    auth["act_as"] = "someone.else@example.invalid"
    await f.client.connect()
    assert f.requests[0].headers.get("x-humchat-sdk-key") == SDK_KEY
    assert f.requests[0].json() == {"org_uuid": ORG_ID, "email": ACT_AS, "expires_in": 900}


class CountingAuth(Mapping[str, str]):
    """Python counterpart of JS getters: values change after their first read."""

    def __init__(self) -> None:
        self.reads = {"sdk_key": 0, "act_as": 0}

    def __getitem__(self, key: str) -> str:
        self.reads[key] += 1
        if key == "sdk_key":
            return SDK_KEY if self.reads[key] == 1 else "hsk_changed_after_validation"
        if key == "act_as":
            return ACT_AS if self.reads[key] == 1 else "changed@example.invalid"
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(["sdk_key", "act_as"])

    def __len__(self) -> int:
        return 2


async def test_sdk_key_credential_getters_are_read_once_so_validation_and_use_cannot_disagree(fixture: Any) -> None:
    auth = CountingAuth()
    f = fixture(auth=auth)
    await f.client.connect()
    assert auth.reads == {"sdk_key": 1, "act_as": 1}
    assert f.requests[0].headers.get("x-humchat-sdk-key") == SDK_KEY
    assert f.requests[0].json()["email"] == ACT_AS


async def test_ascii_email_normalization_preserves_non_ascii_local_part_letters(fixture: Any) -> None:
    f = fixture({"/me": ok({**IDENTITY, "email": "Äctor@example.invalid"})}, auth={"sdk_key": SDK_KEY, "act_as": " ÄCTOR@EXAMPLE.INVALID "})
    await f.client.connect()
    assert f.requests[0].json()["email"] == "Äctor@example.invalid"


async def test_concurrent_sdk_key_connect_and_resource_callers_share_one_mint_and_verified_handshake(fixture: Any) -> None:
    started, release = Deferred(), Deferred()

    async def mint(*_: Any) -> httpx.Response:
        started.resolve()
        await release
        return json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint})
    client = f.client
    calls = (client.connect(), client.connect(), client.get_capabilities(), client.members.list(), client.members.list())
    operations = [asyncio.ensure_future(call) for call in calls]
    await started
    assert f.paths() == [MINT_PATH]
    release.resolve()
    await asyncio.gather(*operations)
    assert f.server.count(MINT_PATH) == 1
    assert f.server.count("/me") == 1
    assert f.server.count("/api/sdk/capabilities") == 1
    assert f.server.count("/api/org/members") == 2


async def test_cancelling_one_sdk_key_waiter_preserves_the_shared_mint_for_another_caller(fixture: Any) -> None:
    started, release = Deferred(), Deferred()

    async def mint(*_: Any) -> httpx.Response:
        started.resolve()
        await release
        return json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint})
    controller = AbortController()
    cancelled = asyncio.ensure_future(f.client.connect(signal=controller.signal))
    survivor = asyncio.ensure_future(f.client.connect())
    await started
    controller.abort(RuntimeError(f"{SDK_KEY}:{TOKEN}"))
    await rejects(cancelled, error_code("ABORT"))
    assert f.requests[0].cancelled is False
    release.resolve()
    assert (await survivor).actor_id == ACTOR_ID


async def test_close_aborts_a_mint_that_ignores_cancellation_and_prevents_later_verification_or_use(fixture: Any) -> None:
    started, release = Deferred(), Deferred()

    async def mint(*_: Any) -> httpx.Response:
        started.resolve()
        await release
        return json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint})
    pending = asyncio.ensure_future(f.client.connect())
    await started
    await f.client.close()
    await until(lambda: f.requests[0].cancelled, "close cancels the in-flight mint request")
    release.resolve()
    await rejects(pending, error_code("ABORT"))
    await asyncio.sleep(0)
    await rejects(f.client.members.list(), error_code("ABORT"))
    assert f.paths() == [MINT_PATH]


async def test_a_pre_aborted_connect_does_not_transmit_an_sdk_key(fixture: Any) -> None:
    f = fixture()
    controller = AbortController()
    controller.abort()
    await rejects(f.client.connect(signal=controller.signal), error_code("ABORT"))
    assert f.requests == []


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (400, "VALIDATION"),
        (401, "AUTHENTICATION"),
        (403, "PERMISSION"),
        (404, "UNSUPPORTED_FEATURE"),
        (405, "UNSUPPORTED_FEATURE"),
        (413, "VALIDATION"),
        (415, "VALIDATION"),
        (422, "VALIDATION"),
        (500, "SERVER"),
    ],
)
async def test_sdk_key_mint_status_is_sanitized_typed_and_never_retried(fixture: Any, status: int, expected: str) -> None:
    f = fixture(
        {MINT_PATH: lambda *_: text_response(f"{SDK_KEY}:{TOKEN}:{MARKER}", status, {"x-request-id": f"prefix-{SDK_KEY}-suffix"})},
        retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000},
    )

    def check(error: BaseException) -> bool:
        error_code(expected, status)(error)
        assert isinstance(error, HumChatError)
        assert error.retryable is False
        assert error.request_id is None
        return True

    await rejects(f.client.connect(), check)
    assert f.paths() == [MINT_PATH]


@pytest.mark.parametrize(("status", "expected"), [(429, "RATE_LIMIT"), (502, "SERVER"), (503, "SERVER")])
async def test_sdk_key_mint_status_has_bounded_retries_and_retains_its_native_error_category(fixture: Any, status: int, expected: str) -> None:
    f = fixture(
        {MINT_PATH: ok({"detail": f"{SDK_KEY}:{TOKEN}:{MARKER}"}, status)},
        retry={"max_retries": 2, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000},
    )

    def check(error: BaseException) -> bool:
        error_code(expected, status)(error)
        assert isinstance(error, HumChatError) and error.retryable is True
        return True

    await rejects(f.client.connect(), check)
    assert len(f.requests) == 3
    for request in f.requests:
        assert request.path == MINT_PATH
        assert request.body == f.requests[0].body
        assert "authorization" not in request.headers


async def test_transient_sdk_key_mint_failure_retries_successfully_and_keeps_the_key_out_of_normal_calls(fixture: Any) -> None:
    attempts = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return json_response({}, 502) if attempts == 1 else json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint}, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    await f.client.members.list()
    assert attempts == 2
    assert "x-humchat-sdk-key" not in f.requests[-1].headers


async def test_sdk_key_mint_honors_retry_after_within_the_shared_acquisition_deadline(fixture: Any) -> None:
    times: list[float] = []

    def mint(*_: Any) -> httpx.Response:
        times.append(time.perf_counter() * 1000)
        return json_response({}, 429, {"retry-after": "0.04"}) if len(times) == 1 else json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint}, retry={"max_retries": 1, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000})
    await f.client.connect()
    assert len(times) == 2
    assert times[1] - times[0] >= 30


async def test_a_long_mint_retry_after_cannot_outlive_the_acquisition_deadline_or_trigger_an_extra_attempt(fixture: Any) -> None:
    f = fixture({MINT_PATH: ok({}, 429, {"retry-after": "10"})}, retry={"max_retries": 2, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 50})
    await rejects(f.client.connect(), error_code("TIMEOUT"))
    assert len(f.requests) == 1


async def test_sdk_key_mint_timeout_includes_a_stalled_response_body_and_cancels_its_reader(fixture: Any) -> None:
    stream = StalledStream()
    f = fixture({MINT_PATH: lambda *_: httpx.Response(200, stream=stream)}, timeout_ms=25)
    await rejects(f.client.connect(), error_code("TIMEOUT"))
    assert stream.cancelled is True


async def test_oversized_mint_responses_are_bounded_and_discarded_before_identity_verification(fixture: Any) -> None:
    stream = StalledStream([bytes(10 * 1024 * 1024 + 1)])
    f = fixture({MINT_PATH: lambda *_: httpx.Response(200, stream=stream)})
    await rejects(f.client.connect(), error_code("PROTOCOL"))
    assert stream.cancelled is True
    assert f.paths() == [MINT_PATH]


class FailingStream(httpx.AsyncByteStream):
    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise self.error
        yield b""  # pragma: no cover


async def test_custom_mint_fetch_and_body_failures_cannot_smuggle_secrets_through_sdk_errors_or_causes(fixture: Any) -> None:
    for boundary in ["fetch", "body"]:

        def mint(*_: Any, boundary: str = boundary) -> httpx.Response:
            sensitive = HumChatError(f"{SDK_KEY}:{TOKEN}:{MARKER}", "AUTHENTICATION", cause=RuntimeError(SDK_KEY))
            if boundary == "fetch":
                raise sensitive
            return httpx.Response(200, stream=FailingStream(sensitive))

        f = fixture({MINT_PATH: mint})
        await rejects(f.client.connect(), error_code("NETWORK"))


async def test_native_transport_refuses_redirects_from_the_mint_endpoint_without_transmitting_the_key_to_the_target(clients: list[HumChat]) -> None:
    async def http(request: web.Request) -> web.StreamResponse:
        return web.Response(status=302, headers={"location": "/must-not-receive-key"})

    peer = await LoopbackPeer(http).start()
    try:
        client = HumChat(
            org_id=ORG_ID,
            base_url=peer.base_url,
            auth={"sdk_key": SDK_KEY, "act_as": ACT_AS},
            # Wider than Node's 500 ms: the first real httpx client builds its TLS context
            # synchronously, which can consume a short deadline on a cold Windows host.
            timeout_ms=5_000,
            retry={"max_retries": 0, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 10_000},
        )
        clients.append(client)
        await rejects(client.connect(), error_code("NETWORK"))
        assert [entry["path"] for entry in peer.request_log] == [MINT_PATH]
    finally:
        await peer.close()


async def test_malformed_mint_success_bodies_fail_before_identity_lookup_without_copying_response_data(fixture: Any) -> None:
    cases: list[Any] = [
        None,
        {},
        [],
        {**MINT_RESULT, "access_token": ""},
        {**MINT_RESULT, "access_token": f"Bearer {TOKEN}"},
        {**MINT_RESULT, "access_token": f"{TOKEN}\n"},
        {**MINT_RESULT, "access_token": f"{TOKEN}\u0085"},
        {**MINT_RESULT, "access_token": f"{TOKEN}é"},
        {**MINT_RESULT, "access_token": 42},
        {**MINT_RESULT, "token_type": "Basic"},
        {**MINT_RESULT, "token_type": None},
        {**MINT_RESULT, "expires_in": 0},
        {**MINT_RESULT, "expires_in": -1},
        {**MINT_RESULT, "expires_in": 3601},
        {**MINT_RESULT, "expires_in": 1.5},
        {**MINT_RESULT, "expires_in": "900"},
        {**MINT_RESULT, "expires_in": None},
        {**MINT_RESULT, "expires_in": 9_007_199_254_740_992},
    ]
    for result in cases:
        f = fixture({MINT_PATH: ok(result)})
        await rejects(f.client.connect(), error_code("PROTOCOL"))
        assert f.paths() == [MINT_PATH]
    f = fixture({MINT_PATH: lambda *_: text_response(f"not-json:{SDK_KEY}:{TOKEN}:{MARKER}")})
    await rejects(f.client.connect(), error_code("PROTOCOL"))


async def test_mint_token_type_accepts_case_insensitive_bearer_scheme_spelling(fixture: Any) -> None:
    for token_type in ["bearer", "BEARER", "bEaReR"]:
        f = fixture({MINT_PATH: ok({**MINT_RESULT, "token_type": token_type})})
        assert (await f.client.connect()).actor_id == ACTOR_ID


@pytest.mark.parametrize(
    ("label", "returned"),
    [
        ("the configured SDK key", SDK_KEY),
        ("another SDK-shaped key", "hsk_" + "synthetic_unrelated_response_key"),
        ("a value containing the configured SDK key", f"prefix-{SDK_KEY}-suffix"),
    ],
)
async def test_mint_response_cannot_promote_a_key_into_a_bearer_credential(fixture: Any, label: str, returned: str) -> None:
    f = fixture({MINT_PATH: ok({**MINT_RESULT, "access_token": returned})})

    def check(error: BaseException) -> bool:
        error_code("PROTOCOL")(error)
        assert returned not in exposed(error)
        return True

    await rejects(f.client.members.list(), check)
    assert f.paths() == [MINT_PATH], "An SDK key returned as a token must be rejected before identity or resource requests"
    assert all("authorization" not in request.headers for request in f.requests)


async def test_mint_lifetimes_accept_the_inclusive_supported_boundaries(fixture: Any) -> None:
    for expires_in in [1, 3600]:
        f = fixture({MINT_PATH: ok({**MINT_RESULT, "expires_in": expires_in})})
        assert (await f.client.connect()).actor_id == ACTOR_ID


async def test_mint_expiry_is_based_on_request_start_rather_than_response_completion(fixture: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock(monkeypatch, now=2_000_000_000_000, timers=False)
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        if mints == 1:
            clock.now += 850_000
        return json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint})
    await f.client.connect()
    clock.now += 30_000
    await f.client.members.list()
    assert mints == 2, "Slow mint responses must not extend the advertised token lifetime"


async def test_a_shorter_verified_identity_expiry_controls_renewal_while_null_identity_expiry_keeps_the_mint_limit(
    fixture: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock(monkeypatch, now=2_000_000_000_000, timers=False)
    for verified in [120, None]:
        mints = 0

        def mint(*_: Any) -> httpx.Response:
            nonlocal mints
            mints += 1
            return json_response(MINT_RESULT)

        def me(*_: Any, verified: int | None = verified) -> httpx.Response:
            return json_response({**IDENTITY, "expires_at": None if verified is None else int(clock.now / 1000) + verified})

        f = fixture({MINT_PATH: mint, "/me": me})
        await f.client.connect()
        clock.now += (841 if verified is None else 61) * 1000
        await f.client.members.list()
        assert mints == 2


async def test_already_expired_verified_identity_cannot_expose_a_usable_sdk_key_session(fixture: Any) -> None:
    f = fixture({"/me": lambda *_: json_response({**IDENTITY, "expires_at": int(_clock.now_ms() // 1000) - 1})})
    await rejects(f.client.members.list(), error_code("AUTHENTICATION"))
    assert "/api/org/members" not in f.paths()


async def test_wrong_organization_or_normalized_email_is_terminal_before_capabilities_or_resource_access(fixture: Any) -> None:
    for wrong in [{"org": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}, {"email": "different@example.invalid"}]:
        f = fixture({"/me": ok({**IDENTITY, **wrong})})
        await rejects(f.client.members.list(), error_code("AUTHENTICATION"))
        await rejects(f.client.members.list(), error_code("AUTHENTICATION"))
        assert f.paths() == [MINT_PATH, "/me"]


async def test_verified_actor_email_uses_the_same_ascii_case_normalization_as_the_requested_actor(fixture: Any) -> None:
    f = fixture({"/me": ok({**IDENTITY, "email": " SYNTHETIC.Actor@EXAMPLE.INVALID "})})
    assert (await f.client.connect()).actor_id == ACTOR_ID


async def test_unconfigured_or_absent_sdk_key_capability_prevents_exposing_the_minted_session(fixture: Any) -> None:
    for value in [None, {**KEY_CAPABILITY, "configured": False}]:
        auth = dict(CAPABILITIES["authentication"])
        if value is None:
            del auth["sdk_key_user_tokens"]
        else:
            auth["sdk_key_user_tokens"] = value
        f = fixture({"/api/sdk/capabilities": ok({**CAPABILITIES, "authentication": auth})})
        await rejects(f.client.members.list(), error_code("UNSUPPORTED_FEATURE"))
        assert "/api/org/members" not in f.paths()


async def test_malformed_and_unsafe_advertised_sdk_key_contracts_cannot_redirect_credentials(fixture: Any) -> None:
    for value in [
        None,
        {},
        {**KEY_CAPABILITY, "configured": "true"},
        {**KEY_CAPABILITY, "endpoint": "https://attacker.example.invalid/mint"},
        {**KEY_CAPABILITY, "endpoint": "//attacker.example.invalid/mint"},
        {**KEY_CAPABILITY, "endpoint": "/api/unrelated"},
        {**KEY_CAPABILITY, "key_header": "Authorization"},
        {**KEY_CAPABILITY, "key_header": "X-HumChat-Sdk-Key\r\nAuthorization"},
    ]:
        caps = {**CAPABILITIES, "authentication": {**CAPABILITIES["authentication"], "sdk_key_user_tokens": value}}
        f = fixture({"/api/sdk/capabilities": ok(caps)})
        await rejects(f.client.members.list(), error_code("PROTOCOL"))
        assert f.paths() == [MINT_PATH, "/me", "/api/sdk/capabilities"]
        assert all(request.url.host == "humchat.example.invalid" and request.url.scheme == "https" for request in f.requests)


async def test_capability_header_matching_is_case_insensitive(fixture: Any) -> None:
    caps = {
        **CAPABILITIES,
        "authentication": {**CAPABILITIES["authentication"], "sdk_key_user_tokens": {**KEY_CAPABILITY, "key_header": "x-humchat-sdk-key"}},
    }
    f = fixture({"/api/sdk/capabilities": ok(caps)})
    assert (await f.client.connect()).actor_id == ACTOR_ID


async def test_concurrent_normal_call_401s_share_one_sdk_key_remint_and_retry_with_the_replacement_token(fixture: Any) -> None:
    second = "synthetic-sdk-key-replacement-token"
    mints = 0
    release, started = Deferred(), Deferred()

    async def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        current = mints
        if current > 1:
            started.resolve()
            await release
        return json_response({**MINT_RESULT, "access_token": TOKEN if current == 1 else second})

    def members(request: Recorded, *_: Any) -> httpx.Response:
        return json_response({}, 401) if request.headers.get("authorization") == f"Bearer {TOKEN}" else json_response([])

    f = fixture({MINT_PATH: mint, "/api/org/members": members})
    await f.client.connect()
    operations = [asyncio.ensure_future(f.client.members.list()), asyncio.ensure_future(f.client.members.list())]
    await started
    release.resolve()
    await asyncio.gather(*operations)
    assert mints == 2
    resources = [request for request in f.requests if request.path == "/api/org/members"]
    assert len(resources) == 4
    assert all("x-humchat-sdk-key" not in request.headers for request in resources)


async def test_a_second_normal_call_401_surfaces_the_error_without_reminting_forever(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        return json_response(MINT_RESULT)

    f = fixture({MINT_PATH: mint, "/api/org/members": lambda *_: text_response(f"{SDK_KEY}:{TOKEN}:{MARKER}", 401)})
    await rejects(f.client.members.list(), error_code("AUTHENTICATION", 401))
    assert mints == 2
    assert f.server.count("/api/org/members") == 2


async def test_sdk_key_mode_preserves_the_no_replay_rule_for_conversation_creation_after_401(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        return json_response(MINT_RESULT)

    f = fixture(
        {MINT_PATH: mint, "/api/conversations": ok({}, 401)},
        retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000},
    )
    await rejects(f.client.conversations.create(member_ids=["synthetic-peer"]), error_code("AUTHENTICATION", 401))
    assert mints == 1
    assert f.server.count("/api/conversations") == 1


async def test_a_revoked_key_during_normal_call_renewal_fails_once_and_does_not_replay_the_resource(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        return json_response(MINT_RESULT) if mints == 1 else json_response({}, 401)

    f = fixture(
        {MINT_PATH: mint, "/api/org/members": ok({}, 401)},
        retry={"max_retries": 3, "initial_delay_ms": 0, "max_delay_ms": 0, "total_timeout_ms": 1000},
    )
    await rejects(f.client.members.list(), error_code("AUTHENTICATION", 401))
    assert mints == 2
    assert f.server.count("/api/org/members") == 1


async def test_renewal_cannot_change_actor_even_when_the_returned_email_and_organization_still_match(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        return json_response(MINT_RESULT)

    f = fixture(
        {
            MINT_PATH: mint,
            "/me": lambda *_: json_response({**IDENTITY, "user_uuid": ACTOR_ID if mints == 1 else "different-synthetic-actor"}),
            "/api/org/members": ok({}, 401),
        }
    )
    await rejects(f.client.members.list(), error_code("AUTHENTICATION"))
    await rejects(f.client.members.list(), error_code("AUTHENTICATION"))
    assert mints == 2
    assert f.server.count("/api/org/members") == 1


async def test_normal_authenticated_error_request_ids_cannot_expose_either_the_sdk_key_or_minted_token(fixture: Any) -> None:
    for secret in [SDK_KEY, TOKEN]:
        f = fixture({"/api/org/members": ok({"detail": MARKER}, 500, {"x-request-id": f"prefix-{secret}-suffix"})})

        def check(error: BaseException) -> bool:
            error_code("SERVER", 500)(error)
            assert isinstance(error, HumChatError) and error.request_id is None
            return True

        await rejects(f.client.members.list(), check)


async def test_sanitized_server_request_ids_remain_available_for_diagnosing_mint_failures(fixture: Any) -> None:
    f = fixture({MINT_PATH: ok({}, 403, {"x-request-id": "safe-mint-request-42"})})

    def check(error: BaseException) -> bool:
        error_code("PERMISSION", 403)(error)
        assert isinstance(error, HumChatError) and error.request_id == "safe-mint-request-42"
        return True

    await rejects(f.client.connect(), check)


def no_request_id(expected: str, status: int) -> Callable[[BaseException], bool]:
    def check(error: BaseException) -> bool:
        error_code(expected, status)(error)
        assert isinstance(error, HumChatError) and error.request_id is None
        return True

    return check


async def test_failed_sdk_key_renewal_cannot_expose_the_prior_bearer_token_in_a_mint_request_id(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        if mints == 1:
            return json_response(MINT_RESULT)
        return json_response({"detail": MARKER}, 500, {"x-request-id": f"prefix-{TOKEN}-suffix"})

    f = fixture({MINT_PATH: mint, "/api/org/members": ok({}, 401)})
    await f.client.connect()
    await rejects(f.client.members.list(), no_request_id("SERVER", 500))
    assert mints == 2
    assert f.server.count("/api/org/members") == 1, "A failed renewal must not replay the resource"


async def test_a_token_from_a_rejected_mint_response_remains_redacted_from_later_mint_request_ids(fixture: Any) -> None:
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        if mints == 1:
            return json_response({**MINT_RESULT, "expires_in": 0})
        return json_response({}, 500, {"x-request-id": f"prefix-{TOKEN}-suffix"})

    f = fixture({MINT_PATH: mint})
    await rejects(f.client.connect(), error_code("PROTOCOL"))
    await rejects(f.client.connect(), no_request_id("SERVER", 500))
    assert mints == 2
    assert f.paths() == [MINT_PATH, MINT_PATH]
    assert all("authorization" not in request.headers for request in f.requests)


async def test_successful_sdk_key_rotation_continues_redacting_a_retired_token_from_resource_request_ids(fixture: Any) -> None:
    replacement = "synthetic-request-id-replacement-token"
    mints = 0

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        return json_response({**MINT_RESULT, "access_token": TOKEN if mints == 1 else replacement})

    def members(request: Recorded, *_: Any) -> httpx.Response:
        if request.headers.get("authorization") == f"Bearer {TOKEN}":
            return json_response({}, 401)
        return json_response({"detail": MARKER}, 500, {"x-request-id": f"prefix-{TOKEN}-suffix"})

    f = fixture({MINT_PATH: mint, "/api/org/members": members})
    await f.client.connect()

    def check(error: BaseException) -> bool:
        no_request_id("SERVER", 500)(error)
        assert replacement not in exposed(error)
        return True

    await rejects(f.client.members.list(), check)
    assert mints == 2
    resources = [request for request in f.requests if request.path == "/api/org/members"]
    assert len(resources) == 2
    assert resources[1].headers.get("authorization") == f"Bearer {replacement}"


async def test_sdk_key_request_id_redaction_stays_private_after_bounded_token_history_is_saturated(fixture: Any) -> None:
    mints = 0
    renew = False
    error_request_id: str | None = None
    issued: list[str] = []

    def mint(*_: Any) -> httpx.Response:
        nonlocal mints
        mints += 1
        access_token = f"synthetic-history-token-{mints:04d}"
        issued.append(access_token)
        return json_response({**MINT_RESULT, "access_token": access_token})

    def members(*_: Any) -> httpx.Response:
        nonlocal renew
        if renew:
            renew = False
            return json_response({}, 401)
        if error_request_id is not None:
            return json_response({}, 500, {"x-request-id": error_request_id})
        return json_response([])

    f = fixture({MINT_PATH: mint, "/api/org/members": members})
    await f.client.connect()
    # More than the 128 remembered short secrets, without depending on whether the
    # configured SDK key itself occupies one of those history slots.
    for _ in range(130):
        renew = True
        assert await f.client.members.list() == []
    assert mints == 131
    for request_id in [f"prefix-{issued[0]}-suffix", f"prefix-{issued[-1]}-suffix", "otherwise-safe-request-after-history-saturation"]:
        error_request_id = request_id

        def check(error: BaseException) -> bool:
            no_request_id("SERVER", 500)(error)
            text = exposed(error) + json.dumps(vars(error), default=str)
            assert not any(secret in text for secret in issued)
            return True

        await rejects(f.client.members.list(), check)
