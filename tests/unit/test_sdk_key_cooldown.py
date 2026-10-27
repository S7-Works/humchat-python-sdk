"""Port of test/unit/sdk-key-cooldown.test.mjs.

Synthetic identities only. Virtual time keeps outage tests independent of host load.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import pytest

from helpers import FakeClock, MockServer, Observed, Recorded, json_response
from humchat import AbortController, HumChat

ORG_ID = "01234567-89ab-cdef-0123-456789abcdef"
ACTOR_ID = "synthetic-cooldown-actor"
ACT_AS = "synthetic.cooldown@example.invalid"
MINT_PATH = "/api/sdk/auth/user-token"
IDENTITY = {"user_uuid": ACTOR_ID, "org": ORG_ID, "email": ACT_AS, "roles": ["member"], "expires_at": None}
CAPABILITIES = {
    "sdk_protocol_version": 1,
    "authentication": {
        "user_sessions": True,
        "service_identities": False,
        "api_key_exchange": False,
        "app_binding_required": False,
        "mock_authentication": False,
        "sdk_key_user_tokens": {"endpoint": MINT_PATH, "key_header": "X-HumChat-Sdk-Key", "configured": True},
    },
    "messaging": {
        "send_receipt_versions": [1],
        "message_state_versions": [1],
        "history_versions": [2],
        "durable_replay": False,
        "websocket_scope": "channel",
    },
}
MINT_RESULT = {"access_token": "synthetic-cooldown-token", "token_type": "Bearer", "expires_in": 900}


@dataclass
class Request:
    path: str
    at: float
    entry: Recorded


@dataclass
class Fixture:
    client: HumChat
    requests: list[Request]
    clock: FakeClock

    def mints(self) -> list[Request]:
        return [request for request in self.requests if request.path == MINT_PATH]


@pytest.fixture
async def clients() -> AsyncIterator[list[HumChat]]:
    created: list[HumChat] = []
    yield created
    for client in created:
        await client.close()


@pytest.fixture
def make(monkeypatch: pytest.MonkeyPatch, clients: list[HumChat]) -> Callable[..., Fixture]:
    def build(handlers: dict[str, Callable[..., Any]] | None = None, options: dict[str, Any] | None = None, random: float = 0) -> Fixture:
        handlers = handlers or {}
        options = options or {}
        clock = FakeClock(monkeypatch, now=1_800_000_000_000, random=random)
        requests: list[Request] = []

        def handle(entry: Recorded) -> Any:
            request = Request(entry.path, clock.now, entry)
            requests.append(request)
            if entry.path in handlers:
                return handlers[entry.path](request, requests)
            if entry.path == MINT_PATH:
                return json_response(MINT_RESULT)
            if entry.path == "/me":
                return json_response(IDENTITY)
            if entry.path == "/api/sdk/capabilities":
                return json_response(CAPABILITIES)
            assert entry.path == "/api/org/members"
            return json_response([])

        config: dict[str, Any] = {
            "org_id": ORG_ID,
            "base_url": "https://humchat.example.invalid",
            "auth": {"sdk_key": "hsk_synthetic_cooldown_key", "act_as": ACT_AS},
            "transport": MockServer(handle).transport,
            "timeout_ms": 120_000,
            "retry": {"max_retries": 0, "total_timeout_ms": 120_000},
            **options,
        }
        if "token_provider" in options:
            del config["auth"]
        client = HumChat(**config)
        clients.append(client)
        return Fixture(client, requests, clock)

    return build


def observe(awaitable: Any) -> Observed:
    return Observed(awaitable)


async def rejected(fixture: Fixture, result: Observed, code: str) -> BaseException:
    await fixture.clock.drain()
    assert result.state == "rejected", result.state
    assert result.error is not None
    assert getattr(result.error, "code", None) == code, repr(result.error)
    return result.error


async def advance(fixture: Fixture, milliseconds: float) -> None:
    # JavaScript starts an async call synchronously; let freshly observed coroutines reach
    # their first await (creating their deadlines) before virtual time moves.
    for _ in range(10):
        await asyncio.sleep(0)
    await fixture.clock.advance(milliseconds)


def fail_with(status: int, headers: dict[str, str] | None = None) -> Callable[..., Any]:
    return lambda *_: json_response({}, status, headers)


async def test_successive_exhausted_sdk_key_acquisitions_back_off_across_flights_and_cap_the_shared_delay(make: Any) -> None:
    f = make({MINT_PATH: fail_with(503)})
    await rejected(f, observe(f.client.connect()), "SERVER")
    for wait in [1_000, 2_000, 4_000, 8_000, 15_000, 15_000]:
        count = len(f.mints())
        following = observe(f.client.connect())
        await f.clock.drain()
        assert len(f.mints()) == count, "Clearing a failed flight must not permit an immediate new mint"
        await advance(f, wait - 1)
        assert len(f.mints()) == count, "Cooldown must protect its entire lower-bound interval"
        assert following.state == "pending"
        await advance(f, 1)
        await rejected(f, following, "SERVER")
        assert len(f.mints()) == count + 1


async def test_sdk_key_acquisition_cooldown_includes_jitter_up_to_its_upper_bound(make: Any) -> None:
    f = make({MINT_PATH: fail_with(502)}, {}, 0.999999)
    await rejected(f, observe(f.client.connect()), "SERVER")
    following = observe(f.client.connect())
    await advance(f, 1_998)
    assert len(f.mints()) == 1
    assert following.state == "pending"
    await advance(f, 2)
    await rejected(f, following, "SERVER")
    assert len(f.mints()) == 2
    assert 1_999 <= f.mints()[1].at - f.mints()[0].at <= 2_000


async def test_a_retryable_network_failure_cannot_trigger_immediate_sdk_key_minting_from_the_next_caller(make: Any) -> None:
    calls = 0

    def mint(*_: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic network failure")
        return json_response(MINT_RESULT)

    f = make({MINT_PATH: mint})
    await rejected(f, observe(f.client.connect()), "NETWORK")
    following = observe(f.client.connect())
    await advance(f, 999)
    assert len(f.mints()) == 1
    await advance(f, 1)
    assert following.state == "fulfilled"
    assert len(f.mints()) == 2


async def test_an_attempted_mint_timeout_starts_cooldown_at_failure_time_and_can_recover_afterward(make: Any) -> None:
    calls = 0

    async def never() -> Any:
        await asyncio.get_running_loop().create_future()

    def mint(*_: Any) -> Any:
        nonlocal calls
        calls += 1
        return never() if calls == 1 else json_response(MINT_RESULT)

    f = make({MINT_PATH: mint}, {"timeout_ms": 200})
    initial = observe(f.client.connect())
    await f.clock.drain()
    assert len(f.mints()) == 1
    await advance(f, 200)
    await rejected(f, initial, "TIMEOUT")
    assert f.mints()[0].entry.cancelled is True
    following = observe(f.client.connect())
    await advance(f, 999)
    assert len(f.mints()) == 1
    await advance(f, 1)
    assert following.state == "fulfilled"
    assert len(f.mints()) == 2
    assert f.mints()[1].at - f.mints()[0].at == 1_200


@pytest.mark.parametrize("status", [429, 502, 503])
async def test_sdk_key_retry_after_remains_effective_after_the_failed_flight_is_cleared(make: Any, status: int) -> None:
    f = make({MINT_PATH: fail_with(status, {"retry-after": "7"})})
    expected = "RATE_LIMIT" if status == 429 else "SERVER"
    await rejected(f, observe(f.client.connect()), expected)
    following = observe(f.client.connect())
    await advance(f, 6_999)
    assert len(f.mints()) == 1
    assert following.state == "pending"
    await advance(f, 1)
    await rejected(f, following, expected)
    assert len(f.mints()) == 2


@pytest.mark.parametrize("path", ["/me", "/api/sdk/capabilities"])
async def test_transient_verification_failure_also_limits_subsequent_sdk_key_minting(make: Any, path: str) -> None:
    f = make({path: fail_with(503)})
    await rejected(f, observe(f.client.connect()), "SERVER")
    assert len(f.mints()) == 1
    following = observe(f.client.connect())
    await advance(f, 999)
    assert len(f.mints()) == 1
    await advance(f, 1)
    await rejected(f, following, "SERVER")
    assert len(f.mints()) == 2
    assert sum(1 for request in f.requests if request.path == path) == 2


async def test_concurrent_http_and_connect_callers_share_one_cooldown_and_one_recovered_acquisition(make: Any) -> None:
    calls = 0

    def mint(*_: Any) -> Any:
        nonlocal calls
        calls += 1
        return json_response({}, 503) if calls == 1 else json_response(MINT_RESULT)

    f = make({MINT_PATH: mint})
    await rejected(f, observe(f.client.connect()), "SERVER")
    pending = [
        observe(f.client.connect()),
        observe(f.client.get_capabilities()),
        observe(f.client.members.list()),
        observe(f.client.connect()),
    ]
    await advance(f, 999)
    assert len(f.mints()) == 1
    assert all(result.state == "pending" for result in pending)
    await advance(f, 1)
    assert all(result.state == "fulfilled" for result in pending), [result.state for result in pending]
    assert len(f.mints()) == 2


async def test_aborting_one_cooldown_waiter_leaves_the_shared_acquisition_available_to_another_caller(make: Any) -> None:
    calls = 0

    def mint(*_: Any) -> Any:
        nonlocal calls
        calls += 1
        return json_response({}, 503) if calls == 1 else json_response(MINT_RESULT)

    f = make({MINT_PATH: mint})
    await rejected(f, observe(f.client.connect()), "SERVER")
    controller = AbortController()
    cancelled = observe(f.client.connect(signal=controller.signal))
    survivor = observe(f.client.connect())
    await f.clock.drain()
    controller.abort(RuntimeError("synthetic caller cancellation"))
    await rejected(f, cancelled, "ABORT")
    await advance(f, 999)
    assert len(f.mints()) == 1
    assert survivor.state == "pending"
    await advance(f, 1)
    assert survivor.state == "fulfilled"
    assert len(f.mints()) == 2


@pytest.mark.parametrize("action", ["close", "revoke"])
async def test_action_cancels_a_pending_sdk_key_cooldown_without_transmitting_another_key(make: Any, action: str) -> None:
    f = make({MINT_PATH: fail_with(503)})
    await rejected(f, observe(f.client.connect()), "SERVER")
    waiting = observe(f.client.connect())
    await f.clock.drain()
    if action == "close":
        await f.client.close()
    else:
        f.client.realtime.revoke("synthetic terminal revocation")
    expected = "ABORT" if action == "close" else "SESSION_REVOKED"
    await rejected(f, waiting, expected)
    await advance(f, 30_000)
    assert len(f.mints()) == 1
    await rejected(f, observe(f.client.connect()), expected)
    assert len(f.mints()) == 1


async def test_cooldown_consumes_the_acquisition_deadline_and_a_delay_only_timeout_does_not_extend_it(make: Any) -> None:
    calls = 0

    def mint(*_: Any) -> Any:
        nonlocal calls
        calls += 1
        return json_response({}, 503) if calls == 1 else json_response(MINT_RESULT)

    f = make({MINT_PATH: mint}, {"retry": {"max_retries": 0, "total_timeout_ms": 600}})
    await rejected(f, observe(f.client.connect()), "SERVER")
    timed_out = observe(f.client.connect())
    await advance(f, 599)
    assert timed_out.state == "pending"
    assert len(f.mints()) == 1
    await advance(f, 1)
    await rejected(f, timed_out, "TIMEOUT")
    assert len(f.mints()) == 1, "A timeout spent waiting must not attempt minting"
    recovered = observe(f.client.connect())
    await advance(f, 399)
    assert recovered.state == "pending"
    assert len(f.mints()) == 1
    await advance(f, 1)
    assert recovered.state == "fulfilled", "The original cooldown deadline must survive a waiting-only timeout"
    assert len(f.mints()) == 2


async def test_a_successful_verified_session_resets_accumulated_sdk_key_acquisition_backoff(make: Any) -> None:
    fail = True
    f = make({MINT_PATH: lambda *_: json_response({}, 503) if fail else json_response(MINT_RESULT)})
    await rejected(f, observe(f.client.connect()), "SERVER")
    following = observe(f.client.connect())
    await advance(f, 1_000)
    await rejected(f, following, "SERVER")
    fail = False
    following = observe(f.client.connect())
    await advance(f, 2_000)
    assert following.state == "fulfilled"
    session = await f.client.realtime.sessions.get(AbortController().signal)
    f.client.realtime.sessions.expire(session)
    fail = True
    await rejected(f, observe(f.client.connect()), "SERVER")
    count = len(f.mints())
    following = observe(f.client.connect())
    await advance(f, 999)
    assert len(f.mints()) == count
    await advance(f, 1)
    await rejected(f, following, "SERVER")
    assert len(f.mints()) == count + 1, "A new outage must restart at the initial cooldown"


@pytest.mark.parametrize(("status", "expected"), [(400, "VALIDATION"), (401, "AUTHENTICATION"), (403, "PERMISSION")])
async def test_sdk_key_status_stays_terminal_for_its_request_without_a_transient_retry_loop(make: Any, status: int, expected: str) -> None:
    f = make({MINT_PATH: fail_with(status)}, {"retry": {"max_retries": 3, "total_timeout_ms": 120_000}})
    await rejected(f, observe(f.client.connect()), expected)
    assert len(f.mints()) == 1
    await rejected(f, observe(f.client.connect()), expected)
    # Bad input may be requested again explicitly; denied immutable credentials remain blocked.
    assert len(f.mints()) == (2 if status == 400 else 1)
    await advance(f, 30_000)
    assert len(f.mints()) == (2 if status == 400 else 1)


async def test_legacy_token_provider_acquisition_failures_do_not_acquire_sdk_key_cooldown_behavior(make: Any) -> None:
    providers = 0

    async def provider(_: Any) -> dict[str, str]:
        nonlocal providers
        providers += 1
        return {"token": MINT_RESULT["access_token"]}

    f = make({"/me": fail_with(503)}, {"token_provider": provider})
    await rejected(f, observe(f.client.connect()), "SERVER")
    await rejected(f, observe(f.client.connect()), "SERVER")
    assert providers == 2
    assert len(f.mints()) == 0
    assert len(f.requests) == 2
    assert f.requests[0].at == f.requests[1].at, "Legacy caller-controlled acquisition timing must be preserved"
