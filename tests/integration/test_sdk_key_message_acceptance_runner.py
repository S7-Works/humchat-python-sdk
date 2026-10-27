"""Port of test/integration/sdk-key-message-acceptance-runner.test.mjs."""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pytest

from sdk_key_dm_fixture import ALICE, BOB, MEMBERS, ORG, SDK_KEY, DmPeer, peer

RUNNER = Path(__file__).resolve().parent.parent / "acceptance" / "sdk_key_direct_messaging.py"
ROOT = Path(__file__).resolve().parent.parent.parent


def roster(status: str) -> list[dict[str, Any]]:
    return [
        {"user_uuid": member.id, "org_uuid": ORG, "name": member.name, "email": member.email, "role": "member",
         "status": status if member.id == BOB.id else "active"}
        for member in MEMBERS
    ]  # fmt: skip


def assert_sanitized(fixture: DmPeer, output: str) -> None:
    values = [SDK_KEY, ORG, *[value for member in MEMBERS for value in (member.id, member.email)], "synthetic-native-dm-",
              "synthetic-app-session-", "server-error-private-data",
              *[value for state in fixture.states.values() for value in (state["message_id"], state["body"])]]  # fmt: skip
    for value in values:
        assert value not in output, "Acceptance output must contain only sanitized status and counts"


async def execute(fixture: DmPeer, **overrides: str | None) -> dict[str, Any]:
    keep = ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "PROGRAMDATA", "TEMP", "TMP", "USERPROFILE", "APPDATA", "LOCALAPPDATA")
    inherited = {k: v for k, v in os.environ.items() if k.upper() in keep}
    configured: dict[str, str | None] = {
        "HUMCHAT_BASE_URL": fixture.origin, "HUMCHAT_ORG_ID": ORG, "HUMCHAT_SDK_KEY": SDK_KEY, "HUMCHAT_ACT_AS": ALICE.email,
        "HUMCHAT_TEST_RECIPIENT_EMAIL": BOB.email, "HUMCHAT_ACCEPTANCE_SEND": "true", "HUMCHAT_ACCEPTANCE_TIMEOUT_MS": "10000", **overrides,
    }  # fmt: skip
    env = {**inherited, **{k: v for k, v in configured.items() if v is not None}, "PYTHONPATH": str(ROOT / "src"), "PYTHONIOENCODING": "utf-8"}
    child = await asyncio.create_subprocess_exec(
        sys.executable, str(RUNNER), env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=str(ROOT)
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(child.communicate(), 20)
    except asyncio.TimeoutError:
        child.kill()
        await child.wait()
        raise AssertionError("Runner must end by itself and close all handles") from None
    stdout, stderr = stdout_bytes.decode(), stderr_bytes.decode()
    assert stderr == ""
    assert_sanitized(fixture, stdout + stderr)
    lines = stdout.strip().splitlines()
    assert len(lines) == 1, "CLI emits exactly one JSON report"
    report = json.loads(lines[0])
    assert child.returncode == (0 if report["passed"] else 1)
    return report


def assert_no_message_writes(fixture: DmPeer, report: dict[str, Any]) -> None:
    assert report["message_send_attempts"] == 0 and report["accepted_messages"] == 0
    assert len(fixture.states) == 0 and len(fixture.conversations) == 0
    assert not any(r["method"] == "POST" and r["path"] in ("/api/conversations", "/api/messages/send") for r in fixture.requests)


async def test_acceptance_cli_sends_exactly_two_native_auth_messages_and_verifies_both_actors_inboxes_and_four_history_reads() -> None:
    fixture = await peer("channel")
    try:
        report = await execute(fixture)
        assert report["passed"] is True and "failure" not in report
        assert (report["message_send_attempts"], report["accepted_messages"], report["history_matches"]) == (2, 2, 4)
        assert report["receiver_deliveries"] == 1 and report["sender_deliveries"] == 1
        assert report["receiver_live_deliveries"] + report["receiver_recovery_deliveries"] == 1
        assert report["sender_live_deliveries"] + report["sender_recovery_deliveries"] == 1
        assert report["checks"] == [
            "both_verified_member_identities", "both_messaging_capabilities", "both_rosters_identify_exact_active_participants",
            "both_conversation_scopes_and_capacity", "both_inboxes_ready_before_writes", "sender_direct_message_accepted",
            "receiver_inbox_received_direct_message", "both_conversation_streams_live_before_reply", "sender_inbox_received_thread_reply",
            "both_messages_persisted_in_both_member_histories",
        ]  # fmt: skip
        messages = list(fixture.states.values())
        assert len(messages) == 2 and len(fixture.conversations) == 1
        assert messages[0]["sender_id"] == ALICE.id and messages[0]["thread_root_id"] is None
        assert messages[1]["sender_id"] == BOB.id and messages[1]["thread_root_id"] == messages[0]["message_id"]
        assert messages[0]["channel_id"] == messages[1]["channel_id"]
        assert re.search("intentionally retained", messages[0]["body"]) and re.search("intentionally retained", messages[1]["body"])
        assert sorted(fixture.mints) == sorted([ALICE.id, BOB.id])
        assert sum(1 for r in fixture.requests if r["path"] == "/api/messages/send") == 2
    finally:
        await fixture.close()


@pytest.mark.parametrize(
    ("label", "overrides", "code"),
    [
        ("missing explicit write opt-in", {"HUMCHAT_ACCEPTANCE_SEND": None}, "OPT_IN_REQUIRED"),
        ("missing designated recipient", {"HUMCHAT_TEST_RECIPIENT_EMAIL": None}, "MISSING_CONFIGURATION"),
        ("same sender and recipient", {"HUMCHAT_TEST_RECIPIENT_EMAIL": f" {ALICE.email.upper()} "}, "DISTINCT_RECIPIENT_REQUIRED"),
        ("invalid deadline", {"HUMCHAT_ACCEPTANCE_TIMEOUT_MS": "1"}, "INVALID_DEADLINE"),
        ("credential-bearing origin", {"HUMCHAT_BASE_URL": "https://secret@example.invalid"}, "INVALID_ORIGIN"),
    ],
)
async def test_acceptance_cli_refuses_before_any_network_call(label: str, overrides: dict[str, str | None], code: str) -> None:
    fixture = await peer("channel")
    try:
        report = await execute(fixture, **overrides)
        assert report["passed"] is False and report["failure"] == {"code": code}, label
        assert len(fixture.requests) == 0
        assert_no_message_writes(fixture, report)
    finally:
        await fixture.close()


async def test_acceptance_cli_refuses_an_inactive_recipient_from_either_verified_roster_before_conversation_or_message_writes() -> None:
    def route(path: str, member: Any, **_: Any) -> Any:
        return (200, roster("disabled" if member.id == BOB.id else "active")) if path == "/api/org/members" else None

    fixture = await peer("channel", route=route)
    try:
        report = await execute(fixture)
        assert report["failure"] == {"code": "ROSTER_MISMATCH"}
        assert_no_message_writes(fixture, report)
    finally:
        await fixture.close()


async def test_acceptance_cli_refuses_different_emails_resolving_to_the_same_actor_before_any_writes() -> None:
    def route(path: str, member: Any, **_: Any) -> Any:
        if path == "/me":
            return 200, {"user_uuid": ALICE.id, "org": ORG, "email": member.email, "roles": ["member"], "scopes": []}
        return None

    fixture = await peer("channel", route=route)
    try:
        report = await execute(fixture)
        assert report["failure"] == {"code": "IDENTITY_MISMATCH"}
        assert_no_message_writes(fixture, report)
    finally:
        await fixture.close()


async def test_acceptance_cli_reports_an_http_preflight_failure_without_leaking_the_response_or_writing_messages() -> None:
    def route(path: str, **_: Any) -> Any:
        if path == "/api/conversations":
            return 403, {"reason": f"server-error-private-data {SDK_KEY} synthetic-native-dm-secret {BOB.email}"}
        return None

    fixture = await peer("channel", route=route)
    try:
        report = await execute(fixture)
        assert report["failure"] == {"code": "PERMISSION", "status": 403}
        assert_no_message_writes(fixture, report)
    finally:
        await fixture.close()


async def test_acceptance_cli_rejects_a_callback_payload_mismatch_and_stops_before_sending_a_reply() -> None:
    def event(value: dict[str, Any]) -> None:
        value["state"]["body"] = "server-error-private-data"
        value["state"]["original_body"] = value["state"]["body"]

    fixture = await peer("channel", event=event)
    try:
        report = await execute(fixture)
        assert report["failure"] == {"code": "DELIVERY_MISMATCH"}
        assert report["message_send_attempts"] == 1 and report["accepted_messages"] == 1
        assert report["history_matches"] == 0 and len(fixture.states) == 1
    finally:
        await fixture.close()


async def test_acceptance_cli_deadline_cancels_stalled_inbox_replay_closes_handles_and_never_sends_a_reply() -> None:
    async def route(path: str, **_: Any) -> Any:
        if path == "/api/events/sync":
            await asyncio.Event().wait()
        return None

    fixture = await peer("channel", route=route)
    try:
        report = await execute(fixture)
        assert report["failure"] == {"code": "DEADLINE_EXCEEDED"}
        assert report["message_send_attempts"] == 1 and report["accepted_messages"] == 1
        assert report["receiver_deliveries"] == 0 and len(fixture.states) == 1
    finally:
        await fixture.close()
