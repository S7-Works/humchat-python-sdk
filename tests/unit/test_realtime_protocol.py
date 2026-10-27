"""Port of test/unit/realtime-protocol.test.mjs.

Like the Node test, this exercises internal parser/cache invariants directly without making
them public API.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from typing import Any

import pytest

from helpers import SAMPLES
from humchat import BackpressureError, ProtocolError
from humchat._util import MAX_SAFE_INTEGER
from humchat._wire import parse_state
from humchat.realtime.protocol import ParsedFrame, message_from_state, parse_frame
from humchat.realtime.state_cache import StateCache
from humchat.types import MessageAttachment, MessageState, PresenceEvent

f: dict[str, Any] = SAMPLES
ORG_ID: str = f["identity"]["org"]
CHANNEL_ID: str = f["message"]["channel_id"]
SCOPED = {"org_uuid": ORG_ID, "channel_id": CHANNEL_ID}


def base() -> MessageState:
    return parse_state(copy.deepcopy(f["history"]["messages"][0]), ORG_ID, CHANNEL_ID)


def cache() -> StateCache:
    return StateCache(1000, 8 * 1024 * 1024)


def parse(raw: Any) -> ParsedFrame:
    return parse_frame(raw, ORG_ID, CHANNEL_ID)


def test_captured_creation_and_all_state_bearing_mutation_frames_normalize_once() -> None:
    created = parse({**f["message"], "additive": True})
    assert created.kind == "message"
    assert created.message is not None
    assert created.message.content == "!status"
    assert created.message.thread_root_id is None
    assert created.message.is_long is False
    for name, event in [("edit", "message_updated"), ("delete", "message_deleted"), ("reaction", "reaction_updated")]:
        frame = parse(f[name])
        assert frame.kind == "state"
        assert frame.event == event
        assert frame.state is not None and frame.state.id == f[name]["message_id"]


def test_legacy_edit_delete_reaction_are_hydration_hints_and_malformed_embedded_state_never_downgrades() -> None:
    for name in ["edit", "delete", "reaction"]:
        legacy = copy.deepcopy(f[name])
        del legacy["state"]
        assert parse(legacy).kind == "legacy_mutation"
        for state in [None, {}, {**f[name]["state"], "schema": 2}, {**f[name]["state"], "revision": MAX_SAFE_INTEGER + 1}]:
            with pytest.raises(ProtocolError):
                parse({**legacy, "state": state})


def test_all_actual_ephemeral_and_control_frame_shapes_normalize_with_bound_socket_scope() -> None:
    assert parse(f["typing"]).payload.ttl_ms == 6000
    assert parse(f["receipt"]).payload.through == f["receipt"]["ts"]
    presence = parse({"type": "presence", **SCOPED, "user_uuid": "actor", "status": "away", "last_seen": 100})
    assert presence.payload == PresenceEvent(org_id=ORG_ID, channel_id=CHANNEL_ID, actor_id="actor", status="away", last_seen=100)
    call = parse({"type": "call_invite", **SCOPED, "meeting_id": "meeting", "inviter_id": "actor", "invitee_id": "bot", "created_at": 100})
    assert call.kind == "call_invite"
    assert call.payload.invitee_id == "bot"
    assert parse({"type": "resync", "channel_id": CHANNEL_ID}) == ParsedFrame("resync")
    for reason in ["session expired", "session deprovisioned", "session no longer valid"]:
        assert parse({"type": "session_revoked", "reason": reason}) == ParsedFrame("session_revoked", reason=reason)
    assert parse({"type": "session_revoked", "reason": "untrusted secret details"}).reason == "session revoked"


def test_every_scoped_frame_rejects_contradictory_tenant_channel_identities() -> None:
    frames = [
        f["message"],
        f["typing"],
        f["receipt"],
        f["edit"],
        f["delete"],
        f["reaction"],
        {"type": "resync", "channel_id": CHANNEL_ID},
        {"type": "session_revoked", "reason": "session deprovisioned"},
        {"type": "future_signal"},
    ]
    for frame in frames:
        with pytest.raises(ProtocolError):
            parse({**frame, "org_uuid": "foreign"})
        with pytest.raises(ProtocolError):
            parse({**frame, "channel_id": f"{ORG_ID}:foreign"})
    with pytest.raises(ProtocolError):
        parse({**f["edit"], "state": {**f["edit"]["state"], "message_id": "foreign"}})


def test_unknown_event_metadata_is_bounded_prototype_names_are_never_mutation_kinds() -> None:
    for kind in ["constructor", "__proto__", "toString", "future_signal", "__class__", "__dict__"]:
        frame = parse({"type": kind, "secret": "never exposed"})
        assert frame.kind == "unknown"
        # Only kind and type are populated; no untrusted metadata is retained.
        assert frame == ParsedFrame("unknown", type=frame.type)
        assert "never exposed" not in repr(frame)
    assert parse({"type": "x" * 10000}).type == "unknown"
    with pytest.raises(ProtocolError):
        parse({**f["message"], "type": None})
    with pytest.raises(ProtocolError):
        parse([])


def test_creation_attachments_preserve_canonical_data_and_optional_asset_urls() -> None:
    attachment = {
        "upload_id": "upload",
        "content_hash": "a" * 64,
        "file_name": "image.png",
        "media_type": "image/png",
        "byte_len": 32,
        "url": "https://assets.example/image",
        "thumb_url": "https://assets.example/thumb",
    }
    parsed = parse({**f["message"], "attachments": [attachment]}).message
    assert parsed is not None
    assert parsed.attachments[0].ordinal == 0
    assert parsed.attachments[0].byte_length == 32
    assert parsed.attachments[0].thumbnail_url == attachment["thumb_url"]
    with pytest.raises(ProtocolError):
        parse({**f["message"], "attachments": [{**attachment, "byte_len": MAX_SAFE_INTEGER + 1}]})
    legacy = parse({**f["message"], "attachment_hash": attachment["content_hash"], "attachment_name": attachment["file_name"]}).message
    assert legacy is not None
    assert legacy.attachments[0].content_hash == attachment["content_hash"]
    assert legacy.attachments[0].byte_length is None
    legacy_vector = parse(
        {
            **f["message"],
            "attachments": [
                {"upload_id": "legacy:0", "content_hash": attachment["content_hash"], "file_name": "", "media_type": "application/octet-stream", "byte_len": -1}
            ],
        }
    ).message
    assert legacy_vector is not None
    assert legacy_vector.attachments[0].file_name is None
    assert legacy_vector.attachments[0].byte_length is None
    with pytest.raises(ProtocolError):
        parse({**f["message"], "attachments": [{**attachment, "byte_len": -2}]})
    singular = parse({**f["message"], "attachment_hash": attachment["content_hash"]}).message
    assert singular is not None
    assert singular.attachments == legacy_vector.attachments


def test_attachment_identities_agree_across_live_state_frames_while_refreshed_urls_are_ignored() -> None:
    live = parse(
        {
            **f["message"],
            "attachments": [
                {
                    "upload_id": "upload",
                    "content_hash": "a" * 64,
                    "file_name": "image.png",
                    "media_type": "image/png",
                    "byte_len": 32,
                    "url": "https://assets.example/first",
                }
            ],
        }
    ).message
    assert live is not None
    state = replace(
        base(),
        attachments=[
            MessageAttachment(a.ordinal, a.upload_id, a.content_hash, a.file_name, a.media_type, a.byte_length, a.attachment_variants) for a in live.attachments
        ],
    )
    store = cache()
    assert store.observe_message(live) is True
    store.merge(state)
    refreshed = replace(live, attachments=[replace(a, url="https://assets.example/refreshed") for a in live.attachments])
    assert store.observe_message(refreshed) is False
    with pytest.raises(ProtocolError):
        store.merge(replace(state, attachments=[replace(a, content_hash="b" * 64) for a in state.attachments]))
    state_first = cache()
    state_first.merge(state)
    with pytest.raises(ProtocolError):
        state_first.observe_message(replace(live, attachments=[]))


def test_state_merge_preserves_independently_newer_content_and_reactions_in_both_arrival_orders() -> None:
    a = base()
    edit = replace(a, revision=2, body="changed", updated_at=a.updated_at + 1)
    reaction = replace(a, reaction_revision=1, reactions={"✅": ["actor"]})
    for ordered in [[edit, reaction], [reaction, edit]]:
        store = cache()
        store.merge(a)
        for following in ordered:
            store.merge(following)
        final = store.get(a.id)
        assert final is not None
        assert final.body == "changed"
        assert final.revision == 2
        assert final.reaction_revision == 1
        assert final.reactions == {"✅": ["actor"]}
        assert store.merge(a).changed is False
        current = store.get(a.id)
        assert current is not None and current.body == "changed"


def test_cache_rejects_immutable_changes_and_contradictory_equal_revisions() -> None:
    a = base()
    store = cache()
    store.merge(a)
    changes: list[dict[str, Any]] = [
        {"sender_id": "other"},
        {"org_id": "other"},
        {"channel_id": "other"},
        {"original_body": "other"},
        {"body": "different"},
        {"updated_at": a.updated_at + 1},
        {"reactions": {"yes": ["actor"]}},
    ]
    for change in changes:
        with pytest.raises(ProtocolError):
            store.merge(replace(a, **change))
    current = store.get(a.id)
    assert current is not None and current.body == a.body


def test_creation_dedup_remains_separate_from_revision_one_snapshots_and_rejects_stale_resurrection() -> None:
    a = base()
    store = cache()
    store.merge(a)
    assert store.observe_message(message_from_state(a)) is True
    assert store.observe_message(message_from_state(a)) is False
    with pytest.raises(ProtocolError):
        store.observe_message(replace(message_from_state(a), content="conflict"))
    tombstones = cache()
    tombstones.merge(replace(a, revision=2, deleted=True, updated_at=a.updated_at + 1))
    assert tombstones.observe_message(message_from_state(a)) is False
    edited = cache()
    edited.merge(replace(a, revision=2, body="edit", updated_at=a.updated_at + 1))
    assert edited.observe_message(message_from_state(a)) is False


def test_recovered_full_original_text_and_a_later_live_long_snippet_deduplicate_compatibly() -> None:
    text = "😀" * 1000
    a = replace(base(), body=text, original_body=text)
    full = message_from_state(a)
    snippet = replace(full, is_long=True, content="😀" * 900 + "…", body_digest="a" * 64)
    for first, second in [(full, snippet), (snippet, full)]:
        store = cache()
        assert store.observe_message(first) is True
        store.merge(a)
        assert store.observe_message(second) is False
        assert store.observe_message(full) is False
        with pytest.raises(ProtocolError):
            store.observe_message(replace(snippet, content="wrong snippet"))
        with pytest.raises(ProtocolError):
            store.observe_message(replace(snippet, body_digest="b" * 64))


def test_cache_copies_caller_data_uses_lru_eviction_and_enforces_entry_byte_budgets() -> None:
    a = base()
    store = StateCache(2, 10000)
    first = store.merge(a)
    a.body = "caller changed input"
    first.state.body = "caller changed output"
    stored = store.get(a.id)
    assert stored is not None and stored.body != a.body
    fetched = store.get(a.id)
    assert fetched is not None
    fetched.reactions["oops"] = ["caller"]
    again = store.get(a.id)
    assert again is not None and "oops" not in again.reactions
    store.merge(replace(base(), id="second"))
    store.get(a.id)
    store.merge(replace(base(), id="third"))
    assert store.ids() == [a.id, "third"]
    assert store.size == 2
    assert store.bytes <= 10000
    tiny = StateCache(1, 10)
    with pytest.raises(BackpressureError):
        tiny.merge(base())
    assert tiny.size == 0
    store.clear()
    assert store.size == 0
    assert store.bytes == 0
