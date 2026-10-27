"""Channel WebSocket frame parsing (already-decoded JSON)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .._wire import (
    Invalid,
    array,
    assert_scope,
    boolean,
    bounded_text,
    check,
    digest,
    emoji,
    identity,
    nullable_identity,
    obj,
    parse_state,
    priority,
    required,
    safe_int,
    timestamp,
)
from ..errors import ProtocolError
from ..types import CallInviteEvent, LiveAttachment, Message, MessageState, PresenceEvent, ReceiptEvent, TypingEvent

_MUTATIONS = {"edit": "message_updated", "delete": "message_deleted", "reaction": "reaction_updated"}
_UNKNOWN_TYPE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.:-]{0,63}$")
_KNOWN_REASONS = ("session deprovisioned", "session no longer valid", "session expired")


@dataclass
class ParsedFrame:
    """``kind`` is message | state | legacy_mutation | typing | presence | receipt | call_invite |
    resync | session_revoked | session_expired | unknown."""

    kind: str
    message: Message | None = None
    event: str | None = None
    state: MessageState | None = None
    message_id: str | None = None
    payload: Any = None
    reason: str | None = None
    type: str | None = None


def _read(parse: Any, value: Any) -> Any:
    try:
        return parse(value)
    except Invalid:
        raise ProtocolError("Invalid realtime frame") from None


def _nullable_text(v: Mapping[str, Any], key: str, limit: int) -> str | None:
    value = v.get(key)
    return None if value is None else bounded_text(value, limit)


def _bound(value: Mapping[str, Any], org_id: str, channel_id: str, required_scope: bool = True) -> None:
    if required_scope or "org_uuid" in value:
        assert_scope(value.get("org_uuid") == org_id, "Realtime organization")
    if required_scope or "channel_id" in value:
        assert_scope(value.get("channel_id") == channel_id, "Realtime channel")


def _scope(v: Mapping[str, Any]) -> None:
    identity(required(v, "org_uuid"))
    identity(required(v, "channel_id"), 1024)


def _actor(v: Mapping[str, Any]) -> str:
    _scope(v)
    return identity(required(v, "user_uuid"), 256)


def _message_schema(raw: Any) -> dict[str, Any]:
    v = obj(raw)
    _scope(v)
    attachments = v.get("attachments")
    parsed_attachments = []
    if attachments is not None:
        for item in array(attachments, 64):
            a = obj(item)
            parsed_attachments.append(
                {
                    "upload_id": identity(required(a, "upload_id")),
                    "content_hash": digest(required(a, "content_hash")),
                    "file_name": bounded_text(required(a, "file_name"), 1024),
                    "media_type": bounded_text(required(a, "media_type"), 256),
                    "byte_len": safe_int(required(a, "byte_len"), -1),
                    "attachment_variants": _nullable_text(a, "attachment_variants", 65_536),
                    "url": _nullable_text(a, "url", 16_384),
                    "thumb_url": _nullable_text(a, "thumb_url", 16_384),
                }
            )
    body_digest = v.get("body_blake3")
    attachment_hash = v.get("attachment_hash")
    result = {
        "id": identity(required(v, "id")),
        "sender_id": identity(required(v, "sender_id"), 256),
        "content": bounded_text(required(v, "content"), 1_048_576),
        "thread_root_id": nullable_identity(required(v, "thread_root_id")),
        "created_at": timestamp(required(v, "created_at")),
        "is_long": boolean(required(v, "is_long")),
        "priority": priority(required(v, "priority")),
        "body_blake3": None if body_digest is None else digest(body_digest),
        "attachments": parsed_attachments,
        "attachment_hash": None if attachment_hash is None else digest(attachment_hash),
        "attachment_name": _nullable_text(v, "attachment_name", 1024),
        "attachment_variants": _nullable_text(v, "attachment_variants", 65_536),
        "attachment_url": _nullable_text(v, "attachment_url", 16_384),
        "attachment_thumb_url": _nullable_text(v, "attachment_thumb_url", 16_384),
        "org_uuid": v["org_uuid"],
        "channel_id": v["channel_id"],
    }
    check(result["thread_root_id"] != result["id"])
    return result


def _message(raw: Any, org_id: str, channel_id: str) -> Message:
    v = _read(_message_schema, raw)
    _bound(v, org_id, channel_id)
    attachments = [
        LiveAttachment(
            ordinal=ordinal,
            upload_id=a["upload_id"],
            content_hash=a["content_hash"],
            file_name=a["file_name"] or None,
            media_type=a["media_type"] or None,
            byte_length=None if a["byte_len"] < 0 else a["byte_len"],
            attachment_variants=a["attachment_variants"],
            url=a["url"],
            thumbnail_url=a["thumb_url"],
        )
        for ordinal, a in enumerate(v["attachments"])
    ]
    # The legacy singular attachment projection is still emitted by the server. Versioned
    # state converts its empty name and negative unknown length into null values.
    if not attachments and v["attachment_hash"] is not None:
        attachments.append(
            LiveAttachment(
                ordinal=0,
                upload_id="legacy:0",
                content_hash=v["attachment_hash"],
                file_name=v["attachment_name"] or None,
                media_type="application/octet-stream",
                byte_length=None,
                attachment_variants=v["attachment_variants"],
                url=v["attachment_url"],
                thumbnail_url=v["attachment_thumb_url"],
            )
        )
    if attachments and v["attachment_hash"] is not None:
        assert_scope(attachments[0].content_hash == v["attachment_hash"], "Realtime attachment projection")
    return Message(
        id=v["id"],
        org_id=org_id,
        channel_id=channel_id,
        sender_id=v["sender_id"],
        content=v["content"],
        thread_root_id=v["thread_root_id"],
        created_at=v["created_at"],
        priority=v["priority"],
        is_long=v["is_long"],
        attachments=attachments,
        body_digest=v["body_blake3"],
    )


def parse_frame(raw: Any, org_id: str, channel_id: str) -> ParsedFrame:
    """The socket's verified identity supplies missing control-frame scope."""
    value: Mapping[str, Any] = _read(obj, raw)
    if "type" in value and not isinstance(value["type"], str):
        raise ProtocolError("Invalid realtime discriminator")
    if "type" not in value:
        if "state" in value:
            raise ProtocolError("Unexpected state in a message creation frame")
        return ParsedFrame("message", message=_message(value, org_id, channel_id))
    kind: str = value["type"]
    if kind in _MUTATIONS:
        event = _MUTATIONS[kind]

        def envelope(v: Any) -> str:
            _scope(v)
            return identity(required(v, "message_id"))

        message_id = _read(envelope, value)
        _bound(value, org_id, channel_id)
        if "state" in value:
            # Presence, not truthiness: a malformed/null state must never become a legacy hint.
            state = parse_state(value["state"], org_id, channel_id, message_id)
            if kind == "delete" and not state.deleted:
                raise ProtocolError("Delete frame has no tombstone")
            if kind == "edit" and state.deleted:
                raise ProtocolError("Edit frame contains a tombstone")
            return ParsedFrame("state", event=event, state=state)

        def legacy(v: Any) -> None:
            if kind == "edit":
                bounded_text(required(v, "content"), 1_048_576)
                timestamp(required(v, "edited_at"))
            elif kind == "delete":
                timestamp(required(v, "deleted_at"))
            else:
                reactions = obj(required(v, "reactions"))
                for key, actors in reactions.items():
                    emoji(key)
                    for actor in array(actors, 16_384):
                        identity(actor, 256)

        _read(legacy, value)
        return ParsedFrame("legacy_mutation", event=event, message_id=message_id)
    if kind == "typing":

        def typing(v: Any) -> TypingEvent:
            actor = _actor(v)
            return TypingEvent(org_id, channel_id, actor, timestamp(required(v, "ts")), safe_int(required(v, "ttl_ms"), 0, 4_294_967_295))

        payload: Any = _read(typing, value)
        _bound(value, org_id, channel_id)
        return ParsedFrame("typing", payload=payload)
    if kind == "presence":

        def presence(v: Any) -> PresenceEvent:
            actor = _actor(v)
            status = required(v, "status")
            check(isinstance(status, str) and status in ("online", "away", "busy", "offline"))
            return PresenceEvent(org_id, channel_id, actor, status, timestamp(required(v, "last_seen")))  # type: ignore[arg-type]

        payload = _read(presence, value)
        _bound(value, org_id, channel_id)
        return ParsedFrame("presence", payload=payload)
    if kind == "receipt":

        def receipt(v: Any) -> ReceiptEvent:
            actor = _actor(v)
            return ReceiptEvent(org_id, channel_id, actor, safe_int(required(v, "ts"), 0, 99_999_999_999))

        payload = _read(receipt, value)
        _bound(value, org_id, channel_id)
        return ParsedFrame("receipt", payload=payload)
    if kind == "call_invite":

        def invite(v: Any) -> CallInviteEvent:
            _scope(v)
            return CallInviteEvent(
                org_id,
                channel_id,
                identity(required(v, "meeting_id")),
                identity(required(v, "inviter_id"), 256),
                identity(required(v, "invitee_id"), 256),
                timestamp(required(v, "created_at")),
            )

        payload = _read(invite, value)
        _bound(value, org_id, channel_id)
        return ParsedFrame("call_invite", payload=payload)
    if kind == "resync":
        _read(lambda v: identity(required(v, "channel_id"), 1024), value)
        _bound(value, org_id, channel_id, False)
        return ParsedFrame("resync")
    if kind == "session_revoked":
        _read(lambda v: bounded_text(required(v, "reason"), 1024), value)
        _bound(value, org_id, channel_id, False)
        if value.get("code") == "TOKEN_EXPIRED":
            return ParsedFrame("session_expired")
        # Do not turn arbitrary server strings into logs; known wire reasons are benign constants.
        reason = value["reason"] if value["reason"] in _KNOWN_REASONS else "session revoked"
        return ParsedFrame("session_revoked", reason=reason)
    _bound(value, org_id, channel_id, False)
    return ParsedFrame("unknown", type=kind if _UNKNOWN_TYPE.fullmatch(kind) else "unknown")


def message_from_state(state: MessageState) -> Message:
    """Snapshot conversion is not an event-history reconstruction; callers choose whether to emit it."""
    return Message(
        id=state.id,
        org_id=state.org_id,
        channel_id=state.channel_id,
        sender_id=state.sender_id,
        content=state.body,
        thread_root_id=state.thread_root_id,
        created_at=state.created_at,
        priority=state.priority,
        is_long=False,
        attachments=[
            LiveAttachment(
                ordinal=a.ordinal,
                upload_id=a.upload_id,
                content_hash=a.content_hash,
                file_name=a.file_name,
                media_type=a.media_type,
                byte_length=a.byte_length,
                attachment_variants=a.attachment_variants,
                url=None,
                thumbnail_url=None,
            )
            for a in state.attachments
        ],
    )
