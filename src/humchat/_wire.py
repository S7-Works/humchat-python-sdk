"""HTTP wire validation and normalization.

Bounds and invariants mirror the server's chat-state contract. Additive response fields
are allowed; known fields and tenant/channel identity are always checked.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, NoReturn

from ._util import MAX_SAFE_INTEGER, is_safe_int, js_trim, json_loads, utf8_len, utf16_len
from .errors import ProtocolError, ValidationError
from .types import (
    Channel,
    ChannelList,
    Conversation,
    ConversationMember,
    HistoryPage,
    MessageAttachment,
    MessageState,
    MutationReceipt,
    SendReceipt,
)

_IDENTITY = re.compile(r"^[A-Za-z0-9._:-]+$")
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_CURSOR_WIRE = re.compile(r"^(?:[a-f0-9]{2})+$")
_CONTROL = re.compile(r"[\u0000-\u001f\u007f-\u009f]")
PRIORITIES = ("standard", "important", "urgent")


class Invalid(Exception):
    """Internal schema mismatch; converted to a public error by the caller."""


def fail() -> NoReturn:
    raise Invalid


def check(condition: bool) -> None:
    if not condition:
        raise Invalid


# --- primitive checks (each raises Invalid) ------------------------------------------------


def identity(value: Any, maximum: int = 128) -> str:
    check(isinstance(value, str) and 1 <= len(value) <= maximum and _IDENTITY.fullmatch(value) is not None and value not in (".", ".."))
    return value


def nullable_identity(value: Any, maximum: int = 128) -> str | None:
    return None if value is None else identity(value, maximum)


def string(value: Any, minimum: int = 0, maximum: int | None = None) -> str:
    """``z.string().min().max()``; lengths are UTF-16 code units like JavaScript."""
    check(isinstance(value, str))
    length = utf16_len(value)
    check(length >= minimum and (maximum is None or length <= maximum))
    return value


def bounded_text(value: Any, maximum: int) -> str:
    check(isinstance(value, str) and utf8_len(value) <= maximum)
    return value


def boolean(value: Any) -> bool:
    check(isinstance(value, bool))
    return value


def safe_int(value: Any, minimum: int | None = None, maximum: int | None = None) -> int:
    check(is_safe_int(value))
    number = int(value)
    check((minimum is None or number >= minimum) and (maximum is None or number <= maximum))
    return number


def timestamp(value: Any) -> int:
    return safe_int(value, 1, 99_999_999_999)


def nullable(value: Any, parse: Any, *args: Any) -> Any:
    return None if value is None else parse(value, *args)


def digest(value: Any) -> str:
    check(isinstance(value, str) and _DIGEST.fullmatch(value) is not None)
    return value


def obj(value: Any) -> Mapping[str, Any]:
    check(isinstance(value, Mapping))
    return value


def array(value: Any, maximum: int | None = None, minimum: int = 0) -> list[Any]:
    check(isinstance(value, list) and len(value) >= minimum and (maximum is None or len(value) <= maximum))
    return value


def literal(value: Any, *expected: Any) -> Any:
    check(not isinstance(value, bool) and isinstance(value, (str, int, float)) and value in expected)
    return value


def priority(value: Any) -> str:
    check(isinstance(value, str) and value in PRIORITIES)
    return value


def body_text(value: Any) -> str:
    check(isinstance(value, str) and utf8_len(value) <= 1_048_576 and len(js_trim(value)) > 0)
    return value


def emoji(value: Any) -> str:
    check(isinstance(value, str) and utf8_len(value) <= 32 and utf16_len(value) > 0 and js_trim(value) == value and not _CONTROL.search(value))
    return value


def revision(value: Any) -> int:
    return safe_int(value, 1)


def cursor_wire(value: Any) -> str:
    check(isinstance(value, str) and 2 <= len(value) <= 4096 and _CURSOR_WIRE.fullmatch(value) is not None)
    return value


def required(source: Mapping[str, Any], key: str) -> Any:
    check(key in source)
    return source[key]


def optional(source: Mapping[str, Any], key: str) -> Any:
    """Missing and explicit ``None`` are both absent for ``.optional()``-only fields."""
    return source.get(key)


# --- public error conversion ----------------------------------------------------------------


def input_error(label: str, **intent: Any) -> ValidationError:
    return ValidationError(f"Invalid {label}", **intent)


def validate_input(parse: Any, value: Any, label: str, *args: Any) -> Any:
    try:
        return parse(value, *args)
    except Invalid:
        raise ValidationError(f"Invalid {label}") from None


def wire(parse: Any, value: Any, label: str, *args: Any) -> Any:
    try:
        return parse(value, *args)
    except Invalid:
        raise ProtocolError(f"Invalid {label} response") from None


def assert_scope(found: bool, label: str) -> None:
    if not found:
        raise ProtocolError(f"{label} identity or scope mismatch")


def validate_channel(channel_id: Any, org_id: str) -> None:
    validate_input(identity, channel_id, "channelId", 1024)
    if not channel_id.startswith(f"{org_id}:") or len(channel_id) == len(org_id) + 1:
        raise ValidationError("channelId must be a canonical routing key in the configured organization", channel_id=channel_id)


# --- message state ------------------------------------------------------------------------------


def _attachment(value: Any) -> MessageAttachment:
    v = obj(value)
    name = nullable(required(v, "file_name"), bounded_text, 1024)
    if name is not None:
        check(utf16_len(name) >= 1)
    media = nullable(required(v, "media_type"), bounded_text, 256)
    if media is not None:
        check(utf16_len(media) >= 1)
    return MessageAttachment(
        ordinal=safe_int(required(v, "ordinal"), 0, 63),
        upload_id=nullable_identity(required(v, "upload_id")),
        content_hash=nullable(required(v, "content_hash"), digest),
        file_name=name,
        media_type=media,
        byte_length=nullable(required(v, "byte_len"), safe_int, 0),
        attachment_variants=nullable(required(v, "attachment_variants"), bounded_text, 65_536),
    )


def state_schema(value: Any) -> MessageState:
    v = obj(value)
    literal(required(v, "schema"), 1)
    reactions_raw = obj(required(v, "reactions"))
    reactions: dict[str, list[str]] = {}
    for key, actors in reactions_raw.items():
        emoji(key)
        array(actors, 16_384, 1)
        reactions[key] = [identity(actor, 256) for actor in actors]
    state = MessageState(
        schema=1,
        org_id=identity(required(v, "org_uuid")),
        channel_id=identity(required(v, "channel_id"), 1024),
        id=identity(required(v, "message_id")),
        sender_id=identity(required(v, "sender_id"), 256),
        created_at=timestamp(required(v, "created_at")),
        thread_root_id=nullable_identity(required(v, "thread_root_id")),
        priority=priority(required(v, "priority")),  # type: ignore[arg-type]
        original_body=bounded_text(required(v, "original_body"), 1_048_576),
        body=bounded_text(required(v, "body"), 1_048_576),
        revision=revision(required(v, "revision")),
        updated_at=timestamp(required(v, "updated_at")),
        deleted=boolean(required(v, "deleted")),
        reaction_revision=safe_int(required(v, "reaction_revision"), 0),
        reactions=reactions,
        attachments=[_attachment(item) for item in array(required(v, "attachments"), 64)],
    )
    check(not (state.thread_root_id == state.id or (state.revision == 1 and (state.deleted or state.body != state.original_body))))
    groups = list(state.reactions.values())
    check(len(groups) <= 256 and not (state.reaction_revision == 0 and len(groups) > 0))
    check(sum(len(actors) for actors in groups) <= 16_384)
    for actors in groups:
        for index in range(1, len(actors)):
            check(_utf16_less(actors[index - 1], actors[index]))
    for index, attachment in enumerate(state.attachments):
        check(attachment.ordinal == index and not _CONTROL.search(f"{attachment.file_name or ''}{attachment.media_type or ''}"))
    return state


def _utf16_key(value: str) -> bytes:
    return value.encode("utf-16-be", "surrogatepass")


def _utf16_less(a: str, b: str) -> bool:
    """JavaScript string ``<`` (UTF-16 code-unit order)."""
    return _utf16_key(a) < _utf16_key(b)


def js_less(a: str, b: str) -> bool:
    return _utf16_less(a, b)


def parse_state(value: Any, org_id: str, channel_id: str, message_id: str | None = None) -> MessageState:
    state: MessageState = wire(state_schema, value, "message state")
    assert_scope(state.org_id == org_id and state.channel_id == channel_id and (message_id is None or state.id == message_id), "Message state")
    return state


def parse_send_receipt(value: Any, channel_id: str, message_id: str, idempotency_key: str) -> SendReceipt:
    def schema(raw: Any) -> dict[str, Any]:
        v = obj(raw)
        literal(required(v, "status"), "ACK")
        return {
            "message_id": identity(required(v, "message_id")),
            "created_at": timestamp(required(v, "created_at")),
            "attachment_count": safe_int(required(v, "attachment_count"), 0, 64),
            "attachment_manifest_blake3": digest(required(v, "attachment_manifest_blake3")),
        }

    v = wire(schema, value, "send receipt")
    assert_scope(v["message_id"] == message_id and v["attachment_count"] == 0, "Send receipt")
    return SendReceipt(
        id=v["message_id"],
        channel_id=channel_id,
        status="accepted",
        created_at=v["created_at"],
        idempotency_key=idempotency_key,
        attachment_count=v["attachment_count"],
        attachment_manifest_blake3=v["attachment_manifest_blake3"],
    )


def parse_mutation(value: Any, org_id: str, channel_id: str, message_id: str, operation_id: str, actor_id: str) -> MutationReceipt:
    def schema(raw: Any) -> dict[str, Any]:
        v = obj(raw)
        literal(required(v, "schema"), 1)
        state_schema(required(v, "state"))
        return {
            "operation_id": identity(required(v, "operation_id")),
            "actor_id": identity(required(v, "actor_id"), 256),
            "request_digest": digest(required(v, "request_digest")),
            "state": v["state"],
        }

    v = wire(schema, value, "mutation receipt")
    assert_scope(v["operation_id"] == operation_id and v["actor_id"] == actor_id, "Mutation receipt")
    return MutationReceipt(
        schema=1,
        operation_id=v["operation_id"],
        actor_id=v["actor_id"],
        request_digest=v["request_digest"],
        state=parse_state(v["state"], org_id, channel_id, message_id),
    )


def _cursor_schema(raw: Any) -> dict[str, Any]:
    v = obj(raw)
    literal(required(v, "schema"), 2)
    return {
        "org_uuid": identity(required(v, "org_uuid")),
        "channel_id": identity(required(v, "channel_id"), 1024),
        "thread_root_id": nullable_identity(required(v, "thread_root_id")),
        "created_at": timestamp(required(v, "created_at")),
        "message_id": identity(required(v, "message_id")),
    }


def parse_cursor(value: Any, org_id: str, channel_id: str, thread_root_id: str | None, is_input: bool = False) -> dict[str, Any]:
    try:
        encoded = wire(cursor_wire, value, "history cursor")
        decoded = json_loads(bytes.fromhex(encoded))
        cursor: dict[str, Any] = wire(_cursor_schema, decoded, "history cursor")
        assert_scope(
            cursor["org_uuid"] == org_id and cursor["channel_id"] == channel_id and cursor["thread_root_id"] == thread_root_id,
            "History cursor",
        )
        return cursor
    except Exception:
        if is_input:
            raise ValidationError("Invalid history cursor or cursor scope", channel_id=channel_id) from None
        raise ProtocolError("Invalid history cursor or cursor scope", channel_id=channel_id) from None


def before(a_created: int, a_id: str, b_created: int, b_id: str) -> bool:
    return a_created < b_created or (a_created == b_created and _utf16_less(a_id, b_id))


def parse_history(value: Any, org_id: str, channel_id: str, thread_root_id: str | None, request_cursor: str | None = None) -> HistoryPage:
    def schema(raw: Any) -> dict[str, Any]:
        v = obj(raw)
        literal(required(v, "schema"), 2)
        messages = array(required(v, "messages"), 200)
        for item in messages:
            state_schema(item)
        return {
            "org_uuid": identity(required(v, "org_uuid")),
            "channel_id": identity(required(v, "channel_id"), 1024),
            "thread_root_id": nullable_identity(required(v, "thread_root_id")),
            "messages": messages,
            "next_cursor": nullable(required(v, "next_cursor"), cursor_wire),
        }

    v = wire(schema, value, "history")
    assert_scope(v["org_uuid"] == org_id and v["channel_id"] == channel_id and v["thread_root_id"] == thread_root_id, "History")
    messages = [parse_state(item, org_id, channel_id) for item in v["messages"]]
    ids: set[str] = set()
    for index, current in enumerate(messages):
        assert_scope(current.thread_root_id == thread_root_id and current.id not in ids, "History message")
        ids.add(current.id)
        if index > 0:
            previous = messages[index - 1]
            if not before(previous.created_at, previous.id, current.created_at, current.id):
                raise ProtocolError("History page is not strictly ascending", channel_id=channel_id)
    if request_cursor and messages:
        bound = parse_cursor(request_cursor, org_id, channel_id, thread_root_id)
        last = messages[-1]
        assert_scope(before(last.created_at, last.id, bound["created_at"], bound["message_id"]), "History continuation")
    if v["next_cursor"] is not None:
        cursor = parse_cursor(v["next_cursor"], org_id, channel_id, thread_root_id)
        oldest = messages[0] if messages else None
        assert_scope(oldest is not None and cursor["created_at"] == oldest.created_at and cursor["message_id"] == oldest.id, "History boundary")
    return HistoryPage(schema=2, org_id=org_id, channel_id=channel_id, thread_root_id=thread_root_id, messages=messages, next_cursor=v["next_cursor"])


def parse_channels(value: Any, org_id: str, team_id: str | None = None) -> ChannelList:
    def schema(raw: Any) -> dict[str, Any]:
        v = obj(raw)
        channels = []
        for item in array(required(v, "channels")):
            c = obj(item)
            channels.append(
                {
                    "channel_id": identity(required(c, "channel_id"), 1024),
                    "org_uuid": identity(required(c, "org_uuid")),
                    "team_uuid": identity(required(c, "team_uuid")),
                    "slug": string(required(c, "slug"), 1),
                    "display_name": string(required(c, "display_name")),
                    "is_private": boolean(required(c, "is_private")),
                }
            )
        return {
            "org_uuid": identity(required(v, "org_uuid")),
            "team_uuid": identity(required(v, "team_uuid")),
            "team_name": string(required(v, "team_name")),
            "channels": channels,
        }

    v = wire(schema, value, "channels")
    assert_scope(v["org_uuid"] == org_id and (team_id is None or v["team_uuid"] == team_id), "Channel list")
    ids: set[str] = set()
    channels: list[Channel] = []
    for c in v["channels"]:
        assert_scope(
            c["org_uuid"] == org_id and c["team_uuid"] == v["team_uuid"] and c["channel_id"].startswith(f"{org_id}:") and c["channel_id"] not in ids,
            "Channel",
        )
        ids.add(c["channel_id"])
        channels.append(Channel(c["channel_id"], c["org_uuid"], c["team_uuid"], c["slug"], c["display_name"], c["is_private"]))
    return ChannelList(org_id=org_id, team_id=v["team_uuid"], team_name=v["team_name"], channels=channels)


def parse_conversations(value: Any, org_id: str) -> list[Conversation]:
    def schema(raw: Any) -> list[Conversation]:
        result = []
        for item in array(raw):
            c = obj(item)
            name = required(c, "name")
            result.append(
                Conversation(
                    id=identity(required(c, "id")),
                    channel_id=identity(required(c, "channel_key"), 1024),
                    name=None if name is None else string(name),
                    kind=string(required(c, "kind"), 1),
                    member_count=safe_int(required(c, "member_count"), 0),
                    my_role=string(required(c, "my_role")),
                    members=[
                        ConversationMember(actor_id=identity(required(obj(m), "user_uuid"), 256), role=string(required(obj(m), "role")))
                        for m in array(required(c, "members"))
                    ],
                    last_message_at=nullable(required(c, "last_message_at"), timestamp),
                )
            )
        return result

    conversations: list[Conversation] = wire(schema, value, "conversations")
    ids: set[str] = set()
    for conversation in conversations:
        assert_scope(conversation.channel_id == f"{org_id}:conv:{conversation.id}" and conversation.id not in ids, "Conversation")
        ids.add(conversation.id)
    return conversations


__all__ = ["MAX_SAFE_INTEGER"]
