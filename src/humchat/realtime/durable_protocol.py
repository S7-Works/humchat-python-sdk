"""Durable event protocol v1: cursors, events, ready watermarks, pages and gaps."""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .._util import json_loads
from .._wire import Invalid, array, check, identity, literal, obj, parse_state, required, string
from ..errors import ProtocolError, RecoveryGapError
from ..types import DurableEvent, EventPage

_SEQUENCE = re.compile(r"^(0|[1-9][0-9]{0,18})$")
_CURSOR = re.compile(r"^[A-Za-z0-9_-]+$")
_MAX_I64 = 9_223_372_036_854_775_807
_KINDS = ("message", "edit", "delete", "reaction")


@dataclass(frozen=True)
class CursorPosition:
    epoch: str
    sequence: int


@dataclass(frozen=True)
class ReadyFrame:
    cursor: str
    earliest_cursor: str
    epoch: str
    sequence: str


def sequence(value: Any) -> str:
    check(isinstance(value, str) and _SEQUENCE.fullmatch(value) is not None and int(value) <= _MAX_I64)
    return value


def cursor_text(value: Any) -> str:
    check(isinstance(value, str) and 1 <= len(value) <= 8192 and _CURSOR.fullmatch(value) is not None)
    return value


def _read(parse: Any, value: Any) -> Any:
    try:
        return parse(value)
    except Invalid:
        raise ProtocolError("Invalid durable event protocol") from None


def _scope(v: Mapping[str, Any]) -> dict[str, Any]:
    literal(required(v, "schema"), 1)
    return {
        "org_uuid": identity(required(v, "org_uuid")),
        "channel_id": identity(required(v, "channel_id"), 1024),
        "epoch": identity(required(v, "epoch")),
    }


def _bound(value: Mapping[str, Any], org: str, channel: str) -> None:
    if value["org_uuid"] != org or value["channel_id"] != channel:
        raise ProtocolError("Durable event scope mismatch")


def _b64url_decode(text: str) -> bytes:
    padded = text + "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def cursor_position(cursor: Any, org: str, channel: str) -> CursorPosition:
    _read(cursor_text, cursor)
    try:
        decoded = _b64url_decode(cursor)
        if b64url_encode(decoded) != cursor:
            raise ValueError("non-canonical")
        raw = json_loads(decoded)
    except (ValueError, UnicodeDecodeError, binascii.Error, RecursionError):
        raise ProtocolError("Invalid durable cursor encoding") from None

    def schema(value: Any) -> dict[str, Any]:
        v = obj(value)
        result = _scope(v)
        result["sequence"] = sequence(required(v, "sequence"))
        return result

    value = _read(schema, raw)
    _bound(value, org, channel)
    return CursorPosition(epoch=value["epoch"], sequence=int(value["sequence"]))


def same_stream(a: CursorPosition, b: CursorPosition) -> None:
    if a.epoch != b.epoch:
        raise RecoveryGapError("Event stream epoch changed; explicit recovery is required")


def parse_event(raw: Any, org: str, channel: str) -> DurableEvent:
    def schema(value: Any) -> dict[str, Any]:
        v = obj(value)
        result = _scope(v)
        literal(required(v, "type"), "event")
        kind = required(v, "kind")
        check(isinstance(kind, str) and kind in _KINDS)
        check("state" in v)
        result.update(
            event_id=string(required(v, "event_id"), 0, 160),
            actor_id=identity(required(v, "actor_id"), 256),
            sequence=sequence(required(v, "sequence")),
            cursor=cursor_text(required(v, "cursor")),
            kind=kind,
            state=v["state"],
        )
        return result

    value = _read(schema, raw)
    _bound(value, org, channel)
    position = cursor_position(value["cursor"], org, channel)
    if (
        position.epoch != value["epoch"]
        or position.sequence != int(value["sequence"])
        or position.sequence == 0
        or value["event_id"] != f"{value['epoch']}:{value['sequence']}"
    ):
        raise ProtocolError("Durable event identity does not match its cursor")
    state = parse_state(value["state"], org, channel)
    kind = value["kind"]
    if (
        (kind == "message" and (state.revision != 1 or state.deleted or state.reaction_revision != 0))
        or (kind == "delete" and not state.deleted)
        or (kind == "edit" and state.deleted)
    ):
        raise ProtocolError("Durable event kind contradicts its state")
    return DurableEvent(
        schema=1,
        org_id=org,
        channel_id=channel,
        epoch=value["epoch"],
        event_id=value["event_id"],
        actor_id=value["actor_id"],
        sequence=value["sequence"],
        cursor=value["cursor"],
        kind=kind,
        state=state,
    )


def parse_ready(raw: Any, org: str, channel: str) -> ReadyFrame:
    def schema(value: Any) -> dict[str, Any]:
        v = obj(value)
        result = _scope(v)
        literal(required(v, "type"), "ready")
        result.update(
            cursor=cursor_text(required(v, "cursor")),
            sequence=sequence(required(v, "sequence")),
            earliest_cursor=cursor_text(required(v, "earliest_cursor")),
        )
        return result

    value = _read(schema, raw)
    _bound(value, org, channel)
    head = cursor_position(value["cursor"], org, channel)
    floor = cursor_position(value["earliest_cursor"], org, channel)
    if head.epoch != value["epoch"] or floor.epoch != head.epoch or head.sequence != int(value["sequence"]) or floor.sequence > head.sequence:
        raise ProtocolError("Invalid durable ready watermark")
    return ReadyFrame(cursor=value["cursor"], earliest_cursor=value["earliest_cursor"], epoch=value["epoch"], sequence=value["sequence"])


def parse_page(raw: Any, org: str, channel: str, after: str, through: str | None = None) -> EventPage:
    def schema(value: Any) -> dict[str, Any]:
        v = obj(value)
        result = _scope(v)
        next_cursor = required(v, "next_cursor")
        result.update(
            events=array(required(v, "events"), 200),
            after_cursor=cursor_text(required(v, "after_cursor")),
            through_cursor=cursor_text(required(v, "through_cursor")),
            through_sequence=sequence(required(v, "through_sequence")),
            next_cursor=None if next_cursor is None else cursor_text(next_cursor),
        )
        return result

    value = _read(schema, raw)
    _bound(value, org, channel)
    if value["after_cursor"] != after or (through is not None and value["through_cursor"] != through):
        raise ProtocolError("Event page does not match requested boundaries")
    position = cursor_position(after, org, channel)
    head = cursor_position(value["through_cursor"], org, channel)
    same_stream(position, head)
    if head.epoch != value["epoch"] or head.sequence != int(value["through_sequence"]) or position.sequence > head.sequence:
        raise ProtocolError("Invalid event page watermark")
    events = [parse_event(item, org, channel) for item in value["events"]]
    for event in events:
        number = int(event.sequence)
        if event.epoch != head.epoch or number != position.sequence + 1 or number > head.sequence:
            raise ProtocolError("Durable event page is not contiguous")
        position = CursorPosition(event.epoch, number)
    last = events[-1].cursor if events else after
    if (position.sequence == head.sequence and value["next_cursor"] is not None) or (
        position.sequence < head.sequence and (not events or value["next_cursor"] != last)
    ):
        raise ProtocolError("Durable page skips retained events")
    return EventPage(
        schema=1,
        org_id=org,
        channel_id=channel,
        epoch=value["epoch"],
        events=events,
        after_cursor=after,
        through_cursor=value["through_cursor"],
        through_sequence=value["through_sequence"],
        next_cursor=value["next_cursor"],
    )


def parse_gap(raw: Any, org: str, channel: str) -> RecoveryGapError:
    def schema(value: Any) -> dict[str, Any]:
        v = obj(value)
        literal(required(v, "code"), "EVENT_GAP")
        return {
            "org_uuid": identity(required(v, "org_uuid")),
            "channel_id": identity(required(v, "channel_id"), 1024),
            "epoch": identity(required(v, "epoch")),
            "earliest_cursor": cursor_text(required(v, "earliest_cursor")),
            "earliest_sequence": sequence(required(v, "earliest_sequence")),
        }

    value = _read(schema, raw)
    _bound(value, org, channel)
    floor = cursor_position(value["earliest_cursor"], org, channel)
    if floor.epoch != value["epoch"] or floor.sequence != int(value["earliest_sequence"]):
        raise ProtocolError("Invalid recovery gap boundary")
    return RecoveryGapError(
        "Checkpoint is outside the retained event stream; explicit recovery is required",
        status=410,
        channel_id=channel,
        earliest_cursor=value["earliest_cursor"],
        earliest_sequence=value["earliest_sequence"],
    )
