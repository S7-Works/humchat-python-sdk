"""Store documents.

The on-disk JSON format is byte-compatible with ``@humchat/node``: field names stay
camelCase and record keys are the same SHA-256 scope hashes, so a store written by one
SDK can be opened by the other.
"""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any

from .._ulid import next_id
from .._util import MAX_SAFE_INTEGER, is_safe_int, json_dumps, parse_url
from .._wire import Invalid, body_text, check, identity, nullable_identity, priority, string, validate_channel
from ..errors import ConflictError, PersistenceError, ValidationError
from ..types import SendReceipt
from .types import OutboxEntry, OutboxIntent, PersistenceScope

_CURSOR_CONTROL = re.compile(r"[\x00-\x20\x7f]")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SCOPE_FIELDS = ("server_origin", "org_id", "actor_id", "consumer_id", "channel_id")
_WIRE_SCOPE = ("serverOrigin", "orgId", "actorId", "consumerId", "channelId")


def _scope_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, PersistenceScope):
        return {name: getattr(value, name) for name in _SCOPE_FIELDS}
    if isinstance(value, Mapping):
        return dict(value)
    raise Invalid


def _scope_fields(raw: Mapping[str, Any], names: tuple[str, ...]) -> tuple[str, str, str, str, str]:
    check(set(raw) == set(names))
    origin, org, actor, consumer, channel = (raw[name] for name in names)
    string(origin, 0, 2048)
    identity(org)
    identity(actor, 256)
    identity(consumer)
    identity(channel, 1024)
    return origin, org, actor, consumer, channel


def _normalize(origin: str, org: str, actor: str, consumer: str, channel: str) -> PersistenceScope:
    try:
        url = parse_url(origin)
    except ValueError:
        raise ValidationError("Invalid persistence server origin") from None
    if url.scheme not in ("http", "https") or url.username or url.password or url.pathname != "/" or url.search or url.hash:
        raise ValidationError("Persistence server origin must be an HTTP(S) origin")
    validate_channel(channel, org)
    return PersistenceScope(server_origin=url.origin, org_id=org, actor_id=actor, consumer_id=consumer, channel_id=channel)


def scope_value(value: Any) -> PersistenceScope:
    try:
        fields = _scope_fields(_scope_mapping(value), _SCOPE_FIELDS)
    except Invalid:
        raise ValidationError("Invalid persistence scope") from None
    return _normalize(*fields)


def scope_wire(scope: PersistenceScope) -> dict[str, str]:
    return {wire: getattr(scope, name) for wire, name in zip(_WIRE_SCOPE, _SCOPE_FIELDS, strict=True)}


def cursor_value(value: Any) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 16_384 or _CURSOR_CONTROL.search(value):
        raise ValidationError("Invalid checkpoint cursor")
    return value


def key_value(value: Any) -> str:
    try:
        return identity(value)
    except Invalid:
        raise ValidationError("Invalid outbox key") from None


def scope_key(scope: PersistenceScope, key: str | None = None) -> str:
    payload = json_dumps([scope.server_origin, scope.org_id, scope.actor_id, scope.consumer_id, scope.channel_id, key])
    return hashlib.sha256(payload.encode("utf-8", "surrogatepass")).hexdigest()


def empty_document(kind: str) -> dict[str, Any]:
    return {"schema": 1, "kind": kind, "records": {}}


# --- outbox ------------------------------------------------------------------------------------

_MESSAGE_FIELDS = {"channel_id", "content", "id", "thread_root_id", "priority"}


def normalized_request(scope: PersistenceScope, message: Any, idempotency_key: Any = None) -> dict[str, Any]:
    try:
        check(isinstance(message, Mapping) and set(message) <= _MESSAGE_FIELDS)
        channel = identity(message.get("channel_id"), 1024)
        content = body_text(message.get("content"))
        explicit_id = message.get("id")
        if explicit_id is not None:
            identity(explicit_id)
        thread = nullable_identity(message.get("thread_root_id"))
        level = message.get("priority")
        if level is not None:
            priority(level)
        if idempotency_key is not None:
            identity(idempotency_key)
    except Invalid:
        raise ValidationError("Invalid outbox send intent") from None
    if channel != scope.channel_id:
        raise ValidationError("Outbox intent channel does not match its scope")
    return {
        "channelId": channel,
        "content": content,
        "id": explicit_id,
        "threadRootId": thread,
        "priority": level or "standard",
        "idempotencyKey": idempotency_key,
    }


def receipt_wire(receipt: Any) -> dict[str, Any]:
    if is_dataclass(receipt) and not isinstance(receipt, type):
        raw = asdict(receipt)
        return {
            "id": raw.get("id"),
            "channelId": raw.get("channel_id"),
            "status": raw.get("status"),
            "createdAt": raw.get("created_at"),
            "idempotencyKey": raw.get("idempotency_key"),
            "attachmentCount": raw.get("attachment_count"),
            "attachmentManifestBlake3": raw.get("attachment_manifest_blake3"),
        }
    if isinstance(receipt, Mapping):
        return dict(receipt)
    raise ValidationError("Invalid outbox send receipt")


def _receipt_schema(raw: Any) -> dict[str, Any]:
    v = dict(raw) if isinstance(raw, Mapping) else None
    check(v is not None)
    assert v is not None
    check(set(v) == {"id", "channelId", "status", "createdAt", "idempotencyKey", "attachmentCount", "attachmentManifestBlake3"})
    identity(v["id"])
    identity(v["channelId"], 1024)
    check(v["status"] == "accepted")
    for name in ("createdAt", "attachmentCount"):
        check(is_safe_int(v[name]) and 0 <= v[name] <= MAX_SAFE_INTEGER)
    identity(v["idempotencyKey"])
    check(isinstance(v["attachmentManifestBlake3"], str) and _DIGEST.fullmatch(v["attachmentManifestBlake3"]) is not None)
    return {**v, "createdAt": int(v["createdAt"]), "attachmentCount": int(v["attachmentCount"])}


def receipt_from_wire(raw: Mapping[str, Any]) -> SendReceipt:
    return SendReceipt(
        id=raw["id"],
        channel_id=raw["channelId"],
        status="accepted",
        created_at=raw["createdAt"],
        idempotency_key=raw["idempotencyKey"],
        attachment_count=raw["attachmentCount"],
        attachment_manifest_blake3=raw["attachmentManifestBlake3"],
    )


def entry_from_wire(raw: Mapping[str, Any]) -> OutboxEntry:
    intent = raw["intent"]
    return OutboxEntry(
        key=raw["key"],
        intent=OutboxIntent(
            id=intent["id"],
            channel_id=intent["channelId"],
            content=intent["content"],
            thread_root_id=intent["threadRootId"],
            priority=intent["priority"],
        ),
        idempotency_key=raw["idempotencyKey"],
        status=raw["status"],
        receipt=receipt_from_wire(raw["receipt"]) if "receipt" in raw else None,
    )


def prepare_entry(document: dict[str, Any], scope: PersistenceScope, key: str, request: dict[str, Any]) -> OutboxEntry:
    digest = scope_key(scope, key)
    previous = document["records"].get(digest)
    if previous is not None:
        if json_dumps(previous["request"]) != json_dumps(request):
            raise ConflictError("Outbox key is already bound to another send intent")
        return entry_from_wire(copy.deepcopy(previous["entry"]))
    message_id = request["id"] if request["id"] is not None else next_id()
    entry = {
        "key": key,
        "intent": {
            "id": message_id,
            "channelId": request["channelId"],
            "content": request["content"],
            "threadRootId": request["threadRootId"],
            "priority": request["priority"],
        },
        "idempotencyKey": request["idempotencyKey"] if request["idempotencyKey"] is not None else message_id,
        "status": "pending",
    }
    document["records"][digest] = {"scope": scope_wire(scope), "request": request, "entry": entry}
    return entry_from_wire(copy.deepcopy(entry))


def _validate_receipt(entry: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
    if receipt["id"] != entry["intent"]["id"] or receipt["channelId"] != entry["intent"]["channelId"] or receipt["idempotencyKey"] != entry["idempotencyKey"]:
        raise ConflictError("Outbox receipt does not match its send intent")


def mark_entry_sent(document: dict[str, Any], scope: PersistenceScope, key: str, receipt: Any) -> OutboxEntry:
    try:
        parsed = _receipt_schema(receipt_wire(receipt))
    except Invalid:
        raise ValidationError("Invalid outbox send receipt") from None
    record = document["records"].get(scope_key(scope, key))
    if record is None:
        raise ConflictError("Outbox intent does not exist")
    entry = record["entry"]
    _validate_receipt(entry, parsed)
    if entry["status"] == "sent" and json_dumps(entry.get("receipt")) != json_dumps(parsed):
        raise ConflictError("Outbox receipt conflicts with the recorded send")
    entry["status"] = "sent"
    entry["receipt"] = parsed
    return entry_from_wire(copy.deepcopy(entry))


def pending_entries(document: dict[str, Any], scope: PersistenceScope) -> list[OutboxEntry]:
    wanted = json_dumps(scope_wire(scope))
    return [
        entry_from_wire(copy.deepcopy(record["entry"]))
        for record in document["records"].values()
        if json_dumps(record["scope"]) == wanted and record["entry"]["status"] == "pending"
    ]


# --- document validation ----------------------------------------------------------------------


def _intent_schema(v: Any) -> None:
    check(isinstance(v, Mapping) and set(v) == {"channelId", "content", "id", "threadRootId", "priority"})
    identity(v["channelId"], 1024)
    body_text(v["content"])
    identity(v["id"])
    nullable_identity(v["threadRootId"])
    priority(v["priority"])


def _request_schema(v: Any) -> None:
    check(isinstance(v, Mapping) and set(v) == {"channelId", "content", "id", "threadRootId", "priority", "idempotencyKey"})
    identity(v["channelId"], 1024)
    body_text(v["content"])
    nullable_identity(v["id"])
    nullable_identity(v["threadRootId"])
    priority(v["priority"])
    nullable_identity(v["idempotencyKey"])


def _entry_schema(v: Any) -> None:
    check(isinstance(v, Mapping) and {"key", "intent", "idempotencyKey", "status"} <= set(v) <= {"key", "intent", "idempotencyKey", "status", "receipt"})
    identity(v["key"])
    _intent_schema(v["intent"])
    identity(v["idempotencyKey"])
    check(v["status"] in ("pending", "sent"))
    if "receipt" in v:
        _receipt_schema(v["receipt"])


def _canonical_scope(raw: Any) -> PersistenceScope:
    check(isinstance(raw, Mapping))
    fields = _scope_fields(raw, _WIRE_SCOPE)
    scope = _normalize(*fields)
    check(json_dumps(scope_wire(scope)) == json_dumps(dict(raw)))
    return scope


def parse_document(value: Any, kind: str) -> dict[str, Any]:
    try:
        check(isinstance(value, dict) and set(value) == {"schema", "kind", "records"})
        check(value["schema"] == 1 and not isinstance(value["schema"], bool) and value["kind"] in ("checkpoints", "outbox"))
        check(value["kind"] == kind and isinstance(value["records"], dict))
        for digest, record in value["records"].items():
            check(isinstance(record, dict))
            if kind == "checkpoints":
                check(set(record) == {"scope", "cursor"})
                scope = _canonical_scope(record["scope"])
                cursor = record["cursor"]
                check(isinstance(cursor, str) and 1 <= len(cursor) <= 16_384 and not _CURSOR_CONTROL.search(cursor))
                check(digest == scope_key(scope))
            else:
                check(set(record) == {"scope", "request", "entry"})
                scope = _canonical_scope(record["scope"])
                request, entry = record["request"], record["entry"]
                _request_schema(request)
                _entry_schema(entry)
                intent = entry["intent"]
                check(
                    digest == scope_key(scope, entry["key"])
                    and request["channelId"] == scope.channel_id
                    and intent["channelId"] == scope.channel_id
                    and intent["content"] == request["content"]
                    and intent["threadRootId"] == request["threadRootId"]
                    and intent["priority"] == request["priority"]
                    and (request["id"] is None or request["id"] == intent["id"])
                    and (request["idempotencyKey"] is None or request["idempotencyKey"] == entry["idempotencyKey"])
                    and (request["idempotencyKey"] is not None or entry["idempotencyKey"] == intent["id"])
                    and (entry["status"] == "sent") == ("receipt" in entry)
                )
                if "receipt" in entry:
                    _validate_receipt(entry, entry["receipt"])
        return value
    except Exception:
        raise PersistenceError("Persistence file is invalid or belongs to another store format") from None
