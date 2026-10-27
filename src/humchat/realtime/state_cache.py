"""Per-channel LRU of authoritative message state and observed creations."""

from __future__ import annotations

import copy
import hashlib
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any

from .._util import is_safe_int, json_dumps, utf8_len
from ..errors import BackpressureError, ConfigurationError, ProtocolError
from ..types import Message, MessageState


@dataclass
class StateMergeResult:
    state: MessageState
    changed: bool
    content_changed: bool
    reactions_changed: bool


@dataclass
class _Creation:
    fingerprint: str
    identity: str
    content_hash: str
    is_long: bool
    attachments_hash: str
    body_digest: str | None = None


@dataclass
class _Entry:
    state: MessageState | None = None
    creation: _Creation | None = None
    bytes: int = 0


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "surrogatepass")).hexdigest()


def _identity(value: Message | MessageState) -> str:
    return json_dumps([value.org_id, value.channel_id, value.id, value.sender_id, value.created_at, value.thread_root_id, value.priority])


def _attachments(value: Message | MessageState) -> str:
    # Asset URLs expire and are not part of attachment identity.
    return json_dumps(
        [[a.ordinal, a.upload_id, a.content_hash, a.file_name, a.media_type, a.byte_length, a.attachment_variants] for a in value.attachments]
    )


def _immutable(value: MessageState) -> str:
    return json_dumps([_identity(value), value.original_body, _attachments(value)])


def _content(value: MessageState) -> str:
    return json_dumps([value.body, value.deleted, value.updated_at])


def _reactions(value: MessageState) -> str:
    items = list(value.reactions.items())
    # JavaScript default sort compares UTF-16 code units.
    ordered = sorted(items, key=lambda item: item[0].encode("utf-16-be", "surrogatepass"))
    return json_dumps(ordered)


def _creation(value: Message) -> _Creation:
    attachment_identity = _attachments(value)
    return _Creation(
        identity=_identity(value),
        fingerprint=_hash(json_dumps([_identity(value), value.content, value.is_long, value.body_digest, attachment_identity])),
        content_hash=_hash(value.content),
        is_long=value.is_long,
        attachments_hash=_hash(attachment_identity),
        body_digest=value.body_digest,
    )


def _code_points_prefix(value: str, count: int) -> str:
    """``Array.from(value).slice(0, count).join('')`` (Unicode scalar values)."""
    return value[:count]


def _compare_creation_to_state(created: _Creation, state: MessageState) -> None:
    # The server snippet is the first 900 Unicode scalar values plus an ellipsis.
    projection = _code_points_prefix(state.original_body, 900) + "…" if created.is_long else state.original_body
    if created.identity != _identity(state) or created.content_hash != _hash(projection) or created.attachments_hash != _hash(_attachments(state)):
        raise ProtocolError("Message creation conflicts with immutable message state", channel_id=state.channel_id, message_id=state.id)


class StateCache:
    """Eviction bounds memory and also bounds process-local duplicate detection."""

    def __init__(self, max_entries: int = 1000, max_bytes: int = 8 * 1024 * 1024) -> None:
        if not is_safe_int(max_entries) or max_entries < 1 or not is_safe_int(max_bytes) or max_bytes < 1:
            raise ConfigurationError("State cache limits must be positive safe integers")
        self._max_entries = int(max_entries)
        self._max_bytes = int(max_bytes)
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._bytes = 0

    @property
    def size(self) -> int:
        return len(self._entries)

    @property
    def bytes(self) -> int:
        return self._bytes

    def ids(self) -> list[str]:
        return list(self._entries.keys())

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0

    def get(self, message_id: str) -> MessageState | None:
        entry = self._entries.get(message_id)
        if entry is None:
            return None
        self._entries.move_to_end(message_id)
        return None if entry.state is None else copy.deepcopy(entry.state)

    def observe_message(self, message: Message) -> bool:
        """True only for a previously unobserved creation; snapshots at revision 1 do not consume it."""
        created = _creation(message)
        old = self._entries.get(message.id)
        if old is not None and old.creation is not None and old.creation.fingerprint != created.fingerprint:
            previous = old.creation
            # A recovery snapshot supplies full original text; a later live frame may carry its
            # snippet. Accept that representational difference only when cached authoritative
            # state proves both bodies, and attachments still match.
            if (
                old.state is None
                or previous.attachments_hash != created.attachments_hash
                or (previous.body_digest is not None and created.body_digest is not None and previous.body_digest != created.body_digest)
            ):
                raise ProtocolError("Conflicting duplicate message creation", channel_id=message.channel_id, message_id=message.id)
            _compare_creation_to_state(previous, old.state)
        if old is not None and old.state is not None:
            _compare_creation_to_state(created, old.state)
        # A recovery snapshot does not carry the live-frame digest. Keep any digest already
        # observed so later contradictory live frames still fail.
        if created.body_digest is None and old is not None and old.creation is not None and old.creation.body_digest is not None:
            created.body_digest = old.creation.body_digest
        is_new = (old is None or old.creation is None) and not (
            old is not None and old.state is not None and (old.state.deleted or old.state.revision > 1)
        )
        self._put(message.id, _Entry(state=old.state if old else None, creation=created), message.channel_id)
        return is_new

    def merge(self, incoming: MessageState) -> StateMergeResult:
        """Merge content and reactions independently; equal revisions cannot disagree."""
        old_entry = self._entries.get(incoming.id)
        if old_entry is not None and old_entry.creation is not None:
            _compare_creation_to_state(old_entry.creation, incoming)
        old = old_entry.state if old_entry is not None else None
        content_changed = old is None
        reactions_changed = old is None
        state = copy.deepcopy(incoming)
        if old is not None:
            if _immutable(old) != _immutable(incoming):
                raise ProtocolError("Immutable message identity changed", channel_id=incoming.channel_id, message_id=incoming.id)
            if old.revision == incoming.revision and _content(old) != _content(incoming):
                raise ProtocolError("Conflicting message content at the same revision", channel_id=incoming.channel_id, message_id=incoming.id)
            if old.reaction_revision == incoming.reaction_revision and _reactions(old) != _reactions(incoming):
                raise ProtocolError("Conflicting reactions at the same revision", channel_id=incoming.channel_id, message_id=incoming.id)
            content_changed = incoming.revision > old.revision
            reactions_changed = incoming.reaction_revision > old.reaction_revision
            if content_changed and incoming.updated_at < old.updated_at:
                raise ProtocolError("Content revision moved its timestamp backwards", channel_id=incoming.channel_id, message_id=incoming.id)
            if not content_changed:
                state = replace(state, body=old.body, deleted=old.deleted, revision=old.revision, updated_at=old.updated_at)
            if not reactions_changed:
                state = replace(state, reactions=copy.deepcopy(old.reactions), reaction_revision=old.reaction_revision)
        self._put(incoming.id, _Entry(state=state, creation=old_entry.creation if old_entry else None), incoming.channel_id)
        return StateMergeResult(
            state=copy.deepcopy(state),
            changed=content_changed or reactions_changed,
            content_changed=content_changed,
            reactions_changed=reactions_changed,
        )

    def _put(self, message_id: str, entry: _Entry, channel_id: str) -> None:
        entry.bytes = utf8_len(json_dumps([message_id, _state_json(entry.state), _creation_json(entry.creation)]))
        if entry.bytes > self._max_bytes:
            raise BackpressureError("One message exceeds the realtime state cache budget", channel_id=channel_id, message_id=message_id)
        old = self._entries.pop(message_id, None)
        if old is not None:
            self._bytes -= old.bytes
        while self._entries and (len(self._entries) >= self._max_entries or self._bytes + entry.bytes > self._max_bytes):
            _, oldest = self._entries.popitem(last=False)
            self._bytes -= oldest.bytes
        self._entries[message_id] = entry
        self._bytes += entry.bytes


def _state_json(state: MessageState | None) -> Any:
    if state is None:
        return None
    from dataclasses import asdict

    return asdict(state)


def _creation_json(creation: _Creation | None) -> Any:
    if creation is None:
        return None
    from dataclasses import asdict

    return asdict(creation)


__all__ = ["StateCache", "StateMergeResult"]
