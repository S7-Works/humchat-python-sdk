from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

from .._context import ResourceContext
from .._signal import AbortSignal, budget
from .._transport import HttpOptions
from .._ulid import next_id
from .._util import MAX_SAFE_INTEGER, is_int
from .._wire import (
    Invalid,
    assert_scope,
    body_text,
    check,
    cursor_wire,
    emoji,
    identity,
    nullable_identity,
    parse_cursor,
    parse_history,
    parse_mutation,
    parse_send_receipt,
    parse_state,
    priority,
    revision,
    validate_channel,
    validate_input,
)
from ..errors import ProtocolError, ValidationError, with_intent
from ..types import DirectSendReceipt, HistoryPage, MessagePriority, MessageState, MutationReceipt, SendReceipt

if TYPE_CHECKING:
    from .conversations import ConversationsResource

_UNSET: Any = object()


def _validate(label: str, *checks: Any) -> None:
    try:
        for parse, value, *args in checks:
            parse(value, *args)
    except Invalid:
        raise ValidationError(f"Invalid {label}") from None


def _optional(parse: Any, value: Any, *args: Any) -> Any:
    return None if value is None else parse(value, *args)


def _expected_revision(value: Any) -> int:
    revision(value)
    check(value <= MAX_SAFE_INTEGER - 1)
    return int(value)


def _bool(value: Any) -> bool:
    check(isinstance(value, bool))
    return value


def _limit(value: Any) -> int:
    check(is_int(value) and 1 <= value <= 200)
    return int(value)


def _reference(message: Any) -> tuple[str, str, str | None]:
    """A full ``MessageState`` or ``Message`` is a valid reference, as is a mapping."""
    if isinstance(message, Mapping):
        message_id, channel_id, thread = message.get("id"), message.get("channel_id"), message.get("thread_root_id")
    else:
        message_id = getattr(message, "id", None)
        channel_id = getattr(message, "channel_id", None)
        thread = getattr(message, "thread_root_id", None)
    return identity(message_id), identity(channel_id, 1024), nullable_identity(thread)


class MessagesResource:
    def __init__(self, context: ResourceContext, conversations: ConversationsResource) -> None:
        self._context = context
        self._conversations = conversations

    async def send_direct(
        self,
        *,
        recipient_id: str,
        content: str,
        id: str | None = None,  # noqa: A002 - mirrors the wire/Node field name
        thread_root_id: str | None = None,
        priority: MessagePriority | None = None,
        idempotency_key: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> DirectSendReceipt:
        """Open (or reuse) the 1:1 conversation with ``recipient_id`` and send into it."""
        _validate(
            "direct send input",
            (identity, recipient_id, 256),
            (body_text, content),
            (_optional, identity, id),
            (_optional, identity, thread_root_id),
            (_optional, _priority, priority),
        )
        validate_input(identity, idempotency_key if idempotency_key is not None else id if id is not None else "generated", "idempotencyKey")
        operation = budget(self._context.total_timeout_ms, [signal])
        try:
            conversation = await self._conversations.open_direct(member_id=recipient_id, signal=operation.signal, timeout_ms=timeout_ms)
            receipt = await self.send(
                channel_id=conversation.channel_id,
                content=content,
                id=id,
                thread_root_id=thread_root_id,
                priority=priority,
                idempotency_key=idempotency_key,
                signal=operation.signal,
                timeout_ms=timeout_ms,
            )
            return DirectSendReceipt(conversation=conversation, receipt=receipt)
        finally:
            operation.dispose()

    async def send(
        self,
        *,
        channel_id: str,
        content: str,
        id: str | None = None,  # noqa: A002
        thread_root_id: str | None = None,
        priority: MessagePriority | None = None,
        idempotency_key: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> SendReceipt:
        """Send once; retries reuse the same message ID and idempotency key."""
        _validate(
            "send input",
            (identity, channel_id, 1024),
            (body_text, content),
            (_optional, identity, id),
            (_optional, identity, thread_root_id),
            (_optional, _priority, priority),
        )
        validate_channel(channel_id, self._context.org_id)
        message_id = id if id is not None else next_id()
        key = validate_input(identity, idempotency_key if idempotency_key is not None else message_id, "idempotencyKey")
        intent = {"channel_id": channel_id, "message_id": message_id, "idempotency_key": key}
        body = {
            "id": message_id,
            "channel_id": channel_id,
            "content": content,
            "thread_root_id": thread_root_id,
            "priority": priority or "standard",
        }
        try:
            response = await self._context.request(
                "/api/messages/send",
                HttpOptions(
                    signal=signal,
                    timeout_ms=timeout_ms,
                    method="POST",
                    body=body,
                    replay_safe=True,
                    headers={"X-Idempotency-Key": key, "X-Zlack-Message-Receipt": "v1"},
                    **intent,
                ),
            )
            return parse_send_receipt(response, channel_id, message_id, key)
        except Exception as error:
            raise with_intent(error, **intent) from None

    async def reply(
        self,
        message: Any,
        *,
        content: str,
        id: str | None = None,  # noqa: A002
        priority: MessagePriority | None = None,
        idempotency_key: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> SendReceipt:
        """Reply in ``message``'s thread (or start one rooted at ``message``)."""
        try:
            message_id, channel_id, thread = _reference(message)
        except Invalid:
            raise ValidationError("Invalid reply target") from None
        _validate("reply input", (body_text, content), (_optional, identity, id), (_optional, _priority, priority))
        return await self.send(
            channel_id=channel_id,
            content=content,
            thread_root_id=thread if thread is not None else message_id,
            id=id,
            priority=priority,
            idempotency_key=idempotency_key,
            signal=signal,
            timeout_ms=timeout_ms,
        )

    async def get_state(
        self, *, channel_id: str, message_id: str, signal: AbortSignal | None = None, timeout_ms: int | None = None
    ) -> MessageState:
        _validate("message locator", (identity, channel_id, 1024), (identity, message_id))
        validate_channel(channel_id, self._context.org_id)
        response = await self._context.request(
            f"/api/messages/{quote(message_id, safe='')}/state",
            HttpOptions(signal=signal, timeout_ms=timeout_ms, channel_id=channel_id, message_id=message_id),
        )
        return parse_state(response, self._context.org_id, channel_id, message_id)

    async def history(
        self,
        *,
        channel_id: str,
        thread_root_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> HistoryPage:
        """One page, newest first by page; each page's messages are in ascending order."""
        _validate(
            "history input",
            (identity, channel_id, 1024),
            (_optional, identity, thread_root_id),
            (_optional, cursor_wire, cursor),
            (_optional, _limit, limit),
        )
        validate_channel(channel_id, self._context.org_id)
        if cursor is not None:
            parse_cursor(cursor, self._context.org_id, channel_id, thread_root_id, True)
        query: dict[str, str] = {"channel_id": channel_id}
        if thread_root_id is not None:
            query["thread_root_id"] = thread_root_id
        if cursor is not None:
            query["native_cursor"] = cursor
        if limit is not None:
            query["limit"] = str(int(limit))
        response = await self._context.request(
            f"/api/messages/sync?{urlencode(query)}",
            HttpOptions(signal=signal, timeout_ms=timeout_ms, channel_id=channel_id, headers={"X-Zlack-Message-State": "v2"}),
        )
        page = parse_history(response, self._context.org_id, channel_id, thread_root_id, cursor)
        if limit is not None and len(page.messages) > limit:
            raise ProtocolError("History exceeded requested limit", channel_id=channel_id)
        return page

    async def iterate_history(
        self,
        *,
        channel_id: str,
        thread_root_id: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncIterator[HistoryPage]:
        """Yield successively older pages; each page's messages are in ascending order."""
        seen: set[str] = set()
        while True:
            if cursor is not None:
                if cursor in seen:
                    raise ProtocolError("History cursor repeated", channel_id=channel_id)
                seen.add(cursor)
            page = await self.history(
                channel_id=channel_id, thread_root_id=thread_root_id, cursor=cursor, limit=limit, signal=signal, timeout_ms=timeout_ms
            )
            yield page
            if page.next_cursor is None:
                return
            cursor = page.next_cursor

    async def edit(
        self,
        *,
        channel_id: str,
        message_id: str,
        content: str,
        expected_revision: int,
        operation_id: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> MutationReceipt:
        _validate(
            "edit input",
            (identity, channel_id, 1024),
            (identity, message_id),
            (_optional, identity, operation_id),
            (body_text, content),
            (_expected_revision, expected_revision),
        )
        mutation = {"kind": "edit", "body": content, "expected_revision": int(expected_revision)}
        return await self._mutate(channel_id, message_id, operation_id, mutation, signal, timeout_ms)

    async def delete(
        self,
        *,
        channel_id: str,
        message_id: str,
        expected_revision: int,
        operation_id: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> MutationReceipt:
        _validate(
            "delete input",
            (identity, channel_id, 1024),
            (identity, message_id),
            (_optional, identity, operation_id),
            (_expected_revision, expected_revision),
        )
        mutation = {"kind": "delete", "expected_revision": int(expected_revision)}
        return await self._mutate(channel_id, message_id, operation_id, mutation, signal, timeout_ms)

    async def set_reaction(
        self,
        *,
        channel_id: str,
        message_id: str,
        emoji: str,
        present: bool,
        operation_id: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> MutationReceipt:
        _validate(
            "reaction input",
            (identity, channel_id, 1024),
            (identity, message_id),
            (_optional, identity, operation_id),
            (_emoji, emoji),
            (_bool, present),
        )
        mutation = {"kind": "set_reaction", "emoji": emoji, "present": present}
        return await self._mutate(channel_id, message_id, operation_id, mutation, signal, timeout_ms)

    async def _mutate(
        self,
        channel_id: str,
        message_id: str,
        operation_id: str | None,
        mutation: dict[str, Any],
        signal: AbortSignal | None,
        timeout_ms: int | None,
    ) -> MutationReceipt:
        validate_channel(channel_id, self._context.org_id)
        operation = operation_id if operation_id is not None else next_id()
        intent = {"channel_id": channel_id, "message_id": message_id, "operation_id": operation}
        try:
            response = await self._context.request(
                "/api/messages/mutate",
                HttpOptions(
                    signal=signal,
                    timeout_ms=timeout_ms,
                    method="POST",
                    replay_safe=True,
                    body={"operation_id": operation, "channel_id": channel_id, "message_id": message_id, "mutation": mutation},
                    channel_id=channel_id,
                    message_id=message_id,
                    operation_id=operation,
                ),
            )
            actor_id = self._context.get_actor_id()
            receipt = parse_mutation(response, self._context.org_id, channel_id, message_id, operation, actor_id)
            state = receipt.state
            if mutation["kind"] == "edit":
                assert_scope(
                    state.sender_id == actor_id
                    and not state.deleted
                    and state.body == mutation["body"]
                    and state.revision in (mutation["expected_revision"], mutation["expected_revision"] + 1),
                    "Edit result",
                )
            elif mutation["kind"] == "delete":
                assert_scope(state.sender_id == actor_id and state.deleted and state.revision == mutation["expected_revision"] + 1, "Delete result")
            else:
                assert_scope(not state.deleted and (actor_id in state.reactions.get(mutation["emoji"], [])) == mutation["present"], "Reaction result")
            return receipt
        except Exception as error:
            raise with_intent(error, **intent) from None


def _priority(value: Any) -> str:
    return priority(value)


def _emoji(value: Any) -> str:
    return emoji(value)
