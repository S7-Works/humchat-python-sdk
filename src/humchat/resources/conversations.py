from __future__ import annotations

from collections.abc import Sequence

from .._context import ResourceContext
from .._signal import AbortSignal, budget
from .._transport import HttpOptions
from .._util import js_trim
from .._wire import Invalid, check, identity, parse_conversations, string
from ..errors import ProtocolError, ValidationError
from ..types import Conversation


class ConversationsResource:
    def __init__(self, context: ResourceContext) -> None:
        self._context = context

    async def list(self, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> list[Conversation]:
        raw = await self._context.request("/api/conversations", HttpOptions(signal=signal, timeout_ms=timeout_ms))
        return parse_conversations(raw, self._context.org_id)

    async def create(
        self,
        *,
        member_ids: Sequence[str],
        name: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> Conversation:
        """Creation is never automatically replayed: the server has no creation idempotency key."""
        try:
            check(isinstance(member_ids, (list, tuple)) and 1 <= len(member_ids) <= 1000)
            members = [identity(member, 256) for member in member_ids]
            if name is not None:
                string(name, 0, 4096)
        except Invalid:
            raise ValidationError("Invalid conversation create input") from None
        if len(set(members)) != len(members):
            raise ValidationError("Conversation members must be distinct")
        normalized = (js_trim(name) if name is not None else "") or None
        raw = await self._context.request(
            "/api/conversations",
            HttpOptions(
                signal=signal,
                timeout_ms=timeout_ms,
                method="POST",
                replay_safe=False,
                auth_retry=False,
                body={"member_uuids": members, "name": normalized},
            ),
        )
        conversation = parse_conversations([raw], self._context.org_id)[0]
        expected = {*members, self._context.get_actor_id()}
        actual = {member.actor_id for member in conversation.members}
        if (
            conversation.name != normalized
            or conversation.member_count != len(expected)
            or len(actual) != len(conversation.members)
            or actual != expected
        ):
            raise ProtocolError("Created conversation does not match the requested participants")
        return conversation

    async def open_direct(self, *, member_id: str, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> Conversation:
        """Find an unnamed exact two-person conversation, otherwise ask the server to create/reuse one."""
        try:
            identity(member_id, 256)
        except Invalid:
            raise ValidationError("Invalid direct conversation input") from None
        operation = budget(self._context.total_timeout_ms, [signal])
        try:
            me = await self._context.connect(operation.signal, timeout_ms)
            if member_id == me.actor_id:
                raise ValidationError("A direct conversation requires another organization member")
            conversations = await self.list(signal=operation.signal, timeout_ms=timeout_ms)
            for conversation in conversations:
                ids = [member.actor_id for member in conversation.members]
                if (
                    not js_trim(conversation.name or "")
                    and conversation.member_count == 2
                    and len(conversation.members) == 2
                    and len(set(ids)) == 2
                    and me.actor_id in ids
                    and member_id in ids
                ):
                    return conversation
            return await self.create(member_ids=[member_id], signal=operation.signal, timeout_ms=timeout_ms)
        finally:
            operation.dispose()
