from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from .._context import ResourceContext
from .._signal import AbortSignal, budget
from .._transport import HttpOptions
from .._util import is_int
from .._wire import Invalid, check, identity, string, validate_channel
from ..errors import UnsupportedFeatureError, ValidationError
from ..realtime.durable_protocol import cursor_position, parse_page
from ..types import EventPage


def _limit(value: Any) -> None:
    check(is_int(value) and 1 <= value <= 200)


class EventsResource:
    def __init__(self, context: ResourceContext) -> None:
        self._context = context

    async def sync(
        self,
        *,
        channel_id: str,
        after: str,
        through: str | None = None,
        limit: int | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> EventPage:
        """Read the committed durable event log after ``after`` (exclusive)."""
        try:
            identity(channel_id, 1024)
            string(after, 1, 8192)
            if through is not None:
                string(through, 1, 8192)
            if limit is not None:
                _limit(limit)
        except Invalid:
            raise ValidationError("Invalid event sync input") from None
        validate_channel(channel_id, self._context.org_id)
        cursor_position(after, self._context.org_id, channel_id)
        if through is not None:
            cursor_position(through, self._context.org_id, channel_id)
        lifetime = budget(self._context.total_timeout_ms, [signal])
        try:
            caps = await self._context.capabilities(lifetime.signal, timeout_ms)
            if not caps.messaging.durable_replay or 1 not in caps.messaging.event_protocol_versions:
                raise UnsupportedFeatureError("Server lacks durable event protocol v1")
            query = {"channel_id": channel_id, "after": after}
            if through is not None:
                query["through"] = through
            if limit is not None:
                query["limit"] = str(int(limit))
            raw = await self._context.request(
                f"/api/events/sync?{urlencode(query)}", HttpOptions(signal=lifetime.signal, timeout_ms=timeout_ms, channel_id=channel_id)
            )
            return parse_page(raw, self._context.org_id, channel_id, after, through)
        finally:
            lifetime.dispose()
