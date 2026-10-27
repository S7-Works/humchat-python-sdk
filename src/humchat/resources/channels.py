from __future__ import annotations

from urllib.parse import urlencode

from .._context import ResourceContext
from .._signal import AbortSignal
from .._transport import HttpOptions
from .._wire import identity, parse_channels, validate_input
from ..types import ChannelList


class ChannelsResource:
    def __init__(self, context: ResourceContext) -> None:
        self._context = context

    async def list(self, *, team_id: str | None = None, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> ChannelList:
        """List workspace channels, optionally for one team."""
        if team_id is not None:
            validate_input(identity, team_id, "channel list input")
        query = "" if team_id is None else "?" + urlencode({"team_uuid": team_id})
        response = await self._context.request(f"/api/workspace/channels{query}", HttpOptions(signal=signal, timeout_ms=timeout_ms))
        return parse_channels(response, self._context.org_id, team_id)
