from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._signal import AbortSignal
    from ._transport import HttpOptions
    from .types import Identity, SdkCapabilities


@dataclass
class ResourceContext:
    org_id: str
    request: Callable[[str, HttpOptions], Awaitable[Any]]
    connect: Callable[[AbortSignal | None, int | None], Awaitable[Identity]]
    capabilities: Callable[[AbortSignal | None, int | None], Awaitable[SdkCapabilities]]
    #: Internal: available only after an authenticated request succeeds.
    get_actor_id: Callable[[], str]
    total_timeout_ms: int
