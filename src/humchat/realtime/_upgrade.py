from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._transport import parse_retry_after
from ..errors import (
    AuthenticationError,
    HumChatError,
    NotFoundError,
    PermissionError,
    ProtocolError,
    RateLimitError,
    ServerError,
    TimeoutError,
    UnsupportedFeatureError,
)


def upgrade_error(status: int, headers: Mapping[str, str], messages: Mapping[str, str], not_found_unsupported: bool = False) -> HumChatError:
    """Map a rejected WebSocket upgrade to a typed error (``messages`` are per-surface texts)."""
    retry: dict[str, Any] = {"status": status, "retryable": True}
    retry_after = parse_retry_after(headers.get("retry-after"))
    if retry_after is not None:
        retry["retry_after_ms"] = retry_after
    if status == 401:
        return AuthenticationError(messages["401"], status=status)
    if status == 403:
        return PermissionError(messages["403"], status=status)
    if status == 404:
        if not_found_unsupported:
            return UnsupportedFeatureError(messages["404"], status=status)
        return NotFoundError(messages["404"], status=status)
    if status == 429:
        return RateLimitError(messages["429"], **retry)
    if status == 408:
        return TimeoutError(messages["408"], **retry)
    if status in (502, 503, 504):
        return ServerError(messages["5xx"], **retry)
    return ProtocolError(messages["other"], status=status)


def ws_url(base_url: str, path: str) -> str:
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :] + path
    return "ws://" + base_url[len("http://") :] + path
