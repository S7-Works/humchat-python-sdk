"""Stable error categories.

Server bodies and credential-provider errors are never copied into these errors.
"""

from __future__ import annotations

from typing import Any

_INTENT_FIELDS = ("channel_id", "operation_id", "message_id", "idempotency_key")


class HumChatError(Exception):
    """Base class for every SDK failure. ``code`` is a stable machine-readable category."""

    code: str
    kind: str
    status: int | None
    request_id: str | None
    retryable: bool
    channel_id: str | None
    operation_id: str | None
    message_id: str | None
    idempotency_key: str | None
    retry_after_ms: int | None
    earliest_cursor: str | None
    earliest_sequence: str | None

    def __init__(
        self,
        message: str,
        code: str = "HUMCHAT",
        *,
        status: int | None = None,
        request_id: str | None = None,
        retryable: bool = False,
        channel_id: str | None = None,
        operation_id: str | None = None,
        message_id: str | None = None,
        idempotency_key: str | None = None,
        retry_after_ms: int | None = None,
        earliest_cursor: str | None = None,
        earliest_sequence: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.kind = type(self).__name__
        self.code = code
        self.retryable = retryable
        self.status = status
        self.request_id = request_id
        self.channel_id = channel_id
        self.operation_id = operation_id
        self.message_id = message_id
        self.idempotency_key = idempotency_key
        self.retry_after_ms = retry_after_ms
        self.earliest_cursor = earliest_cursor
        self.earliest_sequence = earliest_sequence
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r}, code={self.code!r})"

    def _clone(self) -> HumChatError:
        """Copy without sharing mutable identity with the original error."""
        clone = type(self).__new__(type(self))
        clone.args = self.args
        clone.__dict__.update(self.__dict__)
        clone.__cause__ = self.__cause__
        return clone


class _CategoryError(HumChatError):
    _CODE = "HUMCHAT"

    def __init__(self, message: str, **options: Any) -> None:
        super().__init__(message, type(self)._CODE, **options)


class ConfigurationError(_CategoryError):
    _CODE = "CONFIGURATION"


class ValidationError(_CategoryError):
    _CODE = "VALIDATION"


class AuthenticationError(_CategoryError):
    _CODE = "AUTHENTICATION"


class SessionRevokedError(_CategoryError):
    _CODE = "SESSION_REVOKED"


class PermissionError(_CategoryError):  # Public name mirrors the Node SDK.
    _CODE = "PERMISSION"


class NotFoundError(_CategoryError):
    _CODE = "NOT_FOUND"


class ConflictError(_CategoryError):
    _CODE = "CONFLICT"


class RateLimitError(_CategoryError):
    _CODE = "RATE_LIMIT"


class NetworkError(_CategoryError):
    _CODE = "NETWORK"


class TimeoutError(_CategoryError):  # Public name mirrors the Node SDK.
    _CODE = "TIMEOUT"


class AbortError(_CategoryError):
    _CODE = "ABORT"


class ServerError(_CategoryError):
    _CODE = "SERVER"


class ProtocolError(_CategoryError):
    _CODE = "PROTOCOL"


class UnsupportedFeatureError(_CategoryError):
    _CODE = "UNSUPPORTED_FEATURE"


class BackpressureError(_CategoryError):
    _CODE = "BACKPRESSURE"


class RecoveryGapError(_CategoryError):
    _CODE = "RECOVERY_GAP"


class PersistenceError(HumChatError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "PERSISTENCE")


class WebhookVerificationError(HumChatError):
    """Authentication, timestamp and schema failures share a generic receiver error."""

    def __init__(self) -> None:
        super().__init__("Webhook verification failed", "WEBHOOK_VERIFICATION")


def with_intent(error: BaseException, **intent: str | None) -> HumChatError:
    """Return a copy of ``error`` carrying only caller-owned intent identifiers.

    Shared auth failures and abort reasons must not acquire another call's IDs, so the
    original is never mutated. Non-SDK failures become a retryable ``NetworkError``.
    """
    source = error if isinstance(error, HumChatError) else NetworkError("Transport failed", retryable=True)
    result = source._clone()
    for key in _INTENT_FIELDS:
        value = intent.get(key)
        if value is not None:
            setattr(result, key, value)
    return result


__all__ = [
    "AbortError",
    "AuthenticationError",
    "BackpressureError",
    "ConfigurationError",
    "ConflictError",
    "HumChatError",
    "NetworkError",
    "NotFoundError",
    "PermissionError",
    "PersistenceError",
    "ProtocolError",
    "RateLimitError",
    "RecoveryGapError",
    "ServerError",
    "SessionRevokedError",
    "TimeoutError",
    "UnsupportedFeatureError",
    "ValidationError",
    "WebhookVerificationError",
    "with_intent",
]
