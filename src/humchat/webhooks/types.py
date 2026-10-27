from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..types import DurableEvent

WebhookStatus = Literal["active", "paused", "deleted"]
WebhookDeliveryStatus = Literal["pending", "retry", "delivered", "dead"]


@dataclass
class WebhookChannel:
    channel_id: str
    epoch: str
    sequence: str
    status: Literal["active", "gap", "dead"]
    last_error: str | None


@dataclass
class WebhookSubscription:
    id: str
    org_id: str
    url: str
    status: WebhookStatus
    secret_version: int
    previous_secret_expires_at: int | None
    created_by: str
    created_at: int
    updated_at: int
    channels: list[WebhookChannel]


@dataclass
class WebhookSecretResult:
    """``signing_secret`` is returned only once by create/rotate; store it before serving requests."""

    subscription: WebhookSubscription
    signing_secret: str

    def __repr__(self) -> str:
        return f"WebhookSecretResult(subscription={self.subscription!r}, signing_secret=<redacted>)"


@dataclass
class WebhookDelivery:
    id: str
    subscription_id: str
    channel_id: str
    epoch: str
    sequence: str
    event_id: str
    status: WebhookDeliveryStatus
    attempts: int
    next_attempt_at: int
    created_at: int
    updated_at: int
    last_status: int | None
    last_error: str | None


@dataclass
class WebhookDeliveryPage:
    deliveries: list[WebhookDelivery]
    next_before: str | None


@dataclass
class WebhookDeletion:
    id: str
    deleted: Literal[True]


@dataclass
class WebhookEnvelope:
    schema: Literal[1]
    subscription_id: str
    delivery_id: str
    org_id: str
    channel_id: str
    event_id: str
    attempt: int
    sent_at: int
    event: DurableEvent


__all__ = [
    "WebhookChannel",
    "WebhookDeletion",
    "WebhookDelivery",
    "WebhookDeliveryPage",
    "WebhookDeliveryStatus",
    "WebhookEnvelope",
    "WebhookSecretResult",
    "WebhookStatus",
    "WebhookSubscription",
]
