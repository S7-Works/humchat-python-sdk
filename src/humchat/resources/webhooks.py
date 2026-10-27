from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

from .._context import ResourceContext
from .._signal import AbortSignal, budget
from .._transport import HttpOptions
from .._util import parse_url, utf8_len
from .._wire import Invalid, array, check, identity, literal, obj, required, safe_int, string, validate_channel
from ..errors import ProtocolError, UnsupportedFeatureError, ValidationError
from ..webhooks.types import (
    WebhookChannel,
    WebhookDeletion,
    WebhookDelivery,
    WebhookDeliveryPage,
    WebhookDeliveryStatus,
    WebhookSecretResult,
    WebhookSubscription,
)
from ..webhooks.verify import webhook_secret, webhook_uuid

_SEQUENCE = re.compile(r"^(0|[1-9][0-9]{0,18})$")
_URL_FORBIDDEN = re.compile(r"[\x00-\x20\x7f]")
_STATUSES = ("pending", "retry", "delivered", "dead")


def _time(value: Any) -> int:
    return safe_int(value, 0, 99_999_999_999)


def _sequence(value: Any) -> str:
    check(isinstance(value, str) and _SEQUENCE.fullmatch(value) is not None and int(value) <= 9_223_372_036_854_775_807)
    return value


def _enum(value: Any, *options: str) -> str:
    check(isinstance(value, str) and value in options)
    return value


def _last_error(value: Any) -> str | None:
    return None if value is None else string(value, 0, 1024)


def _url(value: Any) -> str:
    string(value, 1, 2048)
    check(utf8_len(value) <= 2048 and not _URL_FORBIDDEN.search(value))
    try:
        url = parse_url(value)
    except ValueError:
        raise Invalid from None
    check(url.scheme in ("http", "https") and not url.username and not url.password and not url.hash)
    return value


def _href(value: str) -> str:
    return parse_url(value).href


def _subscription_schema(raw: Any) -> dict[str, Any]:
    v = obj(raw)
    channels = []
    for item in array(required(v, "channels"), 50, 1):
        c = obj(item)
        channels.append(
            WebhookChannel(
                channel_id=identity(required(c, "channel_id"), 1024),
                epoch=webhook_uuid(required(c, "epoch")),
                sequence=_sequence(required(c, "sequence")),
                status=_enum(required(c, "status"), "active", "gap", "dead"),  # type: ignore[arg-type]
                last_error=_last_error(required(c, "last_error")),
            )
        )
    previous = required(v, "previous_secret_expires_at")
    return {
        "id": webhook_uuid(required(v, "id")),
        "org_uuid": identity(required(v, "org_uuid")),
        "url": _url(required(v, "url")),
        "status": _enum(required(v, "status"), "active", "paused", "deleted"),
        "secret_version": safe_int(required(v, "secret_version"), 1),
        "previous_secret_expires_at": None if previous is None else _time(previous),
        "created_by": identity(required(v, "created_by"), 256),
        "created_at": _time(required(v, "created_at")),
        "updated_at": _time(required(v, "updated_at")),
        "channels": channels,
    }


def _delivery_schema(raw: Any) -> WebhookDelivery:
    v = obj(raw)
    last_status = required(v, "last_status")
    return WebhookDelivery(
        id=webhook_uuid(required(v, "id")),
        subscription_id=webhook_uuid(required(v, "subscription_id")),
        channel_id=identity(required(v, "channel_id"), 1024),
        epoch=webhook_uuid(required(v, "epoch")),
        sequence=_sequence(required(v, "sequence")),
        event_id=string(required(v, "event_id"), 1, 160),
        status=_enum(required(v, "status"), *_STATUSES),  # type: ignore[arg-type]
        attempts=safe_int(required(v, "attempts"), 0, 8),
        next_attempt_at=_time(required(v, "next_attempt_at")),
        created_at=_time(required(v, "created_at")),
        updated_at=_time(required(v, "updated_at")),
        last_status=None if last_status is None else safe_int(last_status, 100, 599),
        last_error=_last_error(required(v, "last_error")),
    )


def _read(parse: Any, raw: Any) -> Any:
    try:
        return parse(raw)
    except Invalid:
        raise ProtocolError("Invalid webhook management response") from None


class WebhooksResource:
    """Administrative workspace grants. Mutations never automatically repeat, including on 401."""

    def __init__(self, context: ResourceContext) -> None:
        self._context = context

    async def _request(self, path: str, options: HttpOptions, channels: int | None = None) -> Any:
        lifetime = budget(self._context.total_timeout_ms, [options.signal])
        try:
            cap = (await self._context.capabilities(lifetime.signal, options.timeout_ms)).messaging.outgoing_webhooks
            if cap is None or 1 not in cap.schema_versions or not cap.workspace_channels_only:
                raise UnsupportedFeatureError("Server does not advertise outgoing webhook schema v1")
            if channels is not None and channels > cap.max_channels:
                raise ValidationError("Webhook channel grant exceeds the server limit")
            return await self._context.request(path, options.merged(signal=lifetime.signal))
        finally:
            lifetime.dispose()

    def _path(self, webhook_id: Any) -> str:
        try:
            return "/api/webhooks/" + webhook_uuid(webhook_id)
        except Invalid:
            raise ValidationError("Invalid webhook id") from None

    def _subscription(self, raw: Any, expected_id: str | None = None) -> WebhookSubscription:
        value = _read(_subscription_schema, raw)
        org = self._context.org_id
        if (
            value["org_uuid"] != org
            or (expected_id is not None and value["id"] != expected_id)
            or len({channel.channel_id for channel in value["channels"]}) != len(value["channels"])
        ):
            raise ProtocolError("Webhook subscription identity mismatch")
        for channel in value["channels"]:
            if not channel.channel_id.startswith(org + ":"):
                raise ProtocolError("Webhook grant organization mismatch")
        return WebhookSubscription(
            id=value["id"],
            org_id=value["org_uuid"],
            url=value["url"],
            status=value["status"],
            secret_version=value["secret_version"],
            previous_secret_expires_at=value["previous_secret_expires_at"],
            created_by=value["created_by"],
            created_at=value["created_at"],
            updated_at=value["updated_at"],
            channels=value["channels"],
        )

    def _result(self, raw: Any, expected_id: str) -> WebhookSubscription:
        def schema(value: Any) -> Any:
            v = obj(value)
            literal(required(v, "schema"), 1)
            check("subscription" in v)
            return v["subscription"]

        return self._subscription(_read(schema, raw), expected_id)

    def _secret_result(self, raw: Any, expected_id: str) -> WebhookSecretResult:
        def schema(value: Any) -> tuple[Any, str]:
            v = obj(value)
            literal(required(v, "schema"), 1)
            check("subscription" in v)
            return v["subscription"], webhook_secret(required(v, "signing_secret"))

        subscription, secret = _read(schema, raw)
        return WebhookSecretResult(subscription=self._subscription(subscription, expected_id), signing_secret=secret)

    def _delivery(self, raw: Any, subscription_id: str, delivery_id: str | None = None) -> WebhookDelivery:
        value: WebhookDelivery = _read(_delivery_schema, raw)
        if (
            value.subscription_id != subscription_id
            or (delivery_id is not None and value.id != delivery_id)
            or not value.channel_id.startswith(self._context.org_id + ":")
            or value.sequence == "0"
            or value.event_id != value.epoch + ":" + value.sequence
        ):
            raise ProtocolError("Webhook delivery identity mismatch")
        return value

    async def create(
        self, *, id: str, url: str, channel_ids: Sequence[str], signal: AbortSignal | None = None, timeout_ms: int | None = None  # noqa: A002
    ) -> WebhookSecretResult:
        """Create a paused subscription. The signing secret is returned exactly once."""
        try:
            webhook_uuid(id)
            _url(url)
            check(isinstance(channel_ids, (list, tuple)) and 1 <= len(channel_ids) <= 50)
            channels = [identity(channel, 1024) for channel in channel_ids]
        except Invalid:
            raise ValidationError("Invalid webhook create input") from None
        if len(set(channels)) != len(channels):
            raise ValidationError("Webhook channels must be distinct")
        for channel in channels:
            validate_channel(channel, self._context.org_id)
        raw = await self._request(
            "/api/webhooks",
            HttpOptions(
                signal=signal,
                timeout_ms=timeout_ms,
                method="POST",
                replay_safe=False,
                auth_retry=False,
                body={"id": id, "url": url, "channel_ids": channels},
            ),
            len(channels),
        )
        result = self._secret_result(raw, id)
        if (
            _href(result.subscription.url) != _href(url)
            or result.subscription.status != "paused"
            or sorted(channel.channel_id for channel in result.subscription.channels) != sorted(channels)
        ):
            raise ProtocolError("Created webhook does not match the requested grant")
        return result

    async def list(self, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> list[WebhookSubscription]:
        raw = await self._request("/api/webhooks", HttpOptions(signal=signal, timeout_ms=timeout_ms))

        def schema(value: Any) -> tuple[str, list[Any]]:
            v = obj(value)
            literal(required(v, "schema"), 1)
            return identity(required(v, "org_uuid")), array(required(v, "subscriptions"), 100)

        org, items = _read(schema, raw)
        if org != self._context.org_id:
            raise ProtocolError("Webhook list organization mismatch")
        subscriptions = [self._subscription(item) for item in items]
        if len({subscription.id for subscription in subscriptions}) != len(subscriptions):
            raise ProtocolError("Duplicate webhook subscription")
        return subscriptions

    async def get(self, webhook_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> WebhookSubscription:
        path = self._path(webhook_id)
        return self._result(await self._request(path, HttpOptions(signal=signal, timeout_ms=timeout_ms)), webhook_id)

    async def pause(self, webhook_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> WebhookSubscription:
        return await self._change(webhook_id, "pause", signal, timeout_ms)

    async def resume(self, webhook_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> WebhookSubscription:
        return await self._change(webhook_id, "resume", signal, timeout_ms)

    async def _change(self, webhook_id: str, action: str, signal: AbortSignal | None, timeout_ms: int | None) -> WebhookSubscription:
        path = self._path(webhook_id) + "/" + action
        raw = await self._request(path, HttpOptions(signal=signal, timeout_ms=timeout_ms, method="POST", replay_safe=False, auth_retry=False))
        result = self._result(raw, webhook_id)
        if result.status != ("paused" if action == "pause" else "active"):
            raise ProtocolError("Webhook status does not match the requested action")
        return result

    async def rotate_secret(self, webhook_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> WebhookSecretResult:
        path = self._path(webhook_id) + "/rotate-secret"
        raw = await self._request(path, HttpOptions(signal=signal, timeout_ms=timeout_ms, method="POST", replay_safe=False, auth_retry=False))
        return self._secret_result(raw, webhook_id)

    async def delete(self, webhook_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> WebhookDeletion:
        path = self._path(webhook_id)
        raw = await self._request(path, HttpOptions(signal=signal, timeout_ms=timeout_ms, method="DELETE", replay_safe=False, auth_retry=False))

        def schema(value: Any) -> str:
            v = obj(value)
            literal(required(v, "schema"), 1)
            check(required(v, "deleted") is True)
            return webhook_uuid(required(v, "id"))

        if _read(schema, raw) != webhook_id:
            raise ProtocolError("Deleted webhook identity mismatch")
        return WebhookDeletion(id=webhook_id, deleted=True)

    async def deliveries(
        self,
        webhook_id: str,
        *,
        status: WebhookDeliveryStatus | None = None,
        limit: int | None = None,
        before: str | None = None,
        signal: AbortSignal | None = None,
        timeout_ms: int | None = None,
    ) -> WebhookDeliveryPage:
        path = self._path(webhook_id)
        try:
            if status is not None:
                _enum(status, *_STATUSES)
            if limit is not None:
                safe_int(limit, 1, 100)
            if before is not None:
                webhook_uuid(before)
        except Invalid:
            raise ValidationError("Invalid webhook delivery list input") from None
        query: dict[str, str] = {}
        if status is not None:
            query["status"] = status
        if limit is not None:
            query["limit"] = str(int(limit))
        if before is not None:
            query["before"] = before
        raw = await self._request(path + "/deliveries" + ("?" + urlencode(query) if query else ""), HttpOptions(signal=signal, timeout_ms=timeout_ms))

        def schema(value: Any) -> tuple[str, list[Any], str | None]:
            v = obj(value)
            literal(required(v, "schema"), 1)
            next_before = required(v, "next_before")
            return (
                identity(required(v, "org_uuid")),
                array(required(v, "deliveries"), 100),
                None if next_before is None else webhook_uuid(next_before),
            )

        org, items, next_before = _read(schema, raw)
        if org != self._context.org_id:
            raise ProtocolError("Webhook deliveries organization mismatch")
        deliveries = [self._delivery(item, webhook_id) for item in items]
        if (
            len({delivery.id for delivery in deliveries}) != len(deliveries)
            or (limit is not None and len(deliveries) > limit)
            or any(status is not None and delivery.status != status for delivery in deliveries)
            or (next_before is not None and (not deliveries or next_before != deliveries[-1].id))
        ):
            raise ProtocolError("Invalid webhook delivery page")
        return WebhookDeliveryPage(deliveries=deliveries, next_before=next_before)

    async def replay(
        self, webhook_id: str, delivery_id: str, *, signal: AbortSignal | None = None, timeout_ms: int | None = None
    ) -> WebhookDelivery:
        path = self._path(webhook_id)
        try:
            webhook_uuid(delivery_id)
        except Invalid:
            raise ValidationError("Invalid webhook delivery id") from None
        raw = await self._request(
            f"{path}/deliveries/{delivery_id}/replay",
            HttpOptions(signal=signal, timeout_ms=timeout_ms, method="POST", replay_safe=False, auth_retry=False),
        )

        def schema(value: Any) -> Any:
            v = obj(value)
            literal(required(v, "schema"), 1)
            check("delivery" in v)
            return v["delivery"]

        return self._delivery(_read(schema, raw), webhook_id, delivery_id)
