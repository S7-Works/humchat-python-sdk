"""Verified session lifecycle: one acquisition/refresh in flight, same-principal only."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from . import _clock
from ._signal import AbortController, AbortSignal, aborted, budget, delay, ensure_future, maybe_await, race_signal
from ._transport import HttpOptions, HttpTransport
from ._util import MAX_SAFE_INTEGER, is_safe_int
from .config import ResolvedConfig, SdkKeyAuth, TokenPrevious, TokenProviderContext, TokenResult, normalize_email
from .errors import (
    AuthenticationError,
    HumChatError,
    NotFoundError,
    PermissionError,
    ProtocolError,
    SessionRevokedError,
    UnsupportedFeatureError,
)
from .types import (
    AuthenticationCapabilities,
    Identity,
    MessagingCapabilities,
    MultiplexedEvents,
    OutgoingWebhooks,
    SdkCapabilities,
    SdkKeyUserTokens,
)

_CONTROL_OR_SPACE = re.compile(r"[\s\x00-\x1f\x7f]")


class _Bad(Exception):
    pass


def _need(condition: bool) -> None:
    if not condition:
        raise _Bad


def _str(value: Any, minimum: int = 0) -> str:
    _need(isinstance(value, str) and len(value) >= minimum)
    return value


def _bool(value: Any) -> bool:
    _need(isinstance(value, bool))
    return value


def _safe(value: Any, minimum: int = 0, maximum: int = MAX_SAFE_INTEGER) -> int:
    _need(is_safe_int(value) and minimum <= value <= maximum)
    return int(value)


def _ints(value: Any) -> list[int]:
    _need(isinstance(value, list))
    return [_safe(item) for item in value]


def _strs(value: Any) -> list[str]:
    _need(isinstance(value, list))
    return [_str(item) for item in value]


def _obj(value: Any) -> Mapping[str, Any]:
    _need(isinstance(value, Mapping))
    return value


def _req(source: Mapping[str, Any], key: str) -> Any:
    _need(key in source)
    return source[key]


def _opt(source: Mapping[str, Any], key: str) -> Any:
    """zod ``.optional()``: an absent key is allowed, an explicit null is not."""
    if key not in source:
        return None
    _need(source[key] is not None)
    return source[key]


def _parse_identity(raw: Any) -> dict[str, Any]:
    v = _obj(raw)
    scopes = _opt(v, "scopes")
    expires_at = v.get("expires_at")
    app = v.get("app_uuid")
    return {
        "user_uuid": _str(_req(v, "user_uuid"), 1),
        "org": _str(_req(v, "org"), 1),
        "email": _str(_req(v, "email")),
        "roles": _strs(_req(v, "roles")),
        "scopes": None if scopes is None else _strs(scopes),
        "expires_at": None if expires_at is None else _safe(expires_at),
        "app_uuid": None if app is None else _str(app, 1),
    }


def _parse_capabilities(raw: Any) -> SdkCapabilities:
    v = _obj(raw)
    _need(_req(v, "sdk_protocol_version") == 1 and not isinstance(v["sdk_protocol_version"], bool))
    auth = _obj(_req(v, "authentication"))
    msg = _obj(_req(v, "messaging"))
    key_tokens = _opt(auth, "sdk_key_user_tokens")
    sdk_key_user_tokens = None
    if key_tokens is not None:
        k = _obj(key_tokens)
        sdk_key_user_tokens = SdkKeyUserTokens(
            endpoint=_str(_req(k, "endpoint")), key_header=_str(_req(k, "key_header")), configured=_bool(_req(k, "configured"))
        )
    end_codes = _opt(auth, "socket_session_end_codes")
    multiplexed = _opt(msg, "multiplexed_events")
    webhooks = _opt(msg, "outgoing_webhooks")
    events = _opt(msg, "event_protocol_versions")
    _need(_req(msg, "websocket_scope") == "channel")
    return SdkCapabilities(
        protocol_version=1,
        authentication=AuthenticationCapabilities(
            user_sessions=_bool(_req(auth, "user_sessions")),
            service_identities=_bool(_req(auth, "service_identities")),
            api_key_exchange=_bool(_req(auth, "api_key_exchange")),
            app_binding_required=_bool(_req(auth, "app_binding_required")),
            mock_authentication=_bool(_req(auth, "mock_authentication")),
            sdk_key_user_tokens=sdk_key_user_tokens,
            socket_session_end_codes=[] if end_codes is None else _strs(end_codes),
        ),
        messaging=MessagingCapabilities(
            send_receipt_versions=_ints(_req(msg, "send_receipt_versions")),
            message_state_versions=_ints(_req(msg, "message_state_versions")),
            history_versions=_ints(_req(msg, "history_versions")),
            durable_replay=_bool(_req(msg, "durable_replay")),
            websocket_scope="channel",
            event_protocol_versions=[] if events is None else _ints(events),
            multiplexed_events=None
            if multiplexed is None
            else MultiplexedEvents(
                protocol_versions=_ints(_req(_obj(multiplexed), "protocol_versions")),
                max_channels=_safe(_req(_obj(multiplexed), "max_channels"), 1, 100),
            ),
            outgoing_webhooks=None
            if webhooks is None
            else OutgoingWebhooks(
                schema_versions=_ints(_req(_obj(webhooks), "schema_versions")),
                max_channels=_safe(_req(_obj(webhooks), "max_channels"), 1, 50),
                workspace_channels_only=_bool(_req(_obj(webhooks), "workspace_channels_only")),
            ),
        ),
    )


@dataclass
class VerifiedSession:
    token: str
    identity: Identity
    expires_at: int | None
    refresh_at: float | None
    capabilities: SdkCapabilities

    def __repr__(self) -> str:
        return f"VerifiedSession(actor_id={self.identity.actor_id!r}, expires_at={self.expires_at!r})"


def _supplied(value: Any) -> TokenResult | None:
    if isinstance(value, TokenResult):
        return value
    if isinstance(value, Mapping):
        token = value.get("token")
        expires = value.get("expires_at", None)
        return TokenResult(token=token, expires_at=expires)  # type: ignore[arg-type]
    return None


class SessionManager:
    def __init__(self, config: ResolvedConfig, http: HttpTransport, closed: AbortSignal, sdk_key_auth: SdkKeyAuth | None = None) -> None:
        self._config = config
        self._http = http
        self._closed = closed
        self._sdk_key_auth = sdk_key_auth
        self._current: VerifiedSession | None = None
        self._pending: asyncio.Future[VerifiedSession] | None = None
        self._bound: tuple[str, str] | None = None
        self._blocked: HumChatError | None = None
        self._expired: VerifiedSession | None = None
        self._revoked = AbortController()
        self._listeners: list[Callable[[VerifiedSession], None]] = []
        self._acquisition_failures = 0
        self._retry_at = 0.0

    def __repr__(self) -> str:
        return "SessionManager()"

    def get_actor_id(self) -> str:
        if self._bound is None:
            raise ProtocolError("No verified actor is bound to this client")
        return self._bound[0]

    def on_change(self, listener: Callable[[VerifiedSession], None]) -> Callable[[], None]:
        self._listeners.append(listener)

        def remove() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove

    def revoke(self) -> None:
        self._blocked = SessionRevokedError("Session ended; create a new client with a new credential lifecycle")
        self._current = None
        self._revoked.abort(self._blocked)

    def expire(self, previous: VerifiedSession) -> None:
        """Only a negotiated explicit expiry signal may invalidate a still-cached token."""
        if self._current is previous:
            self._expired = previous

    async def get(self, signal: AbortSignal) -> VerifiedSession:
        if signal.aborted:
            raise aborted(signal)
        if self._blocked is not None:
            raise self._blocked
        if self._pending is not None:
            return await race_signal(self._pending, signal)
        if self._current is None:
            return await self._refresh("initial", signal)
        current = self._current
        if self._expired is current or (current.refresh_at is not None and _clock.now_ms() / 1000 >= current.refresh_at):
            return await self._refresh("expiring", signal)
        return current

    async def unauthorized(self, previous: VerifiedSession, signal: AbortSignal) -> VerifiedSession:
        if signal.aborted:
            raise aborted(signal)
        if self._blocked is not None:
            raise self._blocked
        if self._pending is not None:
            return await race_signal(self._pending, signal)
        if self._current is not None and self._current is not previous:
            return await self.get(signal)
        return await self._refresh("unauthorized", signal)

    async def _refresh(self, reason: str, signal: AbortSignal) -> VerifiedSession:
        if self._pending is None:
            work = ensure_future(self._acquire(reason))
            self._pending = work

            def clear(_: Any) -> None:
                if self._pending is work:
                    self._pending = None

            work.add_done_callback(clear)
        return await race_signal(self._pending, signal)

    async def _acquire(self, reason: str) -> VerifiedSession:
        lifetime = budget(self._config.retry["total_timeout_ms"], [self._closed, self._revoked.signal])
        attempted = False
        try:
            # A completed failure must not let every channel start another mint. Keep the
            # wait inside the shared flight and deadline, independent of callers.
            if self._sdk_key_auth is not None and self._retry_at > _clock.now_ms():
                await delay(self._retry_at - _clock.now_ms(), lifetime.signal)
            if lifetime.signal.aborted:
                raise aborted(lifetime.signal)
            attempted = True
            context = TokenProviderContext(reason=reason, signal=lifetime.signal)  # type: ignore[arg-type]
            if self._current is not None:
                context.previous = TokenPrevious(expires_at=self._current.expires_at)
            supplied: TokenResult | None
            if self._config.authentication == "sdk_key":
                if self._sdk_key_auth is None:
                    raise AuthenticationError("SDK-key credentials are unavailable")
                try:
                    supplied = await self._http.mint_sdk_key(self._sdk_key_auth, lifetime.signal)
                except HumChatError as error:
                    # A revoked key or denied delegated member cannot recover by retrying the
                    # same immutable credentials from every channel or subsequent HTTP call.
                    if (isinstance(error, AuthenticationError) and error.status == 401) or (
                        isinstance(error, PermissionError) and error.status == 403
                    ):
                        self._blocked = error
                        self._current = None
                    raise
            else:
                provider = self._config.token_provider
                assert provider is not None

                async def call() -> Any:
                    return await maybe_await(provider(context))

                try:
                    supplied = _supplied(await race_signal(call(), lifetime.signal))
                except Exception:
                    if lifetime.signal.aborted:
                        raise aborted(lifetime.signal) from None
                    raise AuthenticationError("Token provider failed") from None
            if supplied is None or not isinstance(supplied.token, str) or not supplied.token or _CONTROL_OR_SPACE.search(supplied.token):
                raise AuthenticationError("Token provider returned an invalid token")
            if supplied.expires_at is not None and (not is_safe_int(supplied.expires_at) or supplied.expires_at <= _clock.now_ms() / 1000):
                raise AuthenticationError("Token provider returned an invalid or expired expiry")
            try:
                raw = _parse_identity(await self._http.request("/me", supplied.token, HttpOptions(), lifetime.signal))
            except _Bad:
                raise ProtocolError("Invalid identity response") from None
            if (
                raw["org"] != self._config.org_id
                or (self._sdk_key_auth is not None and normalize_email(raw["email"]) != self._sdk_key_auth.act_as)
                or (self._bound is not None and self._bound != (raw["user_uuid"], raw["org"]))
            ):
                self._blocked = AuthenticationError("Identity changed or does not match the configured organization; create a new client")
                self._current = None
                raise self._blocked
            try:
                capabilities_raw = await self._http.request("/api/sdk/capabilities", supplied.token, HttpOptions(), lifetime.signal)
            except NotFoundError:
                raise UnsupportedFeatureError("Server SDK capability discovery is required by this preview") from None
            try:
                capabilities = _parse_capabilities(capabilities_raw)
            except _Bad:
                raise ProtocolError("Invalid SDK capability response") from None
            key_capability = capabilities.authentication.sdk_key_user_tokens
            if self._sdk_key_auth is not None:
                if key_capability is None or not key_capability.configured:
                    raise UnsupportedFeatureError("Server SDK-key login is not configured")
                if key_capability.endpoint != "/api/sdk/auth/user-token" or key_capability.key_header.lower() != "x-humchat-sdk-key":
                    raise ProtocolError("Server advertised an unsupported SDK-key login contract")
            messaging = capabilities.messaging
            if 1 not in messaging.send_receipt_versions or 1 not in messaging.message_state_versions or 2 not in messaging.history_versions:
                raise UnsupportedFeatureError("Server lacks the required send v1, state v1, or history v2 protocol")
            candidates = [value for value in (supplied.expires_at, raw["expires_at"]) if value is not None]
            expires_at = int(min(candidates)) if candidates else None
            now = _clock.now_ms() / 1000
            if lifetime.signal.aborted:
                raise aborted(lifetime.signal)
            if self._blocked is not None:
                raise self._blocked
            if expires_at is not None and expires_at <= now:
                raise AuthenticationError("Verified session is expired")
            identity = Identity(
                actor_id=raw["user_uuid"],
                org_id=raw["org"],
                email=raw["email"],
                roles=raw["roles"],
                scopes=raw["scopes"] if raw["scopes"] is not None else [],
                expires_at=raw["expires_at"],
                app_id=raw["app_uuid"],
            )
            self._bound = (identity.actor_id, identity.org_id)
            self._expired = None
            refresh_at = None if expires_at is None else expires_at - min(60, (expires_at - now) / 2)
            self._current = VerifiedSession(supplied.token, identity, expires_at, refresh_at, capabilities)
            self._acquisition_failures = 0
            self._retry_at = 0.0
            for listener in list(self._listeners):
                listener(self._current)
            return self._current
        except HumChatError as error:
            if (
                self._sdk_key_auth is not None
                and attempted
                and error.retryable
                and not self._closed.aborted
                and not self._revoked.signal.aborted
            ):
                self._acquisition_failures = min(self._acquisition_failures + 1, 5)
                ceiling = min(30_000, 2_000 * 2 ** (self._acquisition_failures - 1))
                # Equal jitter retains a one-second floor even with zero HTTP retry delays.
                # Retry-After may extend this client-wide acquisition backoff.
                backoff = math.floor(ceiling * (0.5 + _clock.random() / 2))
                self._retry_at = _clock.now_ms() + max(backoff, error.retry_after_ms or 0)
            raise
        finally:
            lifetime.dispose()
