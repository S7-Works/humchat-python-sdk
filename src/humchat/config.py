"""Client configuration, validated before any network IO."""

from __future__ import annotations

import re
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypedDict, Union

from ._util import (
    MAX_SAFE_INTEGER,
    MAX_TIMER_MS,
    has_js_whitespace_or_control,
    is_safe_int,
    js_trim,
    parse_url,
    utf8_len,
)
from .errors import ConfigurationError, UnsupportedFeatureError, ValidationError

if TYPE_CHECKING:
    import httpx

    from ._signal import AbortSignal


@dataclass
class TokenResult:
    token: str
    #: Unix seconds, never milliseconds. Prefer returning this when known.
    expires_at: int | None = None


@dataclass
class TokenPrevious:
    expires_at: int | None


@dataclass
class TokenProviderContext:
    reason: Literal["initial", "expiring", "unauthorized"]
    #: Shared acquisition deadline. A single caller's abort does not cancel other waiters.
    signal: AbortSignal
    previous: TokenPrevious | None = None


TokenProviderResult = Union[TokenResult, Mapping[str, Any]]
TokenProvider = Callable[[TokenProviderContext], Union[Awaitable[TokenProviderResult], TokenProviderResult]]


class RetryOptions(TypedDict, total=False):
    max_retries: int
    initial_delay_ms: int
    max_delay_ms: int
    total_timeout_ms: int


class RealtimeOptions(TypedDict, total=False):
    #: Multiplex requires durable event protocol support; it never falls back silently.
    transport: Literal["channel", "multiplex"]
    max_channels: int
    connect_concurrency: int
    max_buffered_events: int
    max_buffered_bytes: int
    max_payload_bytes: int
    max_cached_messages: int
    max_cached_bytes: int
    reconnect_initial_delay_ms: int
    reconnect_max_delay_ms: int
    ping_interval_ms: int
    pong_timeout_ms: int
    handler_timeout_ms: int


class RecoveryOptions(TypedDict, total=False):
    #: Durable is the default; older servers require explicit ``best_effort`` opt-in.
    mode: Literal["best_effort", "durable"]
    max_pages: int
    max_threads: int
    max_bytes: int
    max_state_refresh: int
    timeout_ms: int


@dataclass(frozen=True)
class SdkKeyAuth:
    """Delegated member login through the configured HumChat server.

    ``sdk_key`` is a secret backend credential and is never sent as a Bearer token.
    ``act_as`` is the organization member whose identity the server verifies.
    """

    sdk_key: str = field(repr=False)
    act_as: str

    def __repr__(self) -> str:
        return f"SdkKeyAuth(sdk_key=<redacted>, act_as={self.act_as!r})"


@dataclass
class SafeConfig:
    org_id: str
    base_url: str
    timeout_ms: int
    retry: dict[str, int]
    realtime: dict[str, Any]
    recovery: dict[str, Any]
    authentication: Literal["token_provider", "sdk_key"]


@dataclass
class ResolvedConfig:
    org_id: str
    base_url: str
    timeout_ms: int
    retry: dict[str, int]
    realtime: dict[str, Any]
    recovery: dict[str, Any]
    authentication: Literal["token_provider", "sdk_key"]
    token_provider: TokenProvider | None
    transport: httpx.AsyncBaseTransport | None

    def __repr__(self) -> str:
        return f"ResolvedConfig(org_id={self.org_id!r}, base_url={self.base_url!r}, authentication={self.authentication!r})"


_RETRY_DEFAULTS = {"max_retries": 3, "initial_delay_ms": 250, "max_delay_ms": 2_000, "total_timeout_ms": 30_000}
_REALTIME_DEFAULTS: dict[str, Any] = {
    "transport": "channel",
    "max_channels": 50,
    "connect_concurrency": 5,
    "max_buffered_events": 1_000,
    "max_buffered_bytes": 16 * 1024 * 1024,
    "max_payload_bytes": 10 * 1024 * 1024,
    "max_cached_messages": 1_000,
    "max_cached_bytes": 8 * 1024 * 1024,
    "reconnect_initial_delay_ms": 500,
    "reconnect_max_delay_ms": 30_000,
    "ping_interval_ms": 30_000,
    "pong_timeout_ms": 10_000,
    "handler_timeout_ms": 30_000,
}
_RECOVERY_DEFAULTS: dict[str, Any] = {
    "mode": "durable",
    "max_pages": 3,
    "max_threads": 20,
    "max_bytes": 16 * 1024 * 1024,
    "max_state_refresh": 50,
    "timeout_ms": 30_000,
}
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_SDK_KEY = re.compile(r"^hsk_[A-Za-z0-9_-]+$")
_LOOPBACK = re.compile(r"^http://(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?/?$", re.IGNORECASE)
_KNOWN = {"org_id", "base_url", "token_provider", "auth", "timeout_ms", "retry", "transport", "realtime", "recovery"}


def normalize_email(email: str) -> str:
    return re.sub(r"[A-Z]", lambda match: match.group(0).lower(), js_trim(email))


def _validate_sdk_key_auth(value: Any) -> SdkKeyAuth:
    if isinstance(value, SdkKeyAuth):
        raw: dict[str, Any] = {"sdk_key": value.sdk_key, "act_as": value.act_as}
    elif isinstance(value, Mapping):
        raw = dict(value)
    else:
        raise ValidationError("auth must contain sdk_key and act_as")
    if any(key not in ("sdk_key", "act_as") for key in raw):
        raise ValidationError("Unknown auth field")
    key = raw.get("sdk_key")
    if not isinstance(key, str) or len(key) > 128 or not _SDK_KEY.fullmatch(key):
        raise ValidationError("auth.sdk_key must be an hsk_ credential without whitespace or invalid header characters")
    act_as = raw.get("act_as")
    if not isinstance(act_as, str):
        raise ValidationError("auth.act_as must be an email address")
    # Snapshot once; later caller changes must not change the delegated principal.
    email = normalize_email(act_as)
    parts = email.split("@")
    if (
        utf8_len(email) > 254
        or has_js_whitespace_or_control(email)
        or len(parts) != 2
        or not parts[0]
        or "." not in parts[1]
        or any(not label for label in parts[1].split("."))
    ):
        raise ValidationError("auth.act_as must be an email address")
    return SdkKeyAuth(sdk_key=key, act_as=email)


def _refuse_browser_sdk_key() -> None:
    if sys.platform == "emscripten":
        raise UnsupportedFeatureError("SDK-key authentication is available only in a backend Python process")


def _pick(section: Mapping[str, Any], key: str, default: Any) -> Any:
    """JavaScript ``section[key] ?? default``."""
    value = section.get(key)
    return default if value is None else value


def _integer(value: Any, name: str, minimum: int, maximum: int = MAX_TIMER_MS) -> int:
    if not is_safe_int(value) or value < minimum or value > maximum:
        raise ConfigurationError(f"{name} must be an integer between {minimum} and {maximum}")
    return int(value)


def resolve_config(options: Mapping[str, Any]) -> tuple[ResolvedConfig, SdkKeyAuth | None]:
    raw = dict(options)
    if "api_key" in raw:
        raise UnsupportedFeatureError("api_key bot authentication is not supported; use token_provider or delegated auth configuration")
    sdk_key_mode = "auth" in raw
    if sdk_key_mode and "token_provider" in raw:
        raise ValidationError("Configure either auth or token_provider, not both")
    for key in raw:
        if key not in _KNOWN:
            raise ConfigurationError("Unknown configuration field")
    org_id = raw.get("org_id")
    sdk_key_auth: SdkKeyAuth | None = None
    token_provider: TokenProvider | None = None
    if sdk_key_mode:
        _refuse_browser_sdk_key()
        sdk_key_auth = _validate_sdk_key_auth(raw["auth"])
        if not isinstance(org_id, str) or not _UUID.fullmatch(org_id):
            raise ValidationError("org_id must be a canonical UUID for SDK-key authentication")
        org_id = org_id.lower()
        authentication: Literal["token_provider", "sdk_key"] = "sdk_key"
    else:
        token_provider = raw.get("token_provider")
        if not callable(token_provider):
            raise ConfigurationError("token_provider must be an async function")
        authentication = "token_provider"
    if not isinstance(org_id, str) or not js_trim(org_id) or org_id != js_trim(org_id):
        raise ConfigurationError("org_id must be a non-empty string without surrounding whitespace")
    base_url = raw.get("base_url")
    if not isinstance(base_url, str):
        raise ConfigurationError("base_url is required during the preview")
    try:
        url = parse_url(base_url)
    except ValueError:
        raise ConfigurationError("base_url must be an absolute HTTP(S) origin") from None
    if url.scheme not in ("http", "https") or url.username or url.password or url.search or url.hash or url.pathname != "/":
        raise ConfigurationError("base_url must be an HTTP(S) origin without credentials, path, query, or fragment")
    if sdk_key_mode and url.scheme != "https" and not _LOOPBACK.fullmatch(base_url):
        raise ConfigurationError("SDK-key authentication requires HTTPS outside literal loopback development origins")
    transport = raw.get("transport")
    if transport is not None:
        import httpx

        if not isinstance(transport, httpx.AsyncBaseTransport):
            raise ConfigurationError("transport must be an httpx.AsyncBaseTransport")
    retry_raw = raw.get("retry")
    if "retry" in raw and not isinstance(retry_raw, Mapping):
        raise ConfigurationError("retry must be an object")
    retry_raw = dict(retry_raw or {})
    for key in retry_raw:
        if key not in _RETRY_DEFAULTS:
            raise ConfigurationError("Unknown retry field")
    retry = {
        "max_retries": _integer(_pick(retry_raw, "max_retries", 3), "retry.max_retries", 0, 10),
        "initial_delay_ms": _integer(_pick(retry_raw, "initial_delay_ms", 250), "retry.initial_delay_ms", 0),
        "max_delay_ms": _integer(_pick(retry_raw, "max_delay_ms", 2_000), "retry.max_delay_ms", 0),
        "total_timeout_ms": _integer(_pick(retry_raw, "total_timeout_ms", 30_000), "retry.total_timeout_ms", 1),
    }
    sections: dict[str, dict[str, Any]] = {}
    for name, defaults in (("realtime", _REALTIME_DEFAULTS), ("recovery", _RECOVERY_DEFAULTS)):
        value = raw.get(name)
        if name in raw and not isinstance(value, Mapping):
            raise ConfigurationError(f"{name} must be an object")
        section = dict(value or {})
        for key in section:
            if key not in defaults:
                raise ConfigurationError(f"Unknown {name} field")
        sections[name] = section
    realtime: dict[str, Any] = dict(_REALTIME_DEFAULTS)
    for key, default in _REALTIME_DEFAULTS.items():
        if key == "transport":
            continue
        realtime[key] = _integer(_pick(sections["realtime"], key, default), f"realtime.{key}", 1)
    realtime["transport"] = _pick(sections["realtime"], "transport", "channel")
    if realtime["transport"] not in ("channel", "multiplex"):
        raise ConfigurationError("realtime.transport must be channel or multiplex")
    rec = sections["recovery"]
    mode = _pick(rec, "mode", "durable")
    if mode not in ("best_effort", "durable"):
        raise ConfigurationError("recovery.mode must be best_effort or durable")
    if realtime["transport"] == "multiplex" and mode != "durable":
        raise UnsupportedFeatureError("Multiplex transport requires durable recovery")
    recovery = {
        "mode": mode,
        "max_pages": _integer(_pick(rec, "max_pages", 3), "recovery.max_pages", 1, 100),
        "max_threads": _integer(_pick(rec, "max_threads", 20), "recovery.max_threads", 0, 1_000),
        "max_bytes": _integer(_pick(rec, "max_bytes", 16 * 1024 * 1024), "recovery.max_bytes", 1),
        "max_state_refresh": _integer(_pick(rec, "max_state_refresh", 50), "recovery.max_state_refresh", 0, 10_000),
        "timeout_ms": _integer(_pick(rec, "timeout_ms", 30_000), "recovery.timeout_ms", 1),
    }
    timeout_ms = _integer(_pick(raw, "timeout_ms", 10_000), "timeout_ms", 1)
    config = ResolvedConfig(
        org_id=org_id,
        base_url=url.origin,
        timeout_ms=timeout_ms,
        retry=retry,
        realtime=realtime,
        recovery=recovery,
        authentication=authentication,
        token_provider=token_provider,
        transport=transport,
    )
    return config, sdk_key_auth


__all__ = [
    "MAX_SAFE_INTEGER",
    "RealtimeOptions",
    "RecoveryOptions",
    "ResolvedConfig",
    "RetryOptions",
    "SafeConfig",
    "SdkKeyAuth",
    "TokenPrevious",
    "TokenProvider",
    "TokenProviderContext",
    "TokenResult",
    "normalize_email",
    "resolve_config",
]
