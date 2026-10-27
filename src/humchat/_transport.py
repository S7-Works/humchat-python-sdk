"""HTTP transport. Credentials are supplied by the verified session manager."""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass, field, replace
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

import httpx

from . import _clock
from ._signal import AbortSignal, aborted, budget, delay, ensure_future, race_signal
from ._util import MAX_SAFE_INTEGER, MAX_TIMER_MS, is_safe_int, json_dumps, json_loads
from .errors import (
    AbortError,
    AuthenticationError,
    ConflictError,
    HumChatError,
    NetworkError,
    NotFoundError,
    PermissionError,
    ProtocolError,
    RateLimitError,
    ServerError,
    TimeoutError,
    UnsupportedFeatureError,
    ValidationError,
    with_intent,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import ResolvedConfig, SdkKeyAuth, TokenResult

_MISSING: Any = object()
_TRANSIENT = {408, 429, 502, 503, 504}
_REDIRECTS = {301, 302, 303, 307, 308}
_REQUEST_ID = re.compile(r"^[\w.:-]{1,128}$", re.ASCII)
_NUMERIC = re.compile(r"^\d+(?:\.\d+)?$")
_PRINTABLE = re.compile(r"^[\x21-\x7e]+$")
MAX_RESPONSE_BYTES = 10 * 1024 * 1024


@dataclass
class HttpOptions:
    method: str = "GET"
    body: Any = _MISSING
    headers: dict[str, str] = field(default_factory=dict)
    signal: AbortSignal | None = None
    timeout_ms: int | None = None
    replay_safe: bool = False
    #: Internal: one-time secret operations must not replay after a 401.
    auth_retry: bool = True
    channel_id: str | None = None
    operation_id: str | None = None
    message_id: str | None = None
    idempotency_key: str | None = None

    def intent(self) -> dict[str, str | None]:
        return {
            "channel_id": self.channel_id,
            "operation_id": self.operation_id,
            "message_id": self.message_id,
            "idempotency_key": self.idempotency_key,
        }

    def merged(self, **changes: Any) -> HttpOptions:
        return replace(self, **changes)


def request_timeout(timeout_ms: Any, config: ResolvedConfig) -> int:
    value = config.timeout_ms if timeout_ms is None else timeout_ms
    if not is_safe_int(value) or value <= 0 or value > MAX_TIMER_MS:
        raise ValidationError("timeout must be a positive bounded integer")
    return int(value)


def parse_retry_after(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    milliseconds: float
    if _NUMERIC.fullmatch(text):
        milliseconds = float(text) * 1000
    else:
        try:
            moment = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None
        if moment is None:
            return None
        milliseconds = moment.timestamp() * 1000 - _clock.now_ms()
    if not math.isfinite(milliseconds):
        return None
    return max(0, min(MAX_TIMER_MS, math.ceil(milliseconds)))


def _status_error(response: httpx.Response, is_sensitive: Callable[[str], bool], mint: bool) -> HumChatError:
    status = response.status_code
    options: dict[str, Any] = {"status": status, "retryable": status in _TRANSIENT}
    request_id = response.headers.get("x-request-id")
    if request_id and _REQUEST_ID.fullmatch(request_id) and not is_sensitive(request_id):
        options["request_id"] = request_id
    retry_after = parse_retry_after(response.headers.get("retry-after"))
    if retry_after is not None:
        options["retry_after_ms"] = retry_after
    if status == 401:
        return AuthenticationError("Authentication rejected", **options)
    if status == 403:
        return PermissionError("Permission denied", **options)
    if mint and status in (404, 405):
        return UnsupportedFeatureError("Server does not support SDK-key login", **options)
    if status == 404:
        return NotFoundError("Resource not found", **options)
    if status == 409:
        return ConflictError("Operation conflicts with existing state or intent", **options)
    if status == 429:
        return RateLimitError("Rate limit exceeded", **options)
    if status in (400, 422) or (mint and status in (413, 415)):
        return ValidationError("Server rejected request input", **options)
    if status == 408:
        return TimeoutError("Server request timeout", **options)
    if status >= 500:
        return ServerError("Server request failed", **options)
    return ProtocolError("Unexpected HTTP status", **options)


class HttpTransport:
    """httpx-only transport that never follows redirects and never logs bodies."""

    def __init__(self, config: ResolvedConfig, is_sensitive: Callable[[str], bool] | None = None) -> None:
        self._config = config
        self._is_sensitive = is_sensitive or (lambda _value: False)
        self._cooldown_until = 0.0
        self._credentials: set[str] = set()
        self._omit_request_ids = False
        self._client: httpx.AsyncClient | None = None

    def __repr__(self) -> str:
        return "HttpTransport()"

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            # Proxies are application-managed through an explicit transport, like Node's fetch option.
            self._client = httpx.AsyncClient(transport=self._config.transport, follow_redirects=False, trust_env=False, timeout=None)
        return self._client

    async def aclose(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    def _remember_credential(self, value: str) -> None:
        # Only credentials this short can fit in the bounded request-ID header.
        if self._omit_request_ids or not value or len(value) > 128 or value in self._credentials:
            return
        # Never forget an old credential while continuing to expose remote IDs. Long-lived
        # clients fail closed once the bounded redaction history fills.
        if len(self._credentials) >= 128:
            self._credentials.clear()
            self._omit_request_ids = True
        else:
            self._credentials.add(value)

    def _sensitive_request_id(self, value: str) -> bool:
        return self._omit_request_ids or self._is_sensitive(value) or any(credential in value for credential in self._credentials)

    async def request(self, path: str, token: str, options: HttpOptions, signal: AbortSignal) -> Any:
        value, _ = await self._execute(path, {"token": token}, options, signal)
        return value

    async def mint_sdk_key(self, auth: SdkKeyAuth, signal: AbortSignal) -> TokenResult:
        """Bootstrap only at this fixed same-origin endpoint; never follow a credential redirect."""
        from .config import TokenResult

        value, requested_at = await self._execute(
            "/api/sdk/auth/user-token",
            {"sdk_key": auth.sdk_key},
            # Retrying login can issue another short-lived token; it does not mutate messaging state.
            HttpOptions(method="POST", body={"org_uuid": self._config.org_id, "email": auth.act_as, "expires_in": 900}, replay_safe=True),
            signal,
        )
        # Remember even unusable/expired results before verification can reject them.
        if isinstance(value, dict) and isinstance(value.get("access_token"), str):
            self._remember_credential(value["access_token"])
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("access_token"), str)
            or not _PRINTABLE.fullmatch(value["access_token"])
            or value["access_token"].startswith("hsk_")
            or auth.sdk_key in value["access_token"]
            or not isinstance(value.get("token_type"), str)
            or value["token_type"].lower() != "bearer"
            or not is_safe_int(value.get("expires_in"))
            or not 1 <= value["expires_in"] <= 3600
        ):
            raise ProtocolError("Invalid SDK-key token response")
        # Count latency against validity; never extend a token's lifetime by response duration.
        return TokenResult(token=value["access_token"], expires_at=int(requested_at // 1000) + int(value["expires_in"]))

    async def _execute(self, path: str, credential: dict[str, str], options: HttpOptions, signal: AbortSignal) -> tuple[Any, float]:
        timeout = request_timeout(options.timeout_ms, self._config)
        method = options.method
        replay_safe = method == "GET" or options.replay_safe
        body = None if options.body is _MISSING else json_dumps(options.body).encode("utf-8")
        mint = "sdk_key" in credential
        self._remember_credential(credential["sdk_key"] if mint else credential["token"])
        attempt = 0
        while True:
            try:
                await delay(max(0.0, self._cooldown_until - _clock.now_ms()), signal)
                if signal.aborted:
                    raise aborted(signal)
                attempt_budget = budget(timeout, [signal])
                try:
                    headers = {key: value for key, value in options.headers.items()}
                    for key in list(headers):
                        if key.lower() in ("authorization", "x-humchat-sdk-key", "accept", "content-type"):
                            del headers[key]
                    if mint:
                        headers["X-HumChat-Sdk-Key"] = credential["sdk_key"]
                    else:
                        headers["Authorization"] = f"Bearer {credential['token']}"
                    headers["Accept"] = "application/json"
                    if body is not None:
                        headers["Content-Type"] = "application/json"
                    requested_at = _clock.now_ms()
                    response = await self._send(method, self._config.base_url + path, headers, body, attempt_budget.signal)
                    try:
                        if response.status_code in _REDIRECTS:
                            # fetch(redirect: 'error') turns every redirect into a network failure.
                            raise NetworkError("Transport failed", retryable=True)
                        if not response.is_success:
                            if response.status_code == 410 and path.startswith("/api/events/sync?") and options.channel_id:
                                from .realtime.durable_protocol import parse_gap

                                raise parse_gap(await self._read_json(response, attempt_budget.signal), self._config.org_id, options.channel_id)
                            error = _status_error(response, self._sensitive_request_id, mint)
                            if isinstance(error, RateLimitError) and error.retry_after_ms is not None:
                                self._cooldown_until = max(self._cooldown_until, _clock.now_ms() + error.retry_after_ms)
                            raise error
                        return await self._read_json(response, attempt_budget.signal), requested_at
                    finally:
                        await _close_quietly(response)
                finally:
                    attempt_budget.dispose()
            except Exception as failure:
                error = with_intent(aborted(signal) if signal.aborted else failure, **options.intent())
                if signal.aborted or isinstance(error, AbortError) or not replay_safe or not error.retryable or attempt >= self._config.retry["max_retries"]:
                    raise error from None
                ceiling = min(self._config.retry["max_delay_ms"], self._config.retry["initial_delay_ms"] * 2**attempt)
                wait = max(error.retry_after_ms or 0, math.floor(_clock.random() * (ceiling + 1)))
                try:
                    await delay(wait, signal)
                except HumChatError as cancelled:
                    raise with_intent(cancelled, **options.intent()) from None
            attempt += 1

    async def _send(self, method: str, url: str, headers: dict[str, str], body: bytes | None, signal: AbortSignal) -> httpx.Response:
        client = self._http()
        try:
            request = client.build_request(method, url, headers=headers, content=body)
        except Exception:
            raise NetworkError("Transport failed", retryable=True) from None
        task = ensure_future(client.send(request, stream=True))
        try:
            return await race_signal(task, signal)
        except HumChatError:
            if signal.aborted:
                task.cancel()
                task.add_done_callback(_close_late_response)
                raise aborted(signal) from None
            raise
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception:
            if signal.aborted:
                raise aborted(signal) from None
            raise NetworkError("Transport failed", retryable=True) from None

    async def _read_json(self, response: httpx.Response, signal: AbortSignal) -> Any:
        chunks: list[bytes] = []
        length = 0
        # Decoded bytes, like fetch's body stream; the size bound applies after decompression.
        stream = response.aiter_bytes()
        while True:
            try:
                chunk = await race_signal(_next_chunk(stream), signal)
            except HumChatError:
                if signal.aborted:
                    raise aborted(signal) from None
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                if signal.aborted:
                    raise aborted(signal) from None
                raise NetworkError("Response stream failed", retryable=True) from None
            if chunk is None:
                break
            length += len(chunk)
            if length > MAX_RESPONSE_BYTES:
                raise ProtocolError("Response exceeds the 10 MiB SDK limit")
            chunks.append(chunk)
        try:
            return json_loads(b"".join(chunks))
        except (ValueError, UnicodeDecodeError, RecursionError):
            raise ProtocolError("Response is not valid JSON") from None


async def _next_chunk(stream: Any) -> bytes | None:
    try:
        return bytes(await stream.__anext__())
    except StopAsyncIteration:
        return None


async def _close_quietly(response: httpx.Response) -> None:
    try:
        await response.aclose()
    except Exception:
        pass


def _close_late_response(task: asyncio.Future[httpx.Response]) -> None:
    if task.cancelled() or task.exception() is not None:
        return
    from ._signal import spawn

    spawn(_close_quietly(task.result()))


__all__ = ["MAX_SAFE_INTEGER", "HttpOptions", "HttpTransport", "parse_retry_after", "request_timeout"]
