"""The HumChat client."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ._context import ResourceContext
from ._session import SessionManager
from ._signal import AbortController, AbortSignal, budget
from ._transport import HttpOptions, HttpTransport, request_timeout
from .config import RealtimeOptions, RecoveryOptions, RetryOptions, SafeConfig, SdkKeyAuth, TokenProvider, resolve_config
from .errors import AbortError, AuthenticationError, with_intent
from .realtime.manager import RealtimeClient
from .resources.channels import ChannelsResource
from .resources.conversations import ConversationsResource
from .resources.events import EventsResource
from .resources.inbox import InboxResource
from .resources.members import MembersResource
from .resources.messages import MessagesResource
from .resources.outbox import OutboxResource
from .resources.webhooks import WebhooksResource
from .types import Identity, SdkCapabilities

if TYPE_CHECKING:
    import httpx

_OMITTED: Any = object()


class HumChat:
    """Backend HTTP and realtime client. Construction performs no network IO.

    Supply exactly one of ``token_provider`` (an async callable returning a token for the
    member this backend acts for) or ``auth`` (an :class:`SdkKeyAuth` / mapping with
    ``sdk_key`` and ``act_as`` for delegated SDK-key login). ``org_id`` and ``base_url`` are
    always supplied by the application; the SDK has no default server.
    """

    def __init__(
        self,
        *,
        org_id: str = _OMITTED,
        base_url: str = _OMITTED,
        token_provider: TokenProvider = _OMITTED,
        auth: SdkKeyAuth | Mapping[str, str] = _OMITTED,
        timeout_ms: int = _OMITTED,
        retry: RetryOptions = _OMITTED,
        realtime: RealtimeOptions = _OMITTED,
        recovery: RecoveryOptions = _OMITTED,
        transport: httpx.AsyncBaseTransport | None = _OMITTED,
        **unknown: Any,
    ) -> None:
        supplied = {
            key: value
            for key, value in {
                "org_id": org_id,
                "base_url": base_url,
                "token_provider": token_provider,
                "auth": auth,
                "timeout_ms": timeout_ms,
                "retry": retry,
                "realtime": realtime,
                "recovery": recovery,
                "transport": transport,
            }.items()
            if value is not _OMITTED
        }
        config, sdk_key_auth = resolve_config({**supplied, **unknown})
        self._config = config
        self._closed = AbortController()
        key = sdk_key_auth.sdk_key if sdk_key_auth is not None else None
        self._http = HttpTransport(config, lambda value: key is not None and key in value)
        self._sessions = SessionManager(config, self._http, self._closed.signal, sdk_key_auth)
        total = config.retry["total_timeout_ms"]
        context = ResourceContext(
            org_id=config.org_id,
            request=self._request,
            connect=lambda signal, timeout_ms: self.connect(signal=signal, timeout_ms=timeout_ms),
            capabilities=lambda signal, timeout_ms: self.get_capabilities(signal=signal, timeout_ms=timeout_ms),
            get_actor_id=self._sessions.get_actor_id,
            total_timeout_ms=total,
        )
        self.channels = ChannelsResource(context)
        self.members = MembersResource(context)
        self.conversations = ConversationsResource(context)
        self.messages = MessagesResource(context, self.conversations)
        self.events = EventsResource(context)
        self.realtime = RealtimeClient(config, self._sessions, self.messages, self.events)
        self.inbox = InboxResource(context, self.conversations, self.realtime)
        self.outbox = OutboxResource(config.base_url, context.connect, self.messages)
        self.webhooks = WebhooksResource(context)

    def __repr__(self) -> str:
        return f"HumChat(org_id={self._config.org_id!r}, base_url={self._config.base_url!r}, authentication={self._config.authentication!r})"

    async def __aenter__(self) -> HumChat:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def get_config(self) -> SafeConfig:
        """A copy of the resolved configuration without credentials or callables."""
        c = self._config
        return SafeConfig(
            org_id=c.org_id,
            base_url=c.base_url,
            timeout_ms=c.timeout_ms,
            retry=dict(c.retry),
            realtime=dict(c.realtime),
            recovery=dict(c.recovery),
            authentication=c.authentication,
        )

    async def connect(self, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> Identity:
        """Acquire and verify a session; returns a copy of the verified identity."""
        request_timeout(timeout_ms, self._config)
        total = self._config.retry["total_timeout_ms"]
        operation = budget(min(timeout_ms if timeout_ms is not None else total, total), [self._closed.signal, signal])
        try:
            return copy.deepcopy((await self._sessions.get(operation.signal)).identity)
        finally:
            operation.dispose()

    async def get_capabilities(self, *, signal: AbortSignal | None = None, timeout_ms: int | None = None) -> SdkCapabilities:
        request_timeout(timeout_ms, self._config)
        total = self._config.retry["total_timeout_ms"]
        operation = budget(min(timeout_ms if timeout_ms is not None else total, total), [self._closed.signal, signal])
        try:
            return copy.deepcopy((await self._sessions.get(operation.signal)).capabilities)
        finally:
            operation.dispose()

    async def close(self) -> None:
        """Cancel this client's work. Does not log out or revoke the caller's shared token."""
        self._closed.abort(AbortError("Client is closed"))
        await self.inbox.close()
        await self.realtime.close()
        await self._http.aclose()

    async def _request(self, path: str, options: HttpOptions) -> Any:
        request_timeout(options.timeout_ms, self._config)
        operation = budget(self._config.retry["total_timeout_ms"], [self._closed.signal, options.signal])
        try:
            session = await self._sessions.get(operation.signal)
            try:
                return await self._http.request(path, session.token, options, operation.signal)
            except AuthenticationError as error:
                if error.status != 401 or not options.auth_retry:
                    raise
                session = await self._sessions.unauthorized(session, operation.signal)
                return await self._http.request(path, session.token, options, operation.signal)
        except Exception as error:
            raise with_intent(error, **options.intent()) from None
        finally:
            operation.dispose()
