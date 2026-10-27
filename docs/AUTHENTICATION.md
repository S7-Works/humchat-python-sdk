# Authentication and client setup

`humchat` accepts a caller-supplied session token through `token_provider`, or performs delegated member login through `auth={"sdk_key": ..., "act_as": ...}` (also accepted as `SdkKeyAuth(sdk_key=..., act_as=...)`). A top-level `api_key` keyword raises `UnsupportedFeatureError`; a key is never passed as a Bearer token.

SDK-key exchange happens entirely through the HumChat server you configure. `connect()` and lazy resource calls mint once, verify `/me`, and discover capabilities before exposing a session. No identity-provider URL or client credentials belong in SDK configuration. See the [login guide](SDK_KEY_LOGIN_GUIDE.md) for setup, the server contract and deployment validation requirements.

The two authentication modes are exclusive. Supplying both keywords, even with one explicitly `None`, raises `ValidationError`. SDK-key mode requires a canonical UUID organization, an `hsk_` key containing only base64url characters after its prefix (at most 128 characters total), and a member email. It requires HTTPS except for the literal development origins `localhost`, `127.0.0.1` and `[::1]`. Token-provider mode supports opaque/mock organization IDs. `get_config().authentication` identifies the selected mode (`"sdk_key"` or `"token_provider"`) without including credentials or the requested member email.

Python 3.10 or later is required. The package is not published to a package index; build a local wheel as described in the [README](../README.md).

## Minimal configuration

```python
import os

from humchat import HumChat

client = HumChat(
    org_id=os.environ["HUMCHAT_ORG_ID"],
    base_url=os.environ["HUMCHAT_BASE_URL"],
    auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]},
)
try:
    await client.connect()
    members = await client.members.list()
    # Use an authorized recipient's actor_id with messages.send_direct().
finally:
    await client.close()
```

An org owner/admin creates a key in HumChat (`POST /api/workspace/sdk-keys` with `{"name": "Backend integration"}`). The returned key is shown only once. Administrators can list key metadata and revoke keys in HumChat. Give the key to the backend operator privately; delegated messaging credentials are not intended for these admin operations.

The SDK snapshots and normalizes the member email once at construction (later changes to the caller's mapping have no effect), sends `org_uuid`, `email` and `expires_in: 900` to `POST /api/sdk/auth/user-token`, and puts the key only in `X-HumChat-Sdk-Key`. It never follows redirects and uses Bearer tokens for subsequent calls. It validates `token_type`, token shape and a positive integer response lifetime of at most 3600 seconds; a malformed success raises `ProtocolError`. Expiry is measured from the start of the successful mint attempt, then capped by `/me`'s verified expiry.

Acquisition is shared across concurrent callers, cached, and renewed through the same mint endpoint when due (up to 60 seconds early), with one coordinated retry after an eligible resource HTTP 401. Non-replayable conversation creation and one-time admin-secret operations are never retried. Mint 401/403 failures are terminal for this client; 429/502/503 and transient transport failures use bounded backoff within the acquisition deadline. There is no refresh token and no mint on every normal request.

The configured organization and normalized `act_as` must match the server-verified identity. Login bootstraps at a fixed endpoint/header because capability discovery itself requires a token. The discovered SDK-key capability must be configured and match that path/header; it cannot redirect the credential.

Key renewal covers HTTP, per-channel sockets, the multiplexed socket and durable checkpoint recovery. Proactive renewal works without explicit socket expiry codes. When `authentication.socket_session_end_codes` advertises `TOKEN_EXPIRED`, a frame carrying that exact code can invalidate the old token and trigger verified reacquisition/reconnect. An ambiguous `session_revoked` stays terminal. Key revocation stops future mints; issued tokens may remain valid until expiry unless separately revoked.

After a retryable SDK-key acquisition fails, all HTTP and socket callers share an authentication cooldown: 1–2 seconds initially, doubling after consecutive failures to a 15–30-second range; `Retry-After` can extend it. The cooldown resets after mint, identity verification and capability discovery all succeed. Closing the client or a terminal revocation cancels the shared wait; cancelling one caller only detaches that caller. This does not coordinate quotas across separate clients or processes.

## Caller-managed token

```python
import os

from humchat import HumChat, TokenResult


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Set {name}")
    return value


async def token_provider(context):
    return TokenResult(token=required("HUMCHAT_TOKEN"))


async def main() -> None:
    async with HumChat(org_id=required("HUMCHAT_ORG_ID"), base_url=required("HUMCHAT_BASE_URL"), token_provider=token_provider, timeout_ms=10_000) as client:
        identity = await client.connect()
        receipt = await client.messages.send(channel_id=required("HUMCHAT_CHANNEL_ID"), content="Hello from my backend.")
        print({"actor_id": identity.actor_id, "message_id": receipt.id})
```

The environment-token provider above is static; it cannot renew a token. A long-running application needs its own supported credential acquisition and renewal flow.

`base_url` remains explicit. It must be an HTTP(S) origin such as `https://chat.example.invalid`, with no credentials, path prefix, query or fragment. A custom `transport=` (an `httpx.AsyncBaseTransport`) receives credentials, including the SDK key during minting, so supply only an application-controlled transport; it affects HTTP, not WebSockets. Environment proxy variables are ignored, as in the Node SDK.

Construction performs no network work. `connect()` acquires a token, verifies `/me`, and discovers `/api/sdk/capabilities`. The server must advertise send receipts v1, message state v1 and history v2. Durable subscriptions additionally require event protocol v1 and durable replay.

## Token provider contract

The provider is called with a `TokenProviderContext` and returns (or awaits to) a `TokenResult(token, expires_at=None)` or a mapping with those keys:

| Field | Meaning |
| --- | --- |
| `reason` | `initial`, `expiring`, or `unauthorized`. |
| `previous.expires_at` | `previous` is `None` on first use; otherwise the preceding session's effective expiry (Unix seconds) or `None` when unknown. The old token is not included. |
| `signal` | Shared acquisition `AbortSignal` (cancellation/deadline). Check `signal.aborted` or register `signal.add_listener(...)` in cancellable work. |
| returned `token` | A nonempty string without whitespace or control characters. |
| returned `expires_at` | Optional future integer in Unix seconds, not milliseconds. |

Concurrent calls share one acquisition/refresh. Cancelling one request stops that request's wait; it does not cancel acquisition needed by other callers. Closing the client cancels the shared acquisition. Provider exception messages are never copied into SDK errors.

The SDK uses the earlier of provider expiry and server-verified expiry, and refreshes up to 60 seconds before it (or halfway through a short lifetime). With no known expiry there is no proactive timer. A resource HTTP 401 triggers one coordinated refresh and one replay within the operation's total HTTP budget.

## Identity, scope and revocation

`connect()` returns `Identity(actor_id, org_id, email, roles, scopes, expires_at, app_id)`. Its `expires_at` is the server-reported expiry and can be `None`. `scopes` defaults to an empty list and `app_id` to `None` when the server does not provide verified values.

The first successful connection binds the client to its actor and organization. Refresh cannot switch either; a mismatch blocks the client with `AuthenticationError`. Create a new client for a different principal. Returned identity and capability objects are copies.

The server checks channel access. Use `channels.list()` and `conversations.list()` for discovery, and the canonical routing key: `Channel.id` or `Conversation.channel_id`. A conversation's management `id` is not its messaging key.

A real-time revocation blocks all subscriptions and future authenticated work and aborts pending acquisition. Only the separately negotiated `TOKEN_EXPIRED` code permits renewal. `close()` cancels the SDK's work; it does not log out or revoke a shared token.

`get_config()` excludes the provider, SDK key, requested email and token, and `repr()` of the client, its configuration and its errors never includes credentials. Transport errors discard response prose and credential-bearing request IDs: the client remembers up to 128 distinct short credentials and, once that history fills, permanently omits remote request IDs. Mint responses containing the SDK key, or starting with `hsk_`, are rejected before Bearer verification.

Related guides: [API](API.md), [reliability](RELIABILITY.md), [troubleshooting](TROUBLESHOOTING.md).
