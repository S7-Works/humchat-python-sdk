# HumChat SDK-key login

The preview supports delegated member login through your configured HumChat server. Use an SDK key and a member email to send and receive as that member. The SDK caches and renews the returned user token for HTTP requests and sockets. Keep the key in your backend's private configuration (environment variables or a secret manager), never in source. Validate the SDK and your server deployment together before production use.

## Login flow

1. An organization administrator creates an SDK key in HumChat and gives it to the backend operator through a private channel.
2. The backend sends the key, organization UUID and member email to HumChat.
3. HumChat checks the key and member eligibility, then returns a short-lived user access token.
4. The SDK uses that token as Bearer authentication for HumChat HTTP requests and sockets. Messages use the verified member identity and that member's permitted conversations/channels.

The SDK does not perform OTP login or contact any identity backend. No identity-provider client ID or secret belongs in SDK configuration. A key can request a token for members of its own organization only.

## SDK configuration

```python
import os

from humchat import HumChat

humchat = HumChat(
    base_url=os.environ["HUMCHAT_BASE_URL"],
    org_id=os.environ["HUMCHAT_ORG_ID"],
    auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]},
)
try:
    await humchat.connect()
    members = await humchat.members.list()
    # Select an authorized recipient from members for messages.send_direct().
finally:
    await humchat.close()
```

Keep `token_provider` for applications that bring a user token. Combining it with `auth` raises `ValidationError`, including an explicitly `None` alternate keyword. The distinct `api_key` option remains unsupported; this feature is delegated member login, not a bot mode.

## Server contract

```http
POST /api/sdk/auth/user-token
X-HumChat-Sdk-Key: <secret from private configuration>
Content-Type: application/json

{"org_uuid":"<org UUID>","email":"<member email>","expires_in":900}
```

Success is `{ access_token, token_type: "Bearer", expires_in }`. The key belongs only in `X-HumChat-Sdk-Key`. Normal authenticated requests use the returned token in `Authorization: Bearer ...`.

| Status | Handling |
| --- | --- |
| 400 / 413 / 415 / 422 | `ValidationError`; fix the request, do not retry unchanged |
| 401 | `AuthenticationError`; invalid, missing or revoked key; terminal for the client |
| 403 | `PermissionError`; organization mismatch or refused sign-in; terminal for the client |
| 404 / 405 | `UnsupportedFeatureError`; SDK-key login endpoint unsupported |
| 429 | Bounded backoff; `Retry-After` honored |
| 502 / 503 | Bounded retry for unavailable login |

The implementation caches the token, shares concurrent mint requests and renews up to 60 seconds before expiry. Retryable acquisition failures share a client-wide cooldown (1–2 s, doubling to 15–30 s, `Retry-After` honored).

## Sockets and delegated access

Token renewal rotates sockets after verifying that the member and organization remain unchanged. A server advertising `authentication.socket_session_end_codes: ["TOKEN_EXPIRED"]` can explicitly request renewal when a socket token expires. An ambiguous or unadvertised `session_revoked` frame remains terminal. See [reliability](RELIABILITY.md).

Use delegated member sessions for messaging, channel discovery, history and sockets. Administrative operations require a separately authorized administrator session. Supply a known team ID to `channels.list(team_id=...)` when needed. Use `conversations.list()` for direct and group conversations, and [direct messages](DIRECT_MESSAGES.md) for a managed inbox.

## Deployment validation

The package tests use controlled local peers; they do not certify production capacity or a deployment's authorization rules. Before production use, verify login, permitted messaging, denied administration, renewal and recovery on your target deployment. The opt-in [message acceptance procedure](SDK_KEY_MESSAGE_ACCEPTANCE.md) checks sending and receiving.

Key revocation stops future mints; issued tokens may remain usable until expiry. Use a disposable key for revocation tests. Keep keys and tokens out of source, logs, error messages and examples.
