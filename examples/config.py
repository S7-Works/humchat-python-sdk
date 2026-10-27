"""Shared example configuration, read only from the process environment.

Authentication: set either ``HUMCHAT_TOKEN`` (a user-session token your application obtains)
or ``HUMCHAT_SDK_KEY`` plus ``HUMCHAT_ACT_AS`` (delegated SDK-key login for one member).
Credentials are never printed.
"""

from __future__ import annotations

import os
from typing import Any

from humchat import HumChat, TokenProviderContext, TokenResult


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def authentication() -> dict[str, Any]:
    """``auth=`` for SDK-key mode, otherwise a static ``token_provider=``."""
    if os.environ.get("HUMCHAT_SDK_KEY"):
        if os.environ.get("HUMCHAT_TOKEN"):
            raise RuntimeError("Set either HUMCHAT_TOKEN or HUMCHAT_SDK_KEY, not both")
        return {"auth": {"sdk_key": required("HUMCHAT_SDK_KEY"), "act_as": required("HUMCHAT_ACT_AS")}}

    async def token_provider(_: TokenProviderContext) -> TokenResult:
        # Replace this static provider with your application's renewal flow for unattended use.
        return TokenResult(token=required("HUMCHAT_TOKEN"))

    required("HUMCHAT_TOKEN")
    return {"token_provider": token_provider}


def client(**options: Any) -> HumChat:
    return HumChat(base_url=required("HUMCHAT_BASE_URL"), org_id=required("HUMCHAT_ORG_ID"), **authentication(), **options)
