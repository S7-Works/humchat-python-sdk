"""Self-contained backend smoke example (the Node SDK's CommonJS example equivalent).

Prints a JSON channel list. Sending requires HUMCHAT_SEND_MESSAGE to be set explicitly.
Uses HUMCHAT_TOKEN only, so it does not depend on config.py.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from humchat import HumChat, TokenProviderContext, TokenResult


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(name + " is required")
    return value


async def provider(_: TokenProviderContext) -> TokenResult:
    return TokenResult(token=required("HUMCHAT_TOKEN"))


async def main() -> None:
    client = HumChat(base_url=required("HUMCHAT_BASE_URL"), org_id=required("HUMCHAT_ORG_ID"), token_provider=provider)
    try:
        result = await client.channels.list(team_id=os.environ.get("HUMCHAT_TEAM_ID") or None)
        channels = [{"id": channel.id, "display_name": channel.display_name} for channel in result.channels]
        print(json.dumps({"event": "channels", "channels": channels}))
        if os.environ.get("HUMCHAT_SEND_MESSAGE") is not None:
            receipt = await client.messages.send(channel_id=required("HUMCHAT_CHANNEL_ID"), content=required("HUMCHAT_SEND_MESSAGE"))
            print(json.dumps({"event": "sent", "message_id": receipt.id, "status": receipt.status}))
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print("Example failed:", getattr(error, "code", "CONFIGURATION_OR_UNEXPECTED"), file=sys.stderr)
        sys.exit(1)
