"""Set a reaction on an existing message. Also set HUMCHAT_CHANNEL_ID and HUMCHAT_MESSAGE_ID.

Usage: python react.py ["✅"]
"""

from __future__ import annotations

import asyncio
import sys

from config import client, required


async def main() -> None:
    async with client() as humchat:
        receipt = await humchat.messages.set_reaction(
            channel_id=required("HUMCHAT_CHANNEL_ID"),
            message_id=required("HUMCHAT_MESSAGE_ID"),
            emoji=sys.argv[1] if len(sys.argv) > 1 else "✅",
            present=True,
        )
        print({"operation_id": receipt.operation_id, "reaction_revision": receipt.state.reaction_revision})


if __name__ == "__main__":
    asyncio.run(main())
