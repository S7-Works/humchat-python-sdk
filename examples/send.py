"""Send one message. HUMCHAT_CHANNEL_ID is the canonical routing key, including for a DM.

Usage: python send.py ["message text"]
"""

from __future__ import annotations

import asyncio
import sys

from config import client, required


async def main() -> None:
    async with client() as humchat:
        content = sys.argv[1] if len(sys.argv) > 1 else "Hello from the HumChat Python SDK"
        receipt = await humchat.messages.send(channel_id=required("HUMCHAT_CHANNEL_ID"), content=content)
        print({"message_id": receipt.id, "status": receipt.status})


if __name__ == "__main__":
    asyncio.run(main())
