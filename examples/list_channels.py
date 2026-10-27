"""Print workspace channel IDs and display names (optional HUMCHAT_TEAM_ID filter)."""

from __future__ import annotations

import asyncio
import os

from config import client


async def main() -> None:
    async with client() as humchat:
        result = await humchat.channels.list(team_id=os.environ.get("HUMCHAT_TEAM_ID") or None)
        for channel in result.channels:
            print(f"{channel.id}\t{channel.display_name}")


if __name__ == "__main__":
    asyncio.run(main())
