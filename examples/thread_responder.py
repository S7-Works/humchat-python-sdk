"""Best-effort live responder for older servers: reply to "!status" in its thread.

Set HUMCHAT_BASE_URL, HUMCHAT_ORG_ID, credentials (see config.py) and HUMCHAT_CHANNEL_ID.
Commands sent while this process is down can be missed; use durable_responder.py when the
server supports durable events.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import sys
from typing import Any

from config import client, required

from humchat import MessageEvent


async def main() -> int:
    humchat = client(recovery={"mode": "best_effort"})
    loop = asyncio.get_running_loop()
    stopped = asyncio.Event()

    def stop(*_: Any) -> None:
        loop.call_soon_threadsafe(stopped.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, stop)
    subscription = None
    try:
        identity = await humchat.connect()
        subscription = humchat.realtime.subscribe(required("HUMCHAT_CHANNEL_ID"))
        subscription.on("error", lambda error: print("Subscription failed:", error.code, file=sys.stderr))
        subscription.on("handler_error", lambda _: print("A message handler failed", file=sys.stderr))
        subscription.on("recovery_incomplete", lambda _: print("Recovery is incomplete; commands sent during a gap may be missed.", file=sys.stderr))

        def revoked(_: Any) -> None:
            print("Session revoked; stopping.", file=sys.stderr)
            stopped.set()

        subscription.on("session_revoked", revoked)

        async def on_message(event: MessageEvent) -> None:
            message = event.message
            if message.sender_id == identity.actor_id or event.source != "live" or event.replayed:
                return
            body = (await humchat.messages.get_state(channel_id=message.channel_id, message_id=message.id)).body if message.is_long else message.content
            if body.strip() != "!status":
                return
            # A stable intent avoids a duplicate reply if the same command is observed again. It
            # does not provide a durable inbox or guarantee commands received during downtime.
            key = "status-" + hashlib.sha256(json.dumps([message.org_id, message.channel_id, message.id]).encode()).hexdigest()
            await humchat.messages.reply(message, id=key[:128], content="All systems operational.", idempotency_key=key[:128])

        subscription.on("message", on_message)
        await subscription.ready
        print("Listening for !status commands in the selected channel.", flush=True)
        await stopped.wait()
        return 0
    except Exception as error:
        print("Responder stopped:", getattr(error, "code", "UNEXPECTED"), file=sys.stderr)
        return 1
    finally:
        if subscription is not None:
            await subscription.close()
        await humchat.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
