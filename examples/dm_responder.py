"""Reply "All systems operational." to "!status" in any DM or group conversation.

Environment: HUMCHAT_ORG_ID, HUMCHAT_BASE_URL, HUMCHAT_STATE_DIR, either HUMCHAT_TOKEN or
HUMCHAT_SDK_KEY + HUMCHAT_ACT_AS, optional
HUMCHAT_CONSUMER_ID, HUMCHAT_RECIPIENT_ID (send a one-time greeting) and HUMCHAT_RUN_FOR_MS.
Checkpoints and replies are persisted, so a restart never answers the same command twice.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal as signals
import sys
from pathlib import Path
from typing import Any

from humchat import AbortController, AbortSignal, ConsumerContext, DurableEvent, FileCheckpointStore, FileOutboxStore, HumChat


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def log(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


async def main() -> int:
    async def provider(_: Any) -> dict[str, str]:
        # Replace this static provider with your application's session-renewal implementation.
        return {"token": required("HUMCHAT_TOKEN")}

    exit_code = 0
    stop = AbortController()
    stopped = asyncio.get_running_loop().create_future()
    stop.signal.add_listener(lambda _: stopped.done() or stopped.set_result(None))
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signals, name), stop.abort)
        except (NotImplementedError, AttributeError, RuntimeError):
            pass  # Windows event loops do not support signal handlers; Ctrl+C still raises.
    active: set[asyncio.Task[Any]] = set()
    checkpoints = outbox = None
    timer = None
    client = None
    try:
        if os.environ.get("HUMCHAT_SDK_KEY"):
            # Delegated SDK-key login: the key never leaves this backend process.
            auth = {"sdk_key": required("HUMCHAT_SDK_KEY"), "act_as": required("HUMCHAT_ACT_AS")}
            client = HumChat(org_id=required("HUMCHAT_ORG_ID"), base_url=required("HUMCHAT_BASE_URL"), auth=auth)
        else:
            client = HumChat(org_id=required("HUMCHAT_ORG_ID"), base_url=required("HUMCHAT_BASE_URL"), token_provider=provider)
        raw_run_for = os.environ.get("HUMCHAT_RUN_FOR_MS")
        run_for = None if raw_run_for is None else int(raw_run_for)
        if run_for is not None and not 1 <= run_for <= 2_147_483_647:
            raise ValueError("Invalid run duration")
        directory = Path(required("HUMCHAT_STATE_DIR"))
        checkpoints = await FileCheckpointStore.open(directory / "dm-checkpoints.json")
        outbox = await FileOutboxStore.open(directory / "dm-outbox.json")
        identity = await client.connect(signal=stop.signal)
        consumer_id = os.environ.get("HUMCHAT_CONSUMER_ID", "dm-status-responder-v1")
        recipient_id = os.environ.get("HUMCHAT_RECIPIENT_ID")
        if recipient_id:
            conversation = await client.conversations.open_direct(member_id=recipient_id, signal=stop.signal)
            # A persistent key prevents this optional greeting from being repeated on every restart.
            await client.outbox.send(
                consumer_id=consumer_id,
                key="initial-greeting",
                store=outbox,
                message={"channel_id": conversation.channel_id, "content": "Send !status here for a status reply."},
                signal=stop.signal,
            )
        stop.signal.throw_if_aborted()
        store = outbox

        async def respond(event: DurableEvent, context: ConsumerContext) -> None:
            assert client is not None
            if event.kind != "message" or event.state.sender_id == identity.actor_id or event.state.body.strip() != "!status":
                return
            key = "reply-" + hashlib.sha256(json.dumps([event.org_id, event.channel_id, event.event_id], separators=(",", ":")).encode()).hexdigest()
            receipt = await client.outbox.send(
                consumer_id=consumer_id,
                key=key,
                store=store,
                message={
                    "channel_id": event.channel_id,
                    "content": "All systems operational.",
                    "thread_root_id": event.state.thread_root_id or event.state.id,
                },
                signal=AbortSignal.any([context.signal, stop.signal]),
            )
            log({"event": "reply", "channelId": event.channel_id, "messageId": receipt.id})

        async def handler(event: DurableEvent, context: ConsumerContext) -> None:
            task = asyncio.ensure_future(respond(event, context))
            active.add(task)
            task.add_done_callback(active.discard)
            await asyncio.shield(task)

        watcher = client.inbox.watch(handler, consumer_id=consumer_id, checkpoint_store=checkpoints)
        await asyncio.wait([watcher.ready, stopped], return_when=asyncio.FIRST_COMPLETED)
        if not stop.signal.aborted:
            await watcher.ready
            log({"event": "watching", "conversations": watcher.get_stats().conversations})
            if run_for is not None:
                timer = loop.call_later(run_for / 1000, stop.abort)
            done = watcher.done
            await asyncio.wait([done, stopped], return_when=asyncio.FIRST_COMPLETED)
            if done.done() and not stop.signal.aborted:
                done.result()
    except Exception as error:
        if not stop.signal.aborted:
            print("Inbox stopped:", getattr(error, "code", "CONFIGURATION_OR_UNEXPECTED"), file=sys.stderr)
            exit_code = 1
    finally:
        stop.abort()
        if timer is not None:
            timer.cancel()
        if client is not None:
            await client.close()
        await asyncio.gather(*active, return_exceptions=True)
        closed = await asyncio.gather(*(store.close() for store in (checkpoints, outbox) if store is not None), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in closed):
            print("Store shutdown failed: PERSISTENCE", file=sys.stderr)
            exit_code = 1
        log({"event": "stopped"})
    return exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
