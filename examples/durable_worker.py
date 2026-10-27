"""Shared durable "!status" worker for durable_responder.py, multi_channel_worker.py and
multiplex_worker.py.

Environment: HUMCHAT_BASE_URL, HUMCHAT_ORG_ID, credentials (see config.py), HUMCHAT_STATE_DIR
(private directory owned by this process), optional HUMCHAT_START (now | earliest) and
HUMCHAT_RUN_FOR_MS. The handler returns only after the reply receipt is persisted in the
outbox; the SDK then advances that input's checkpoint, so processing is at least once and
retries reuse the same outgoing intent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from config import client, required

from humchat import AbortController, AbortSignal, ConsumerContext, DurableEvent, FileCheckpointStore, FileOutboxStore, HumChatError


def log(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


async def run_durable_worker(channel_ids: list[str], consumer_id: str, transport: str = "channel") -> int:
    channels = list(dict.fromkeys(value.strip() for value in channel_ids))
    if not channels or any(not value for value in channels) or len(channels) > 50:
        raise RuntimeError("Supply between 1 and 50 channel IDs")
    start = os.environ.get("HUMCHAT_START", "now")
    if start not in ("now", "earliest"):
        raise RuntimeError("HUMCHAT_START must be now or earliest")
    raw_run_for = os.environ.get("HUMCHAT_RUN_FOR_MS")
    run_for = None if raw_run_for is None else int(raw_run_for)
    if run_for is not None and not 1 <= run_for <= 2_147_483_647:
        raise RuntimeError("Invalid HUMCHAT_RUN_FOR_MS")
    directory = Path(required("HUMCHAT_STATE_DIR"))
    humchat = client(recovery={"mode": "durable"},
                     realtime={"transport": transport, "max_channels": len(channels), "connect_concurrency": min(5, len(channels))})  # fmt: skip
    cancellation = AbortController()
    active: set[asyncio.Task[Any]] = set()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    exit_code = 0
    checkpoints: FileCheckpointStore | None = None
    outbox: FileOutboxStore | None = None

    def stop(*_: Any) -> None:
        loop.call_soon_threadsafe(stopped.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, stop)
    try:
        checkpoints = await FileCheckpointStore.open(directory / "checkpoints.json")
        outbox = await FileOutboxStore.open(directory / "outbox.json")
        identity = await humchat.connect(signal=cancellation.signal)
        for channel_id in channels:
            await humchat.outbox.flush(consumer_id=consumer_id, channel_id=channel_id, store=outbox, signal=cancellation.signal)
        store = outbox

        async def handle(event: DurableEvent, context: ConsumerContext) -> None:
            if event.kind != "message" or event.state.sender_id == identity.actor_id or event.state.body.strip() != "!status":
                return
            combined = AbortSignal.any([context.signal, cancellation.signal])
            key = "status-" + hashlib.sha256(json.dumps([event.org_id, event.channel_id, event.event_id]).encode()).hexdigest()
            receipt = await humchat.outbox.send(
                consumer_id=consumer_id, key=key[:128], store=store, signal=combined,
                message={"channel_id": event.channel_id, "thread_root_id": event.state.thread_root_id or event.state.id,
                         "content": "All systems operational."},
            )  # fmt: skip
            log({"event": "reply", "channel_id": event.channel_id, "message_id": receipt.id})
            # Returning acknowledges this input only after its reply receipt has been persisted.

        async def tracked(event: DurableEvent, context: ConsumerContext) -> None:
            task = asyncio.ensure_future(handle(event, context))
            active.add(task)
            task.add_done_callback(active.discard)
            await task

        def on_error(error: HumChatError) -> None:
            nonlocal exit_code
            print("Subscription error:", error.code, file=sys.stderr)
            if not error.retryable:
                exit_code = 1
                stopped.set()

        subscriptions = []
        for channel_id in channels:
            subscription = humchat.realtime.subscribe(channel_id, start=start)
            subscription.on("error", on_error)
            subscription.consume(tracked, consumer_id=consumer_id, checkpoint_store=checkpoints)
            subscriptions.append(subscription)
        await asyncio.gather(*(subscription.ready for subscription in subscriptions))
        log({"event": "ready", "channels": len(channels), "consumer_id": consumer_id})
        if run_for is not None:
            loop.call_later(run_for / 1000, stopped.set)
        await stopped.wait()
    except Exception as error:
        if not cancellation.signal.aborted:
            print("Worker stopped:", getattr(error, "code", "CONFIGURATION_OR_UNEXPECTED"), file=sys.stderr)
            exit_code = 1
    finally:
        cancellation.abort()
        await humchat.close()
        await asyncio.gather(*active, return_exceptions=True)
        closed = await asyncio.gather(*(store.close() for store in (checkpoints, outbox) if store is not None), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in closed):
            print("Store shutdown failed: PERSISTENCE", file=sys.stderr)
            exit_code = 1
        log({"event": "stopped"})
    return exit_code
