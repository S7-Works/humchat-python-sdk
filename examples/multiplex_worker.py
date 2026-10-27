"""Durable "!status" responder over one shared socket; requires server multiplexed-events v1."""

from __future__ import annotations

import asyncio
import os
import sys

from config import required
from durable_worker import run_durable_worker

if __name__ == "__main__":
    try:
        channels = required("HUMCHAT_CHANNEL_IDS").split(",")
        consumer = os.environ.get("HUMCHAT_CONSUMER_ID", "multiplex-status-v1")
        sys.exit(asyncio.run(run_durable_worker(channels, consumer, transport="multiplex")))
    except RuntimeError:
        print("Worker stopped: CONFIGURATION", file=sys.stderr)
        sys.exit(1)
