"""Durable "!status" responder across comma-separated HUMCHAT_CHANNEL_IDS (one socket per channel)."""

from __future__ import annotations

import asyncio
import os
import sys

from config import required
from durable_worker import run_durable_worker

if __name__ == "__main__":
    try:
        channels = required("HUMCHAT_CHANNEL_IDS").split(",")
        sys.exit(asyncio.run(run_durable_worker(channels, os.environ.get("HUMCHAT_CONSUMER_ID", "multi-channel-status-v1"))))
    except RuntimeError:
        print("Worker failed: CONFIGURATION", file=sys.stderr)
        sys.exit(1)
