"""Durable single-channel "!status" responder (HUMCHAT_CHANNEL_ID). See durable_worker.py."""

from __future__ import annotations

import asyncio
import os
import sys

from config import required
from durable_worker import run_durable_worker

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(run_durable_worker([required("HUMCHAT_CHANNEL_ID")], os.environ.get("HUMCHAT_CONSUMER_ID", "status-responder-v1"))))
    except RuntimeError:
        print("Worker failed: CONFIGURATION", file=sys.stderr)
        sys.exit(1)
