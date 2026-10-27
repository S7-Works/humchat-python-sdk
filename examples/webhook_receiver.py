"""Loopback signed-webhook receiver (stdlib ``http.server``).

A 2xx response means the verified event was persisted to the local inbox; apply business
effects from that recoverable inbox with application idempotency. Accepts
``POST /humchat/webhook`` on 127.0.0.1. Put a public HTTPS reverse proxy in front for an
external destination. No user-session token is needed.

Environment: HUMCHAT_ORG_ID, HUMCHAT_WEBHOOK_SUBSCRIPTION_ID, HUMCHAT_CHANNEL_IDS
(comma-separated allowlist), HUMCHAT_WEBHOOK_SECRET, optional HUMCHAT_WEBHOOK_PREVIOUS_SECRET
(rotation), HUMCHAT_WEBHOOK_STATE_DIR, optional HUMCHAT_WEBHOOK_PORT (default 43140) and
HUMCHAT_RUN_FOR_MS. Secrets are never printed.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from webhook_inbox import WebhookInbox

from humchat import WebhookVerificationError, verify_webhook

MAX_BYTES = 8 * 1024 * 1024
MAX_ACTIVE = 8


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def integer(value: str | None, fallback: int, minimum: int, maximum: int) -> int:
    result = fallback if value is None else int(value)
    if not minimum <= result <= maximum:
        raise RuntimeError("Invalid receiver option")
    return result


def log(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


def main() -> int:
    org_id, subscription_id = required("HUMCHAT_ORG_ID"), required("HUMCHAT_WEBHOOK_SUBSCRIPTION_ID")
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", org_id) or org_id in (".", "..") or not re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", subscription_id
    ):
        raise RuntimeError("Invalid receiver scope")
    channels = {value.strip() for value in required("HUMCHAT_CHANNEL_IDS").split(",")}
    if not channels or len(channels) > 50 or any(not value.startswith(org_id + ":") for value in channels):
        raise RuntimeError("Invalid receiver channels")
    secrets = [required("HUMCHAT_WEBHOOK_SECRET")]
    if os.environ.get("HUMCHAT_WEBHOOK_PREVIOUS_SECRET"):
        secrets.append(os.environ["HUMCHAT_WEBHOOK_PREVIOUS_SECRET"])
    if any(not re.fullmatch(r"whsec_[0-9a-f]{64}", secret) for secret in secrets) or len(set(secrets)) != len(secrets):
        raise RuntimeError("Invalid receiver secrets")
    port = integer(os.environ.get("HUMCHAT_WEBHOOK_PORT"), 43140, 0, 65535)
    run_for = integer(os.environ.get("HUMCHAT_RUN_FOR_MS"), 0, 0, 2_147_483_647)
    inbox = WebhookInbox.open(required("HUMCHAT_WEBHOOK_STATE_DIR"), org_id, subscription_id)
    active = threading.BoundedSemaphore(MAX_ACTIVE)
    stopping = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        timeout = 10

        def log_message(self, *_: Any) -> None:  # Never log request lines, headers or bodies.
            return

        def reject(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Connection", "close")
            self.send_header("Content-Length", "0")
            self.end_headers()
            self.close_connection = True

        def do_GET(self) -> None:
            self.reject(405 if self.path == "/humchat/webhook" else 404)

        def do_POST(self) -> None:
            if stopping.is_set():
                return self.reject(503)
            if self.path != "/humchat/webhook":
                return self.reject(404)
            try:
                length = int(self.headers.get("Content-Length", "-1"))
            except ValueError:
                length = -1
            if length < 0 or length > MAX_BYTES:
                return self.reject(413 if length > MAX_BYTES else 411)
            if not active.acquire(blocking=False):
                return self.reject(503)
            try:
                body = self.rfile.read(length)
                envelope = verify_webhook(body, self.headers.get("X-HumChat-Signature", ""), secrets,
                                          expected_org_id=org_id, expected_subscription_id=subscription_id)  # fmt: skip
                if envelope.channel_id not in channels:
                    raise WebhookVerificationError()
                inserted = inbox.accept(envelope)
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                log({"event": "accepted", "event_id": envelope.event_id, "duplicate": not inserted})
            except WebhookVerificationError:
                self.reject(401)
            except Exception:
                self.reject(503)
            finally:
                active.release()

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True

    def stop(*_: Any) -> None:
        if not stopping.is_set():
            stopping.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, stop)
    log({"event": "ready", "host": "127.0.0.1", "port": server.server_address[1], "path": "/humchat/webhook"})
    if run_for:
        threading.Timer(run_for / 1000, stop).start()
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        inbox.close()
        log({"event": "stopped"})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        print("Receiver stopped: CONFIGURATION_OR_INBOX", file=sys.stderr)
        sys.exit(1)
