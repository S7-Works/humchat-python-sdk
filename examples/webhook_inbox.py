"""Small single-process webhook inbox. Replace with an application database or queue at scale.

The inbox file is replaced atomically; a sibling ``.lock`` file enforces one owner and is
never expired automatically (inspect and remove it manually after a crash).
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

from humchat import WebhookEnvelope

LIMIT = 64 * 1024 * 1024
MAX_RECORDS = 10_000


class WebhookInboxError(Exception):
    code = "WEBHOOK_INBOX"

    def __init__(self) -> None:
        super().__init__("Webhook inbox unavailable")


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


class WebhookInbox:
    def __init__(self, path: Path, lock_fd: int, owner: str, document: dict[str, Any]) -> None:
        self.path, self._lock_fd, self._owner, self.document = path, lock_fd, owner, document
        self._mutex = threading.Lock()
        self._closed = False

    @classmethod
    def open(cls, directory: str | os.PathLike[str], org_id: str, subscription_id: str) -> WebhookInbox:
        root = Path(directory).resolve()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        path, owner = root / "inbox.json", str(uuid.uuid4())
        try:
            lock_fd = os.open(str(path) + ".lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except OSError:
            raise WebhookInboxError from None  # Stale locks require manual inspection.
        try:
            os.write(lock_fd, json.dumps({"owner": owner, "pid": os.getpid()}).encode())
            os.fsync(lock_fd)
            document: Any = {"schema": 1, "org_id": org_id, "subscription_id": subscription_id, "records": {}}
            if path.exists():
                if path.stat().st_size > LIMIT:
                    raise WebhookInboxError
                document = json.loads(path.read_bytes().decode("utf-8"))
            records = document.get("records") if isinstance(document, dict) else None
            if (
                not isinstance(records, dict) or document.get("schema") != 1 or document.get("org_id") != org_id
                or document.get("subscription_id") != subscription_id or len(records) > MAX_RECORDS
            ):  # fmt: skip
                raise WebhookInboxError
            for key, row in records.items():
                scope = row.get("scope") if isinstance(row, dict) else None
                event = row.get("event") if isinstance(row, dict) else None
                if (
                    not isinstance(scope, list) or len(scope) != 4 or scope[0] != org_id or scope[1] != subscription_id
                    or not isinstance(scope[2], str) or not scope[2].startswith(org_id + ":") or not isinstance(event, dict)
                    or scope[3] != event.get("event_id") or event.get("org_id") != org_id or event.get("channel_id") != scope[2]
                    or digest(scope) != key or digest(event) != row.get("event_digest")
                ):  # fmt: skip
                    raise WebhookInboxError
            return cls(path, lock_fd, owner, document)
        except Exception:
            os.close(lock_fd)
            os.unlink(str(path) + ".lock")
            raise WebhookInboxError from None

    def accept(self, envelope: WebhookEnvelope) -> bool:
        """Persist a verified envelope once. Returns False for a duplicate delivery."""
        with self._mutex:
            if self._closed:
                raise WebhookInboxError
            if envelope.org_id != self.document["org_id"] or envelope.subscription_id != self.document["subscription_id"]:
                raise WebhookInboxError
            event = asdict(envelope.event)
            scope = [envelope.org_id, envelope.subscription_id, envelope.channel_id, envelope.event_id]
            key, event_digest = digest(scope), digest(event)
            prior = self.document["records"].get(key)
            if prior is not None:
                if prior["event_digest"] != event_digest:
                    raise WebhookInboxError
                return False
            if len(self.document["records"]) >= MAX_RECORDS:
                raise WebhookInboxError
            following = copy.deepcopy(self.document)
            following["records"][key] = {"scope": scope, "event_digest": event_digest, "event": event, "received_at": int(time.time())}
            contents = json.dumps(following).encode()
            if len(contents) > LIMIT:
                raise WebhookInboxError
            temporary = Path(f"{self.path}.tmp-{uuid.uuid4()}")
            try:
                with open(temporary, "xb") as handle:
                    handle.write(contents)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
            except OSError:
                temporary.unlink(missing_ok=True)
                raise WebhookInboxError from None
            self.document = following
            return True

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            os.close(self._lock_fd)
            lock = Path(str(self.path) + ".lock")
            if json.loads(lock.read_text(encoding="utf-8")).get("owner") != self._owner:
                raise WebhookInboxError
            lock.unlink()
