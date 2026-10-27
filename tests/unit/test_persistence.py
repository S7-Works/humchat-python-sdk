"""Port of test/unit/persistence.test.mjs."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, TypeVar

import pytest

from helpers import ROOT, rejects
from humchat import (
    ConflictError,
    FileCheckpointStore,
    FileOutboxStore,
    MemoryCheckpointStore,
    MemoryOutboxStore,
    PersistenceError,
    SendReceipt,
    ValidationError,
)
from humchat.persistence import OutboxEntry

ARTIFACT_ROOT = ROOT / "target" / "persistence"
SCOPE: dict[str, str] = {
    "server_origin": "https://chat.example.invalid",
    "org_id": "org",
    "actor_id": "actor",
    "consumer_id": "consumer",
    "channel_id": "org:team:general",
}
INPUT: dict[str, Any] = {"channel_id": SCOPE["channel_id"], "content": "One stable outgoing intent"}
ULID = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
T = TypeVar("T")


def receipt(entry: OutboxEntry) -> SendReceipt:
    return SendReceipt(
        id=entry.intent.id,
        channel_id=entry.intent.channel_id,
        status="accepted",
        created_at=123,
        idempotency_key=entry.idempotency_key,
        attachment_count=0,
        attachment_manifest_blake3="0" * 64,
    )


def directory(prefix: str = "persistence-") -> Path:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=ARTIFACT_ROOT))


@pytest.fixture
async def cleanup() -> AsyncIterator[Callable[[T], T]]:
    stores: list[Any] = []

    def track(store: T) -> T:
        stores.append(store)
        return store

    yield track
    for store in stores:
        await store.close()


async def test_checkpoint_cas_admits_one_worker_and_all_identity_components_isolate_progress(cleanup: Any) -> None:
    store = cleanup(MemoryCheckpointStore())
    assert await store.load(SCOPE) is None
    results = await asyncio.gather(store.compare_and_set(SCOPE, None, "cursor-a"), store.compare_and_set(SCOPE, None, "cursor-b"))
    assert results == [True, False]
    assert await store.compare_and_set(SCOPE, "stale", "cursor-c") is False
    assert await store.load({**SCOPE, "server_origin": SCOPE["server_origin"] + "/"}) == "cursor-a"
    for other in [
        {**SCOPE, "server_origin": "https://another.example.invalid"},
        {**SCOPE, "actor_id": "another-actor"},
        {**SCOPE, "consumer_id": "another-consumer"},
        {**SCOPE, "channel_id": "org:team:other"},
        {**SCOPE, "org_id": "other-org", "channel_id": "other-org:team:general"},
    ]:
        assert await store.load(other) is None
    await rejects(store.load({**SCOPE, "org_id": "other-org"}), ValidationError)
    await rejects(store.compare_and_set(SCOPE, "cursor-a", ""), ValidationError)
    long_actor = {**SCOPE, "actor_id": "a" * 256}
    assert await store.compare_and_set(long_actor, None, "long-actor-cursor") is True
    assert await store.load(long_actor) == "long-actor-cursor"
    await rejects(store.load({**SCOPE, "actor_id": "a" * 257}), ValidationError)


async def test_file_checkpoint_survives_restart_serializes_cas_and_enforces_one_live_owner(cleanup: Any) -> None:
    path = directory() / "checkpoints.json"
    first = cleanup(await FileCheckpointStore.open(path))
    await rejects(FileCheckpointStore.open(path), PersistenceError)
    results = await asyncio.gather(first.compare_and_set(SCOPE, None, "cursor-1"), first.compare_and_set(SCOPE, None, "cursor-2"))
    assert results == [True, False]
    await first.close()
    await rejects(first.load(SCOPE), PersistenceError)
    reopened = cleanup(await FileCheckpointStore.open(path))
    assert await reopened.load(SCOPE) == "cursor-1"
    assert await reopened.compare_and_set(SCOPE, "cursor-1", "cursor-3") is True
    assert await reopened.compare_and_set(SCOPE, "cursor-1", "cursor-4") is False


async def test_outbox_binds_normalized_intent_and_preserves_generated_ids_across_retries(cleanup: Any) -> None:
    store = cleanup(MemoryOutboxStore())
    mutable = dict(INPUT)
    first = await store.prepare(SCOPE, "job-1", mutable)
    assert ULID.fullmatch(first.intent.id)
    assert first.idempotency_key == first.intent.id
    mutable["content"] = "caller mutated its object"
    assert await store.prepare(SCOPE, "job-1", {**INPUT, "priority": "standard", "thread_root_id": None}) == first
    caller_copy = await store.list_pending(SCOPE)
    caller_copy[0].intent.content = "mutated returned value"
    assert (await store.list_pending(SCOPE))[0].intent.content == INPUT["content"]
    await rejects(store.prepare(SCOPE, "job-1", {**INPUT, "content": "different intent"}), ConflictError)
    await rejects(store.prepare(SCOPE, "job-1", {**INPUT, "thread_root_id": "different-thread"}), ConflictError)
    await rejects(store.prepare(SCOPE, "job-1", INPUT, idempotency_key="different-key"), ConflictError)
    await rejects(store.prepare(SCOPE, "job-2", {**INPUT, "channel_id": "org:team:other"}), ValidationError)
    explicit = await store.prepare(SCOPE, "job-2", {**INPUT, "id": "caller-message-id"}, idempotency_key="caller-idempotency-key")
    assert explicit.intent.id == "caller-message-id"
    assert explicit.idempotency_key == "caller-idempotency-key"


async def test_outbox_completion_rejects_unrelated_receipts_and_keeps_exactly_the_recorded_result(cleanup: Any) -> None:
    store = cleanup(MemoryOutboxStore())
    first = await store.prepare(SCOPE, "job", INPUT)
    for bad in [{"id": "other-message"}, {"channel_id": "org:team:other"}, {"idempotency_key": "other-key"}]:
        await rejects(store.mark_sent(SCOPE, "job", replace(receipt(first), **bad)), ConflictError)
    assert len(await store.list_pending(SCOPE)) == 1
    completed = await store.mark_sent(SCOPE, "job", receipt(first))
    assert completed.status == "sent"
    assert len(await store.list_pending(SCOPE)) == 0
    assert await store.mark_sent(SCOPE, "job", receipt(first)) == completed
    assert await store.prepare(SCOPE, "job", INPUT) == completed
    await rejects(store.mark_sent(SCOPE, "job", replace(receipt(first), created_at=124)), ConflictError)
    await rejects(store.mark_sent({**SCOPE, "actor_id": "different-actor"}, "job", receipt(first)), ConflictError)


async def test_file_outbox_survives_pending_and_completed_restarts_without_minting_new_intent(cleanup: Any) -> None:
    path = directory() / "outbox.json"
    first = cleanup(await FileOutboxStore.open(path))
    prepared = await first.prepare(SCOPE, "job", INPUT)
    await first.close()
    second = cleanup(await FileOutboxStore.open(path))
    assert await second.list_pending(SCOPE) == [prepared]
    assert await second.prepare(SCOPE, "job", INPUT) == prepared
    assert await second.list_pending({**SCOPE, "consumer_id": "different-consumer"}) == []
    await second.mark_sent(SCOPE, "job", receipt(prepared))
    await second.close()
    third = cleanup(await FileOutboxStore.open(path))
    assert await third.list_pending(SCOPE) == []
    assert (await third.prepare(SCOPE, "job", INPUT)).status == "sent"
    assert (await third.prepare(SCOPE, "job", INPUT)).intent.id == prepared.intent.id


async def test_a_failed_atomic_replacement_leaves_checkpoint_memory_and_existing_file_unchanged(cleanup: Any) -> None:
    folder = directory()
    path = folder / "checkpoints.json"
    backup = folder / "saved-checkpoints.json"
    store = cleanup(await FileCheckpointStore.open(path))
    await store.compare_and_set(SCOPE, None, "before")
    contents = path.read_text(encoding="utf-8")
    # Both targets are fixed names inside this test's freshly created artifact directory.
    os.rename(path, backup)
    path.mkdir()
    try:
        await rejects(store.compare_and_set(SCOPE, "before", "after"), PersistenceError)
        assert await store.load(SCOPE) == "before"
        assert backup.read_text(encoding="utf-8") == contents
        assert not any(".tmp-" in name for name in os.listdir(folder))
    finally:
        path.rmdir()
        os.rename(backup, path)
    assert await store.compare_and_set(SCOPE, "before", "recovered") is True


async def test_bounded_persistence_writes_cannot_create_an_unpersisted_pending_outbox_entry(cleanup: Any) -> None:
    path = directory() / "small-outbox.json"
    store = cleanup(await FileOutboxStore.open(path, max_bytes=1024))
    await rejects(store.prepare(SCOPE, "too-large", {**INPUT, "content": "x" * 2000}), PersistenceError)
    assert await store.list_pending(SCOPE) == []
    short = await store.prepare(SCOPE, "small", {**INPUT, "content": "ok"})
    assert short.status == "pending"
    await store.close()
    reopened = cleanup(await FileOutboxStore.open(path, max_bytes=1024))
    assert await reopened.list_pending(SCOPE) == [short]


async def test_corrupt_oversized_wrong_kind_and_identity_tampered_files_fail_closed(cleanup: Any) -> None:
    path = directory() / "checkpoint.json"
    store = cleanup(await FileCheckpointStore.open(path))
    await store.compare_and_set(SCOPE, None, "original")
    await store.close()
    original = path.read_text(encoding="utf-8")
    await rejects(FileOutboxStore.open(path), PersistenceError)
    tampered = json.loads(original)
    next(iter(tampered["records"].values()))["scope"]["actorId"] = "changed-actor"
    path.write_text(json.dumps(tampered), encoding="utf-8")
    await rejects(FileCheckpointStore.open(path), PersistenceError)
    path.write_text("{not JSON", encoding="utf-8")
    await rejects(FileCheckpointStore.open(path), PersistenceError)
    path.write_bytes(bytes([0xFF, 0xFE, 0xFD]))
    await rejects(FileCheckpointStore.open(path), PersistenceError)
    path.write_text(" " * 2048, encoding="utf-8")
    await rejects(FileCheckpointStore.open(path, max_bytes=1024), PersistenceError)
    path.write_text(original, encoding="utf-8")
    recovered = cleanup(await FileCheckpointStore.open(path))
    assert await recovered.load(SCOPE) == "original"


async def test_stale_lock_files_are_never_automatically_expired_or_removed() -> None:
    path = directory() / "checkpoint.json"
    stale = json.dumps({"schema": 1, "owner": "stale-test-owner", "pid": 99999999, "createdAt": "2000-01-01T00:00:00.000Z"})
    Path(str(path) + ".lock").write_text(stale, encoding="utf-8")
    await rejects(FileCheckpointStore.open(path), PersistenceError)
    assert Path(str(path) + ".lock").read_text(encoding="utf-8") == stale


# The child performs real durable writes and keeps its locks with no graceful exit handlers.
CHILD = """
import asyncio, json, sys
from dataclasses import asdict
from humchat import FileCheckpointStore, FileOutboxStore

async def main():
    checkpoint_path, outbox_path, scope_json, input_json = sys.argv[1:5]
    scope = json.loads(scope_json)
    checkpoint = await FileCheckpointStore.open(checkpoint_path)
    outbox = await FileOutboxStore.open(outbox_path)
    await checkpoint.compare_and_set(scope, None, "committed-before-process-crash")
    entry = await outbox.prepare(scope, "crash-job", json.loads(input_json))
    # Retain live lock owners until the forced crash.
    stores = [checkpoint, outbox]
    sys.stdout.write(json.dumps({"kind": "committed", "entry": asdict(entry)}) + "\\n")
    sys.stdout.flush()
    while stores:
        await asyncio.sleep(1)

asyncio.run(main())
"""


def _entry_from(raw: dict[str, Any]) -> OutboxEntry:
    from humchat.persistence import OutboxIntent

    return OutboxEntry(
        key=raw["key"],
        intent=OutboxIntent(**raw["intent"]),
        idempotency_key=raw["idempotency_key"],
        status=raw["status"],
        receipt=None,
    )


async def test_a_killed_process_preserves_committed_checkpoint_and_pending_outbox_intent_behind_stale_locks(cleanup: Any) -> None:
    folder = directory("process-crash-")
    checkpoint_path = folder / "checkpoint.json"
    outbox_path = folder / "outbox.json"
    # A Windows venv python.exe is a redirector that spawns the real interpreter as a second
    # process; run the base interpreter directly so the killed PID is the lock owner.
    interpreter = getattr(sys, "_base_executable", sys.executable)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(path for path in sys.path if path)}
    child = subprocess.Popen(
        [interpreter, "-c", CHILD, str(checkpoint_path), str(outbox_path), json.dumps(SCOPE), json.dumps(INPUT)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        assert child.stdout is not None
        line = await asyncio.wait_for(asyncio.to_thread(child.stdout.readline), 15)
        if not line:
            raise AssertionError("Crash fixture exited before committing: " + child.communicate()[1].decode())
        message = json.loads(line)
        assert message["kind"] == "committed"
        checkpoint_lock = Path(str(checkpoint_path) + ".lock").read_text(encoding="utf-8")
        outbox_lock = Path(str(outbox_path) + ".lock").read_text(encoding="utf-8")
        assert json.loads(checkpoint_lock)["pid"] == child.pid
        assert json.loads(outbox_lock)["pid"] == child.pid
        await rejects(FileCheckpointStore.open(checkpoint_path), PersistenceError)
        child.kill()
        _, stderr = await asyncio.to_thread(child.communicate)
        assert stderr == b""
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    await rejects(FileCheckpointStore.open(checkpoint_path), PersistenceError)
    await rejects(FileOutboxStore.open(outbox_path), PersistenceError)
    assert Path(str(checkpoint_path) + ".lock").read_text(encoding="utf-8") == checkpoint_lock
    assert Path(str(outbox_path) + ".lock").read_text(encoding="utf-8") == outbox_lock
    # Explicit operator recovery only after this exact child process has exited.
    os.unlink(str(checkpoint_path) + ".lock")
    os.unlink(str(outbox_path) + ".lock")
    checkpoint = cleanup(await FileCheckpointStore.open(checkpoint_path))
    outbox = cleanup(await FileOutboxStore.open(outbox_path))
    entry = _entry_from(message["entry"])
    assert await checkpoint.load(SCOPE) == "committed-before-process-crash"
    assert await outbox.list_pending(SCOPE) == [entry]
    assert await outbox.prepare(SCOPE, "crash-job", INPUT) == entry
    assert asdict(await outbox.prepare(SCOPE, "crash-job", INPUT))["idempotency_key"] == entry.idempotency_key
