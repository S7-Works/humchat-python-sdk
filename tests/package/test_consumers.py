"""Port of test/package/consumers.test.mjs.

Builds (or receives) the sdist and wheel, installs the exact wheel into a fresh virtual
environment, and checks runtime imports, typed consumers and real loopback requests from
that installation. ``HUMCHAT_SDK_PACKAGE_WHEEL`` / ``HUMCHAT_SDK_PACKAGE_SDIST`` reuse
artifacts from ``scripts/prepare_release.py``; ``HUMCHAT_SDK_ARTIFACTS_DIR`` receives the
consumer evidence. Installing dependencies needs package-index access.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

import webhook_support as webhook
from helpers import LoopbackPeer

ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_TOKEN = "synthetic-installed-example-token"
_spec = importlib.util.spec_from_file_location("release_policy_for_package", ROOT / "scripts" / "release_policy.py")
assert _spec is not None and _spec.loader is not None
policy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(policy)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sanitized(text: str) -> str:
    return text.replace(FIXTURE_TOKEN, "[REDACTED]").replace(webhook.secret, "[REDACTED]")


def run(args: list[str], cwd: Path | None = None, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=600)
    if check:
        assert result.returncode == 0, sanitized(result.stdout + result.stderr)
    return result


@dataclass
class Installed:
    consumer: Path
    python: Path
    wheel: Path
    sdist: Path
    wheel_files: dict[str, bytes]
    sdist_files: dict[str, bytes]
    site_packages: Path
    report: dict[str, Any] = field(default_factory=dict)
    artifacts: Path = ROOT

    def write_report(self) -> None:
        self.artifacts.mkdir(parents=True, exist_ok=True)
        (self.artifacts / "consumer-result.json").write_text(json.dumps(self.report, indent=2), encoding="utf-8")

    def check(self, name: str) -> dict[str, Any]:
        entry = {"name": name, "passed": False}
        self.report["checks"].append(entry)
        self.write_report()
        return entry

    def passed(self, entry: dict[str, Any]) -> None:
        entry["passed"] = True
        self.report["passed"] = all(item["passed"] for item in self.report["checks"])
        self.write_report()


@pytest.fixture(scope="module")
def installed() -> Iterator[Installed]:
    artifacts = Path(os.environ.get("HUMCHAT_SDK_ARTIFACTS_DIR") or ROOT / "target" / "package")
    artifacts.mkdir(parents=True, exist_ok=True)
    consumer = Path(tempfile.mkdtemp(prefix="consumer-", dir=artifacts))
    if os.environ.get("HUMCHAT_SDK_PACKAGE_WHEEL"):
        wheel = Path(os.environ["HUMCHAT_SDK_PACKAGE_WHEEL"]).resolve()
        sdist = Path(os.environ["HUMCHAT_SDK_PACKAGE_SDIST"]).resolve()
    else:
        run([sys.executable, "-m", "build", "--sdist", "--wheel", "--outdir", str(consumer / "dist"), str(ROOT)])
        wheel = next((consumer / "dist").glob("humchat_sdk-*.whl"))
        sdist = next((consumer / "dist").glob("humchat_sdk-*.tar.gz"))
    with zipfile.ZipFile(wheel) as archive:
        wheel_files = {info.filename: archive.read(info) for info in archive.infolist()}
    with tarfile.open(sdist, "r:gz") as archive:
        prefix = archive.getnames()[0].split("/")[0] + "/"
        sdist_files = {}
        for member in archive.getmembers():
            assert member.isfile() and member.name.startswith(prefix), "sdist must contain regular files under one root"
            extracted = archive.extractfile(member)
            assert extracted is not None
            sdist_files[member.name[len(prefix) :]] = extracted.read()
    run([sys.executable, "-m", "venv", str(consumer / "venv")])
    python = consumer / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    run([str(python), "-m", "pip", "install", "--quiet", "--disable-pip-version-check", str(wheel)])
    site = Path(run([str(python), "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"]).stdout.strip())
    version = run([str(python), "-c", "import humchat; print(humchat.__version__)"]).stdout.strip()
    result = Installed(consumer, python, wheel, sdist, wheel_files, sdist_files, site, artifacts=artifacts)
    result.report = {
        "schema": 1, "passed": False, "runtime": {"python": sys.version.split()[0], "platform": sys.platform},
        "wheel": {"path": str(wheel), "sha256": sha256(wheel.read_bytes())}, "sdist": {"path": str(sdist), "sha256": sha256(sdist.read_bytes())},
        "installed_version": version, "files": [{"path": p, "bytes": len(d), "sha256": sha256(d)} for p, d in sorted(wheel_files.items())],
        "checks": [],
    }  # fmt: skip
    result.write_report()
    yield result
    result.write_report()


def test_packed_package_ships_required_files_and_excludes_development_files(installed: Installed) -> None:
    entry = installed.check("packed sdist and wheel contain exactly the allowed public files")
    paths = policy.validate_pack_files([{"path": p, "size": len(d)} for p, d in installed.sdist_files.items()])
    policy.validate_wheel_files([{"path": p, "size": len(d)} for p, d in installed.wheel_files.items()])
    for path, data in installed.sdist_files.items():
        text = data.decode("utf-8")
        policy.inspect_text(path, text)
        if path.endswith(".md"):
            policy.validate_doc_links(path, text, paths)
    everything = [*installed.sdist_files, *installed.wheel_files]
    assert not any(p.split("/")[0] in ("tests", "scripts", "contract", "target", ".venv") for p in everything), "Package must exclude development files"
    assert not any(part == ".env" or part.startswith(".env.") for p in everything for part in p.split("/")), "Package must exclude environment files"
    assert "humchat/py.typed" in installed.wheel_files
    installed.passed(entry)


def test_installed_files_match_the_exact_wheel_and_current_source(installed: Installed) -> None:
    entry = installed.check("installed files match the wheel and current source bytes")
    package_files = {p: d for p, d in installed.wheel_files.items() if p.startswith("humchat/")}
    on_disk = sorted(
        path.relative_to(installed.site_packages).as_posix()
        for path in (installed.site_packages / "humchat").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    )
    assert on_disk == sorted(package_files), "Installed file names must match the exact wheel"
    for path, data in package_files.items():
        assert not (installed.site_packages / path).is_symlink()
        assert (installed.site_packages / path).read_bytes() == data, "Installed content differs from the wheel: " + path
        assert (ROOT / "src" / path).read_bytes() == data, "Wheel content differs from current source: " + path
    metadata = installed.wheel_files[next(p for p in installed.wheel_files if p.endswith("dist-info/METADATA"))].decode()
    assert "Name: humchat-sdk" in metadata
    assert "Classifier: Private :: Do Not Upload" in metadata
    assert "Requires-Python: >=3.10" in metadata
    installed.passed(entry)


RUNTIME = """
import asyncio, json, sys
import httpx
import humchat
from humchat import HumChat, ConflictError, WebhookVerificationError, verify_webhook

missing = [name for name in humchat.__all__ if not hasattr(humchat, name)]
assert not missing, missing
assert isinstance(HumChat, type) and issubclass(ConflictError, humchat.HumChatError)
body, header, secret, org, sent_at = json.loads(sys.argv[1])
verified = verify_webhook(body, header, secret, now=sent_at, expected_org_id=org)
assert verified.event.state.body == "Hello webhook"
try:
    verify_webhook(body + " ", header, secret)
    raise SystemExit("tampered webhook verified")
except WebhookVerificationError:
    pass

async def provider(_):
    return {"token": "fixture-token"}

async def main():
    client = HumChat(org_id="fixture-org", base_url="http://127.0.0.1:43125", token_provider=provider)
    assert client.get_config().org_id == "fixture-org" and client.get_config().authentication == "token_provider"
    await client.close()
    sdk_key = "hsk_" + "synthetic_package_key"
    org_id, email, minted = "abcdefab-2222-4222-8222-222222222222", "synthetic.package.actor@example.invalid", "synthetic-package-minted"
    calls = []
    def handle(request):
        calls.append(request.url.path)
        assert str(request.url).startswith("http://127.0.0.1:43125/") and request.url.query == b""
        if request.url.path == "/api/sdk/auth/user-token":
            assert request.method == "POST" and request.headers["x-humchat-sdk-key"] == sdk_key and "authorization" not in request.headers
            assert json.loads(request.content) == {"org_uuid": org_id, "email": email, "expires_in": 900}
            return httpx.Response(200, json={"access_token": minted, "token_type": "Bearer", "expires_in": 900})
        assert "x-humchat-sdk-key" not in request.headers and request.headers["authorization"] == "Bearer " + minted
        if request.url.path == "/me":
            return httpx.Response(200, json={"user_uuid": "sdk-key-fixture-actor", "org": org_id, "email": email, "roles": ["member"]})
        return httpx.Response(200, json={"sdk_protocol_version": 1, "authentication": {"user_sessions": True, "service_identities": False,
            "api_key_exchange": False, "app_binding_required": False, "mock_authentication": False,
            "sdk_key_user_tokens": {"endpoint": "/api/sdk/auth/user-token", "key_header": "X-HumChat-Sdk-Key", "configured": True}},
            "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
            "durable_replay": False, "websocket_scope": "channel"}})
    keyed = HumChat(org_id=org_id.upper(), base_url="http://127.0.0.1:43125", auth={"sdk_key": sdk_key, "act_as": "  " + email.upper() + "  "},
                    transport=httpx.MockTransport(handle))
    assert keyed.get_config().authentication == "sdk_key" and sdk_key not in repr(keyed)
    assert (await keyed.connect()).actor_id == "sdk-key-fixture-actor"
    assert calls == ["/api/sdk/auth/user-token", "/me", "/api/sdk/capabilities"]
    await keyed.close()

asyncio.run(main())
print("runtime-ok")
"""


def test_runtime_resolves_every_public_export_and_verifies_installed_behaviour(installed: Installed) -> None:
    entry = installed.check("installed runtime resolves every public export")
    raw = webhook.dumps(webhook.envelope())
    arguments = json.dumps([raw, webhook.signature(raw), webhook.secret, webhook.org, webhook.timestamp])
    script = installed.consumer / "runtime_consumer.py"
    script.write_text(RUNTIME, encoding="utf-8")
    result = run([str(installed.python), "-I", str(script), arguments], cwd=installed.consumer)
    assert result.stdout.strip() == "runtime-ok"
    installed.passed(entry)


VALID_TYPES = """
from __future__ import annotations
import asyncio
from humchat import (EventCursor, HumChat, HumChatError, MemoryCheckpointStore, MessageEvent, SendReceipt,
                     TokenProviderContext, TokenResult, verify_webhook)

async def provider(context: TokenProviderContext) -> TokenResult:
    assert context.reason in ("initial", "expiring", "unauthorized")
    return TokenResult(token="fixture-token", expires_at=None)

async def main() -> None:
    client = HumChat(org_id="fixture-org", base_url="http://127.0.0.1:43125", token_provider=provider, timeout_ms=1000,
                     retry={"max_retries": 1}, realtime={"transport": "channel"}, recovery={"mode": "durable"})
    receipt: SendReceipt = await client.messages.send(channel_id="fixture-org:team:general", content="hello", priority="urgent")
    print(receipt.idempotency_key)
    subscription = client.realtime.subscribe("fixture-org:team:general", start=EventCursor("cursor"))
    def on_message(event: MessageEvent) -> None:
        print(event.message.content)
    subscription.on("message", on_message)
    store = MemoryCheckpointStore()
    subscription.consume(lambda event, context: None, consumer_id="worker", checkpoint_store=store)
    try:
        await subscription.ready
    except HumChatError as error:
        print(error.code, error.retryable)
    envelope = verify_webhook(b"{}", "t=1,v1=" + "0" * 64, ["whsec_" + "0" * 64], tolerance_seconds=10)
    print(envelope.event.state.body)
    await client.close()

asyncio.run(main())
"""

INVALID_TYPES = """
from __future__ import annotations
from humchat import HumChat, SendReceipt

async def main(client: HumChat) -> None:
    await client.messages.send(channel_id="x", content=1)
    await client.messages.send(channel_id="x", content="x", priority="loud")
    receipt: SendReceipt = await client.connect()
    client.realtime.subscribe("x", unknown=True)
"""


def test_type_consumers_accept_valid_calls_and_reject_invalid_calls(installed: Installed) -> None:
    entry = installed.check("typed consumers accept valid calls and reject invalid calls")
    valid, invalid = installed.consumer / "typed_valid.py", installed.consumer / "typed_invalid.py"
    valid.write_text(VALID_TYPES, encoding="utf-8")
    invalid.write_text(INVALID_TYPES, encoding="utf-8")
    base = [sys.executable, "-m", "mypy", "--strict", "--no-incremental", "--python-executable", str(installed.python), "--config-file", os.devnull]
    passed = run([*base, str(valid)], cwd=installed.consumer, check=False)
    assert passed.returncode == 0, passed.stdout + passed.stderr
    failed = run([*base, str(invalid)], cwd=installed.consumer, check=False)
    assert failed.returncode == 1, failed.stdout
    for line in (6, 7, 8, 9):
        assert f"typed_invalid.py:{line}: error" in failed.stdout, failed.stdout
    installed.passed(entry)


LIVE = """
import asyncio, os
from humchat import HumChat

async def provider(_):
    return {"token": os.environ["HUMCHAT_TOKEN"]}

async def main():
    async with HumChat(org_id=os.environ["HUMCHAT_ORG_ID"], base_url=os.environ["HUMCHAT_BASE_URL"], token_provider=provider,
                       recovery={"mode": "best_effort"}) as client:
        channel = os.environ["HUMCHAT_CHANNEL_ID"]
        listed = await client.channels.list(team_id=os.environ["HUMCHAT_TEAM_ID"])
        assert [c.id for c in listed.channels][0] == channel
        receipt = await client.messages.send(channel_id=channel, content="hello from the installed package", id="installed-send")
        reaction = await client.messages.set_reaction(channel_id=channel, message_id=receipt.id, emoji="\\u2705", present=True)
        assert reaction.state.reactions == {"\\u2705": ["package-bot"]}
        sub = client.realtime.subscribe(channel)
        seen = asyncio.get_running_loop().create_future()
        sub.on("message", lambda event: seen.done() or seen.set_result(event.message.id))
        await asyncio.wait_for(sub.ready, 10)
        print("ready", flush=True)
        print("received", await asyncio.wait_for(seen, 10), flush=True)

asyncio.run(main())
"""


async def test_installed_package_performs_real_local_list_send_reaction_and_realtime_requests(installed: Installed) -> None:
    entry = installed.check("installed package performs real local list, send, reaction and realtime requests")
    org, actor, team = "package-org", "package-bot", "team"
    channel = f"{org}:{team}:general"
    sends: list[Any] = []

    async def http(request: Any) -> Any:
        assert request.headers.get("authorization") == "Bearer " + FIXTURE_TOKEN
        path = request.path
        if path == "/me":
            return 200, {"user_uuid": actor, "org": org, "email": "package@example.invalid", "roles": ["member"], "scopes": []}
        if path == "/api/sdk/capabilities":
            return 200, {"sdk_protocol_version": 1, "authentication": {"user_sessions": True, "service_identities": False, "api_key_exchange": False,
                         "app_binding_required": False, "mock_authentication": True},
                         "messaging": {"send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
                                       "durable_replay": False, "websocket_scope": "channel"}}  # fmt: skip
        if path == "/api/workspace/channels":
            assert request.query.get("team_uuid") == team
            return 200, {"org_uuid": org, "team_uuid": team, "team_name": "Package team", "channels": [
                {"channel_id": channel, "org_uuid": org, "team_uuid": team, "slug": "general", "display_name": "General", "is_private": False}]}  # fmt: skip
        if path == "/api/messages/sync":
            return 200, {"schema": 2, "org_uuid": org, "channel_id": channel, "thread_root_id": request.query.get("thread_root_id"),
                         "messages": [], "next_cursor": None}  # fmt: skip
        body = await request.json()
        if path == "/api/messages/send":
            sends.append(body)
            assert request.headers.get("x-idempotency-key") == body["id"]
            return 200, {"status": "ACK", "message_id": body["id"], "created_at": 1_792_370_346, "attachment_count": 0,
                         "attachment_manifest_blake3": "0" * 64}  # fmt: skip
        if path == "/api/messages/mutate":
            assert body["mutation"] == {"kind": "set_reaction", "emoji": "✅", "present": True}
            state = {"schema": 1, "org_uuid": org, "channel_id": channel, "message_id": body["message_id"], "sender_id": actor,
                     "created_at": 1_792_370_346, "thread_root_id": None, "priority": "standard", "original_body": "x", "body": "x",
                     "revision": 1, "updated_at": 1_792_370_346, "deleted": False, "reaction_revision": 1, "reactions": {"✅": [actor]},
                     "attachments": []}  # fmt: skip
            return 200, {"schema": 1, "operation_id": body["operation_id"], "actor_id": actor, "request_digest": "0" * 64, "state": state}
        return 404, {}

    peer = await LoopbackPeer(http).start()
    script = installed.consumer / "live_consumer.py"
    script.write_text(LIVE, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k.upper() in ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "PROGRAMDATA", "TEMP", "TMP", "HOME", "USERPROFILE")}
    env.update(HUMCHAT_BASE_URL=peer.base_url, HUMCHAT_ORG_ID=org, HUMCHAT_TOKEN=FIXTURE_TOKEN, HUMCHAT_CHANNEL_ID=channel, HUMCHAT_TEAM_ID=team)
    child = await asyncio.create_subprocess_exec(
        str(installed.python), "-I", str(script), env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        assert child.stdout is not None
        line = await asyncio.wait_for(child.stdout.readline(), 30)
        assert line.decode().strip() == "ready", line
        socket = peer.latest("/ws/chat")
        assert socket is not None
        await socket.send({"id": "installed-live", "org_uuid": org, "channel_id": channel, "sender_id": "package-user", "created_at": 1_792_370_346,
                           "thread_root_id": None, "priority": "standard", "content": "live", "is_long": False})  # fmt: skip
        stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
        output = line.decode() + stdout.decode() + stderr.decode()
        assert child.returncode == 0, sanitized(output)
        assert FIXTURE_TOKEN not in output, "Consumer logged its bearer credential"
        assert "received installed-live" in output
        assert [send["id"] for send in sends] == ["installed-send"]
        assert peer.upgrades[0].headers.get("authorization") == "Bearer " + FIXTURE_TOKEN and "token" not in peer.upgrades[0].query
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        await peer.close()
    installed.passed(entry)


def test_shipped_examples_compile_against_the_installed_package(installed: Installed) -> None:
    entry = installed.check("shipped examples compile against the installed package")
    examples = {p: d for p, d in installed.sdist_files.items() if p.startswith("examples/") and p.endswith(".py")}
    assert examples, "The sdist must ship runnable examples"
    folder = installed.consumer / "examples"
    folder.mkdir(exist_ok=True)
    for path, data in examples.items():
        (installed.consumer / path).write_bytes(data)
    run([str(installed.python), "-I", "-m", "compileall", "-q", str(folder)])
    installed.passed(entry)
