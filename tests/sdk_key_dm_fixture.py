"""Port of test/fixtures/sdk-key-direct-messaging.mjs.

A synthetic HTTP/WebSocket protocol peer, not a running HumChat server or browser app.
State is retained by the peer across SDK restarts. Every credential is synthetic.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import inspect
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
from aiohttp import WSMsgType, web

from humchat import HumChat, PersistenceScope

ORG = "00000000-0000-4000-8000-000000000061"
SDK_KEY = "hsk_synthetic_dm_fixture_only"


@dataclass(frozen=True)
class Member:
    id: str
    email: str
    name: str


MEMBERS = [Member(f"synthetic-dm-{name}", f"{name}@example.invalid", name) for name in ("alice", "bob", "carol")]
ALICE, BOB, CAROL = MEMBERS
EPOCH = "synthetic-direct-messaging-epoch"
CONSUMER_ID = "native-sdk-direct-messaging"


def channel(identifier: str) -> str:
    return f"{ORG}:conv:{identifier}"


def cursor(channel_id: str, sequence: int | str) -> str:
    raw = json.dumps({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": str(sequence)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()


def position(value: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))["sequence"])


async def until(condition: Callable[[], Any], label: str, timeout: float = 6.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"Timed out: {label}")


@dataclass
class _Socket:
    member: Member
    ws: web.WebSocketResponse
    subscriptions: dict[str, str] = field(default_factory=dict)


class DmPeer:
    def __init__(self, transport: str, route: Callable[..., Any] | None = None, event: Callable[[dict[str, Any]], None] | None = None) -> None:
        self.transport = transport
        self.route = route
        self.event_hook = event
        self.conversations: dict[str, dict[str, Any]] = {}
        self.streams: dict[str, list[dict[str, Any]]] = {}
        self.states: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, Member] = {}
        self.sends: dict[str, dict[str, Any]] = {}
        self.mutations: dict[str, dict[str, Any]] = {}
        self.requests: list[dict[str, Any]] = []
        self.mints: list[str] = []
        self.sockets: list[_Socket] = []
        self.clients: list[HumChat] = []
        self.errors: list[BaseException] = []
        self.timestamp = 1_792_370_000
        self.origin = ""
        self._runner: web.AppRunner | None = None
        self.capabilities = {
            "sdk_protocol_version": 1,
            "authentication": {
                "user_sessions": True, "service_identities": False, "api_key_exchange": False,
                "app_binding_required": False, "mock_authentication": True,
                "sdk_key_user_tokens": {"endpoint": "/api/sdk/auth/user-token", "key_header": "X-HumChat-Sdk-Key", "configured": True},
            },
            "messaging": {
                "send_receipt_versions": [1], "message_state_versions": [1], "history_versions": [2],
                "durable_replay": True, "event_protocol_versions": [1], "websocket_scope": "channel",
                "multiplexed_events": {"protocol_versions": [1], "max_channels": 100},
            },
        }  # fmt: skip

    # --- state -----------------------------------------------------------------------------

    def stream(self, channel_id: str | None) -> list[dict[str, Any]]:
        return self.streams.get(channel_id or "", [])

    def permitted(self, member: Member, channel_id: str | None) -> bool:
        return any(row["channel_key"] == channel_id and any(m["user_uuid"] == member.id for m in row["members"]) for row in self.conversations.values())

    async def publish(self, event: dict[str, Any]) -> None:
        for entry in list(self.sockets):
            if entry.ws.closed or not self.permitted(entry.member, event["channel_id"]):
                continue
            for subscription_id, channel_id in list(entry.subscriptions.items()):
                if channel_id == event["channel_id"]:
                    frame = {**event, **({"subscription_id": subscription_id} if subscription_id else {})}
                    try:
                        await entry.ws.send_str(json.dumps(frame))
                    except (ConnectionError, RuntimeError):
                        pass

    async def append(self, state: dict[str, Any], kind: str, actor_id: str) -> dict[str, Any]:
        rows = self.streams.setdefault(state["channel_id"], [])
        sequence = str(len(rows) + 1)
        event = {"type": "event", "schema": 1, "org_uuid": ORG, "channel_id": state["channel_id"], "epoch": EPOCH, "sequence": sequence,
                 "cursor": cursor(state["channel_id"], sequence), "event_id": f"{EPOCH}:{sequence}", "actor_id": actor_id,
                 "kind": kind, "state": copy.deepcopy(state)}  # fmt: skip
        if self.event_hook is not None:
            self.event_hook(event)
        rows.append(event)
        await self.publish(event)
        return event

    # --- lifecycle -------------------------------------------------------------------------

    async def start(self) -> DmPeer:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app, handle_signals=False, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        self.origin = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        return self

    async def close(self) -> None:
        await asyncio.gather(*(client.close() for client in self.clients), return_exceptions=True)
        for entry in self.sockets:
            await entry.ws.close()
        if self._runner is not None:
            await self._runner.cleanup()
        assert self.errors == [], f"Protocol-peer assertion failures must not masquerade as SDK failures: {self.errors!r}"

    def scope(self, member: Member, channel_id: str) -> PersistenceScope:
        return PersistenceScope(server_origin=self.origin, org_id=ORG, actor_id=member.id, consumer_id=CONSUMER_ID, channel_id=channel_id)

    def client(self, member: Member) -> HumChat:
        client = HumChat(
            org_id=ORG, base_url=self.origin, auth={"sdk_key": SDK_KEY, "act_as": member.email}, timeout_ms=1000,
            retry={"max_retries": 0, "total_timeout_ms": 3000},
            realtime={"transport": self.transport, "max_channels": 5, "connect_concurrency": 2, "handler_timeout_ms": 2000,  # type: ignore[typeddict-item]
                      "reconnect_initial_delay_ms": 5, "reconnect_max_delay_ms": 20, "ping_interval_ms": 1000, "pong_timeout_ms": 500},
        )  # fmt: skip
        self.clients.append(client)
        return client

    async def app_read(self, member: Member, channel_id: str, thread_root_id: str | None = None) -> tuple[int, Any]:
        """Independent app-like REST consumer: a normal user token, no SDK instance."""
        token = f"synthetic-app-session-{member.name}"
        self.tokens[token] = member
        params = {"channel_id": channel_id, **({"thread_root_id": thread_root_id} if thread_root_id else {})}
        async with httpx.AsyncClient() as http:
            response = await http.get(f"{self.origin}/api/messages/sync", params=params,
                                      headers={"Authorization": f"Bearer {token}", "X-Zlack-Message-State": "v2"})  # fmt: skip
        return response.status_code, response.json()

    async def app_send(self, member: Member, channel_id: str, identifier: str, content: str) -> tuple[int, Any]:
        token = f"synthetic-app-session-{member.name}"
        self.tokens[token] = member
        async with httpx.AsyncClient() as http:
            response = await http.post(
                f"{self.origin}/api/messages/send",
                headers={"Authorization": f"Bearer {token}", "X-Idempotency-Key": identifier, "X-Zlack-Message-Receipt": "v1"},
                json={"id": identifier, "channel_id": channel_id, "content": content, "thread_root_id": None, "priority": "standard"},
            )
        return response.status_code, response.json()

    # --- HTTP + WebSocket ------------------------------------------------------------------

    def _member(self, request: web.Request) -> Member | None:
        auth = request.headers.get("authorization", "")
        return self.tokens.get(auth[len("Bearer ") :] if auth.startswith("Bearer ") else auth)

    async def _dispatch(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await self._upgrade(request)
        try:
            return await self._http(request)
        except Exception as error:
            self.errors.append(error)
            return web.json_response({}, status=500)

    async def _http(self, request: web.Request) -> web.StreamResponse:
        path, query = request.path, request.query
        entry: dict[str, Any] = {"path": path, "method": request.method, "query": dict(query), "headers": dict(request.headers)}
        self.requests.append(entry)
        if path == "/api/sdk/auth/user-token":
            assert request.method == "POST"
            assert "authorization" not in request.headers
            assert request.headers.get("x-humchat-sdk-key") == SDK_KEY
            body = await request.json()
            assert body["org_uuid"] == ORG and body["expires_in"] == 900
            member = next((value for value in MEMBERS if value.email == body["email"]), None)
            if member is None:
                return web.json_response({}, status=403)
            self.mints.append(member.id)
            token = f"synthetic-native-dm-{member.name}-{len(self.mints)}"
            self.tokens[token] = member
            return web.json_response({"access_token": token, "token_type": "Bearer", "expires_in": 900})
        assert "x-humchat-sdk-key" not in request.headers, "The SDK key is used only for minting"
        member = self._member(request)
        if member is None:
            return web.json_response({}, status=401)
        entry["actor_id"] = member.id
        if self.route is not None:
            override = self.route(path=path, method=request.method, query=query, member=member)
            if inspect.isawaitable(override):
                override = await override
            if override:
                return web.json_response(override[1], status=override[0])
        if path == "/me":
            return web.json_response({"user_uuid": member.id, "org": ORG, "email": member.email, "roles": ["member"], "scopes": []})
        if path == "/api/sdk/capabilities":
            return web.json_response(self.capabilities)
        if path == "/api/org/members":
            return web.json_response([{"user_uuid": v.id, "org_uuid": ORG, "name": v.name, "email": v.email, "role": "member", "status": "active"}
                                      for v in MEMBERS])  # fmt: skip
        if path == "/api/conversations":
            if request.method == "GET":
                return web.json_response([row for row in self.conversations.values() if any(m["user_uuid"] == member.id for m in row["members"])])
            body = await request.json()
            assert body["name"] is None and len(body["member_uuids"]) == 1
            recipient = next((value for value in MEMBERS if value.id == body["member_uuids"][0]), None)
            if recipient is None or recipient.id == member.id:
                return web.json_response({}, status=403)
            pair = sorted([member.id, recipient.id])
            existing = next((row for row in self.conversations.values() if sorted(m["user_uuid"] for m in row["members"]) == pair), None)
            if existing is not None:
                return web.json_response(existing)
            identifier = f"synthetic-conversation-{len(self.conversations) + 1}"
            row = {"id": identifier, "channel_key": channel(identifier), "name": None, "kind": "dm", "member_count": 2, "my_role": "member",
                   "members": [{"user_uuid": user, "role": "member"} for user in pair], "last_message_at": None}  # fmt: skip
            self.conversations[identifier] = row
            return web.json_response(row)
        if path == "/api/messages/send":
            body = await request.json()
            if not self.permitted(member, body["channel_id"]):
                return web.json_response({}, status=403)
            assert request.headers.get("x-zlack-message-receipt") == "v1"
            key = f"{member.id}:{body['channel_id']}:{request.headers.get('x-idempotency-key')}"
            prior = self.sends.get(key)
            if prior is not None:
                assert body == prior["body"]
                return web.json_response(prior["receipt"])
            self.timestamp += 1
            state = {"schema": 1, "org_uuid": ORG, "channel_id": body["channel_id"], "message_id": body["id"], "sender_id": member.id,
                     "created_at": self.timestamp, "thread_root_id": body["thread_root_id"], "priority": body["priority"],
                     "original_body": body["content"], "body": body["content"], "revision": 1, "updated_at": self.timestamp,
                     "deleted": False, "reaction_revision": 0, "reactions": {}, "attachments": []}  # fmt: skip
            self.states[body["id"]] = state
            await self.append(state, "message", member.id)
            row = next(value for value in self.conversations.values() if value["channel_key"] == body["channel_id"])
            row["last_message_at"] = self.timestamp
            receipt = {"status": "ACK", "message_id": body["id"], "created_at": self.timestamp, "attachment_count": 0,
                       "attachment_manifest_blake3": "0" * 64}  # fmt: skip
            self.sends[key] = {"body": body, "receipt": receipt}
            return web.json_response(receipt)
        if path == "/api/messages/mutate":
            body = await request.json()
            if not self.permitted(member, body["channel_id"]):
                return web.json_response({}, status=403)
            key = f"{member.id}:{body['operation_id']}"
            if key in self.mutations:
                return web.json_response(self.mutations[key])
            state = self.states[body["message_id"]]
            assert state["channel_id"] == body["channel_id"] and body["mutation"]["kind"] == "set_reaction" and body["mutation"]["present"] is True
            emoji = body["mutation"]["emoji"]
            state["reactions"][emoji] = sorted({*state["reactions"].get(emoji, []), member.id})
            state["reaction_revision"] += 1
            self.timestamp += 1
            state["updated_at"] = self.timestamp
            await self.append(state, "reaction", member.id)
            receipt = {"schema": 1, "operation_id": body["operation_id"], "actor_id": member.id, "request_digest": "0" * 64, "state": copy.deepcopy(state)}
            self.mutations[key] = receipt
            return web.json_response(receipt)
        channel_id = query.get("channel_id")
        if not self.permitted(member, channel_id):
            return web.json_response({}, status=403)
        if path == "/api/messages/sync":
            assert request.headers.get("x-zlack-message-state") == "v2"
            thread = query.get("thread_root_id")
            messages = [s for s in self.states.values() if s["channel_id"] == channel_id and s["thread_root_id"] == thread]
            return web.json_response(
                {"schema": 2, "org_uuid": ORG, "channel_id": channel_id, "thread_root_id": thread, "messages": messages, "next_cursor": None}
            )
        if path == "/api/events/sync":
            assert channel_id is not None
            after = query["after"]
            through = query.get("through") or cursor(channel_id, len(self.stream(channel_id)))
            return web.json_response({"schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH,
                                      "events": self.stream(channel_id)[position(after) : position(through)], "after_cursor": after,
                                      "through_cursor": through, "through_sequence": str(position(through)), "next_cursor": None})  # fmt: skip
        return web.json_response({}, status=404)

    def _ready(self, channel_id: str, subscription_id: str | None = None) -> dict[str, Any]:
        size = len(self.stream(channel_id))
        return {"type": "ready", "schema": 1, "org_uuid": ORG, "channel_id": channel_id, "epoch": EPOCH, "sequence": str(size),
                "cursor": cursor(channel_id, size), "earliest_cursor": cursor(channel_id, 0),
                **({"subscription_id": subscription_id} if subscription_id else {})}  # fmt: skip

    async def _upgrade(self, request: web.Request) -> web.StreamResponse:
        try:
            assert "x-humchat-sdk-key" not in request.headers
            assert "token" not in request.query and "sdk_key" not in request.query
            member = self._member(request)
            assert member is not None, "Only a minted Bearer opens an SDK socket"
            assert request.path == ("/ws/events" if self.transport == "multiplex" else "/ws/chat")
        except Exception as error:
            self.errors.append(error)
            return web.Response(status=500)
        ws = web.WebSocketResponse(max_msg_size=0)
        await ws.prepare(request)
        entry = _Socket(member, ws)
        self.sockets.append(entry)
        try:
            if self.transport == "channel":
                channel_id = request.query.get("channel_id", "")
                assert self.permitted(member, channel_id)
                entry.subscriptions[""] = channel_id
                await ws.send_str(json.dumps(self._ready(channel_id)))
            else:
                await ws.send_str(json.dumps({"type": "stream_ready", "schema": 1, "org_uuid": ORG, "max_channels": 100}))
            async for message in ws:
                if message.type != WSMsgType.TEXT or self.transport == "channel":
                    continue
                frame = json.loads(message.data)
                if frame["type"] == "subscribe":
                    assert self.permitted(member, frame["channel_id"])
                    entry.subscriptions[frame["subscription_id"]] = frame["channel_id"]
                    await ws.send_str(json.dumps(self._ready(frame["channel_id"], frame["subscription_id"])))
                elif frame["type"] == "unsubscribe":
                    entry.subscriptions.pop(frame["subscription_id"], None)
                    await ws.send_str(json.dumps({"type": "unsubscribed", "schema": 1, "subscription_id": frame["subscription_id"]}))
        except AssertionError as error:
            self.errors.append(error)
            await ws.close()
        except (ConnectionError, RuntimeError, asyncio.CancelledError):
            pass
        return ws


async def peer(transport: str, **options: Any) -> DmPeer:
    return await DmPeer(transport, **options).start()


Route = Callable[..., "Awaitable[tuple[int, Any] | None] | tuple[int, Any] | None"]
