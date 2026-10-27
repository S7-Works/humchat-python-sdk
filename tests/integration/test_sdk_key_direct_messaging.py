"""Port of test/integration/sdk-key-direct-messaging.test.mjs."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from helpers import rejects
from humchat import MemoryCheckpointStore
from sdk_key_dm_fixture import ALICE, BOB, CAROL, CONSUMER_ID, MEMBERS, channel, cursor, peer, until


@pytest.mark.parametrize("transport", ["channel", "multiplex"])
async def test_native_sdk_key_dms_discover_the_first_message_reply_react_and_resume_without_echoing(transport: str) -> None:
    fixture = await peer(transport)
    sender_store, receiver_store = MemoryCheckpointStore(), MemoryCheckpointStore()
    try:
        sender, receiver = fixture.client(ALICE), fixture.client(BOB)
        received: list[str] = []
        observed_by_sender: list[str] = []
        sender_inbound: list[Any] = []
        own_messages: list[str] = []
        replies: list[Any] = []

        async def receiver_handler(event: Any, _: Any) -> None:
            received.append(f"{event.kind}:{event.state.id}")
            if event.kind != "message":
                return
            if event.state.sender_id == BOB.id:
                own_messages.append(event.state.id)
                return
            if event.state.body != "!status":
                return
            replies.append(await receiver.messages.reply(event.state, id="synthetic-reply", content="Ready."))
            await receiver.messages.set_reaction(
                channel_id=event.channel_id, message_id=event.state.id, operation_id="synthetic-reaction", emoji="✅", present=True
            )

        inbox = receiver.inbox.watch(receiver_handler, consumer_id=CONSUMER_ID, checkpoint_store=receiver_store, poll_interval_ms=10000)
        await inbox.ready
        assert inbox.get_stats().conversations == 0
        assert len(fixture.sockets) == 0, "There is no recipient socket before the first conversation exists"
        roster = await sender.members.list()
        target = next(member for member in roster if member.email == BOB.email)
        assert target.actor_id == BOB.id
        sent = await sender.messages.send_direct(
            recipient_id=target.actor_id, content="!status", id="synthetic-first-message", idempotency_key="synthetic-first-intent"
        )
        channel_id = sent.conversation.channel_id
        assert sent.receipt.status == "accepted"
        assert channel_id == channel(sent.conversation.id)
        assert [event["state"]["sender_id"] for event in fixture.stream(channel_id)] == [ALICE.id]

        def sender_handler(event: Any, _: Any) -> None:
            observed_by_sender.append(f"{event.kind}:{event.state.id}")
            if event.kind == "message" and event.state.sender_id == BOB.id:
                sender_inbound.append(event.state)

        sender_inbox = sender.inbox.watch(sender_handler, consumer_id=CONSUMER_ID, checkpoint_store=sender_store, poll_interval_ms=10000)
        await sender_inbox.ready
        await inbox.refresh()

        async def both_committed() -> bool:
            return await receiver_store.load(fixture.scope(BOB, channel_id)) == cursor(channel_id, 3) and await sender_store.load(
                fixture.scope(ALICE, channel_id)
            ) == cursor(channel_id, 3)

        await until(both_committed, "both native clients commit the exchange")
        expected = ["message:synthetic-first-message", "message:synthetic-reply", "reaction:synthetic-first-message"]
        assert received == expected
        assert observed_by_sender == expected
        assert own_messages == ["synthetic-reply"]
        assert len(replies) == 1, "Handler ignores its own reply"
        assert fixture.states["synthetic-reply"]["thread_root_id"] == "synthetic-first-message"
        assert fixture.states["synthetic-first-message"]["reactions"] == {"✅": [BOB.id]}
        assert len(fixture.conversations) == 1
        assert sorted(fixture.mints) == sorted([ALICE.id, BOB.id]), "Each actor has a cached independent mint"

        # The server may replay an accepted request or repeat a live hint. Neither creates a
        # second message or calls an already-acknowledged handler again.
        await sender.messages.send_direct(recipient_id=BOB.id, content="!status", id="synthetic-first-message", idempotency_key="synthetic-first-intent")
        for event in list(fixture.stream(channel_id)):
            await fixture.publish(event)
        await asyncio.sleep(0.05)
        assert len(fixture.stream(channel_id)) == 3
        assert received == expected
        assert len(replies) == 1
        assert sum(1 for item in fixture.requests if item["path"] == "/api/conversations" and item["method"] == "POST") == 1

        status, body = await fixture.app_read(BOB, channel_id)
        assert status == 200
        assert [[m["message_id"], m["sender_id"], m["body"]] for m in body["messages"]] == [["synthetic-first-message", ALICE.id, "!status"]]
        assert body["messages"][0]["reactions"] == {"✅": [BOB.id]}
        _, thread = await fixture.app_read(ALICE, channel_id, "synthetic-first-message")
        assert [[m["message_id"], m["sender_id"], m["thread_root_id"]] for m in thread["messages"]] == [
            ["synthetic-reply", BOB.id, "synthetic-first-message"]
        ]
        history = await receiver.messages.history(channel_id=channel_id, thread_root_id="synthetic-first-message")
        assert history.messages[0].body == "Ready."

        await receiver.close()
        await inbox.done
        await sender.messages.send_direct(recipient_id=BOB.id, id="synthetic-offline-message", content="While you were offline.")
        assert len((await fixture.app_read(BOB, channel_id))[1]["messages"]) == 2, "App-like REST can read the message while the SDK is offline"
        resumed = fixture.client(BOB)
        replayed: list[str] = []
        following = resumed.inbox.watch(
            lambda event, _: replayed.append(event.state.id), consumer_id=CONSUMER_ID, checkpoint_store=receiver_store, poll_interval_ms=10000
        )
        await following.ready

        async def caught_up() -> bool:
            return await receiver_store.load(fixture.scope(BOB, channel_id)) == cursor(channel_id, 4)

        await until(caught_up, "restart catches up from the committed position")
        assert replayed == ["synthetic-offline-message"]
        assert any(
            item["path"] == "/api/events/sync" and item.get("actor_id") == BOB.id and item["query"].get("after") == cursor(channel_id, 3)
            for item in fixture.requests
        ), "Restart requests the prior durable checkpoint"
        assert await receiver_store.load(fixture.scope(CAROL, channel_id)) is None, "Checkpoints are actor-scoped"
        status, _ = await fixture.app_send(BOB, channel_id, "synthetic-from-app", "Sent with a normal user session.")
        assert status == 200

        async def sender_saw_app() -> bool:
            return await sender_store.load(fixture.scope(ALICE, channel_id)) == cursor(channel_id, 5)

        await until(sender_saw_app, "SDK receives a message sent by a separate app-like REST client")
        assert [[m.id, m.sender_id] for m in sender_inbound] == [["synthetic-reply", BOB.id], ["synthetic-from-app", BOB.id]]
        await asyncio.gather(sender.close(), resumed.close())
        await until(lambda: all(entry.ws.closed for entry in fixture.sockets), "all managed inbox sockets close")
        assert sender.realtime.get_stats().active_sockets == 0
        assert resumed.realtime.get_stats().active_sockets == 0
    finally:
        await sender_store.close()
        await receiver_store.close()
        await fixture.close()


@pytest.mark.parametrize("transport", ["channel", "multiplex"])
async def test_native_direct_messaging_reaches_either_same_org_member_while_rejecting_foreign_recipients_and_private_dm_reads(transport: str) -> None:
    fixture = await peer(transport)
    try:
        sender, unrelated = fixture.client(ALICE), fixture.client(CAROL)
        roster = await sender.members.list()
        assert sorted(member.actor_id for member in roster) == sorted(member.id for member in MEMBERS)
        receipts = []
        for recipient in [member for member in roster if member.actor_id != ALICE.id]:
            receipts.append(
                await sender.messages.send_direct(
                    recipient_id=recipient.actor_id, id=f"synthetic-for-{recipient.actor_id}", content=f"Hello {recipient.name}."
                )
            )
        assert len({receipt.conversation.channel_id for receipt in receipts}) == 2
        bob_channel = next(r for r in receipts if any(m.actor_id == BOB.id for m in r.conversation.members)).conversation.channel_id
        assert (await fixture.app_read(BOB, bob_channel))[0] == 200
        assert (await fixture.app_read(CAROL, bob_channel))[0] == 403
        await rejects(unrelated.messages.history(channel_id=bob_channel), "PERMISSION")
        assert len(await unrelated.conversations.list()) == 1
        send_count = sum(1 for item in fixture.requests if item["path"] == "/api/messages/send")
        await rejects(sender.messages.send_direct(recipient_id="synthetic-foreign-member", content="Must not send."), "PERMISSION")
        assert sum(1 for item in fixture.requests if item["path"] == "/api/messages/send") == send_count, (
            "An unauthorized recipient is rejected before sending message content"
        )
        assert len(fixture.conversations) == 2
    finally:
        await fixture.close()
