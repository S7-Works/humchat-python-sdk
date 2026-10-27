"""Public data types. Dates are Unix seconds. IDs are opaque, case-sensitive routing identities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, Union

if TYPE_CHECKING:
    from ._signal import AbortSignal
    from .errors import HumChatError

MessagePriority = Literal["standard", "important", "urgent"]
ConnectionStatus = Literal["idle", "connecting", "syncing", "live", "backoff", "blocked", "closed"]
EventSource = Literal["live", "recovery"]
DurableEventKind = Literal["message", "edit", "delete", "reaction"]


@dataclass
class Identity:
    actor_id: str
    org_id: str
    email: str
    roles: list[str]
    scopes: list[str]
    expires_at: int | None
    app_id: str | None


@dataclass
class Channel:
    id: str
    org_id: str
    team_id: str
    slug: str
    display_name: str
    is_private: bool


@dataclass
class ChannelList:
    org_id: str
    team_id: str
    team_name: str
    channels: list[Channel]


@dataclass
class ConversationMember:
    actor_id: str
    role: str


@dataclass
class Conversation:
    """``id`` is the management identifier; use ``channel_id`` for messaging."""

    id: str
    channel_id: str
    name: str | None
    kind: str
    member_count: int
    my_role: str
    members: list[ConversationMember]
    last_message_at: int | None


@dataclass
class OrgMember:
    actor_id: str
    name: str
    email: str
    role: str
    status: str


@dataclass
class MessageAttachment:
    ordinal: int
    upload_id: str | None
    content_hash: str | None
    file_name: str | None
    media_type: str | None
    byte_length: int | None
    #: Opaque server metadata; never evaluate this string.
    attachment_variants: str | None


@dataclass
class MessageReference:
    id: str
    channel_id: str
    thread_root_id: str | None = None


@dataclass
class MessageState:
    schema: Literal[1]
    org_id: str
    channel_id: str
    id: str
    sender_id: str
    created_at: int
    thread_root_id: str | None
    priority: MessagePriority
    original_body: str
    body: str
    revision: int
    updated_at: int
    deleted: bool
    reaction_revision: int
    reactions: dict[str, list[str]]
    attachments: list[MessageAttachment]


@dataclass
class SendReceipt:
    id: str
    channel_id: str
    status: Literal["accepted"]
    created_at: int
    idempotency_key: str
    attachment_count: int
    attachment_manifest_blake3: str


@dataclass
class DirectSendReceipt:
    conversation: Conversation
    receipt: SendReceipt


@dataclass
class HistoryPage:
    schema: Literal[2]
    org_id: str
    channel_id: str
    thread_root_id: str | None
    messages: list[MessageState]
    next_cursor: str | None


@dataclass
class MutationReceipt:
    schema: Literal[1]
    operation_id: str
    actor_id: str
    request_digest: str
    state: MessageState


@dataclass
class SdkKeyUserTokens:
    endpoint: str
    key_header: str
    configured: bool


@dataclass
class AuthenticationCapabilities:
    user_sessions: bool
    service_identities: bool
    api_key_exchange: bool
    app_binding_required: bool
    mock_authentication: bool
    sdk_key_user_tokens: SdkKeyUserTokens | None
    socket_session_end_codes: list[str]


@dataclass
class MultiplexedEvents:
    protocol_versions: list[int]
    max_channels: int


@dataclass
class OutgoingWebhooks:
    schema_versions: list[int]
    max_channels: int
    workspace_channels_only: bool


@dataclass
class MessagingCapabilities:
    send_receipt_versions: list[int]
    message_state_versions: list[int]
    history_versions: list[int]
    durable_replay: bool
    websocket_scope: Literal["channel"]
    event_protocol_versions: list[int]
    multiplexed_events: MultiplexedEvents | None
    outgoing_webhooks: OutgoingWebhooks | None


@dataclass
class SdkCapabilities:
    protocol_version: Literal[1]
    authentication: AuthenticationCapabilities
    messaging: MessagingCapabilities


# --- Durable events -------------------------------------------------------------------


@dataclass
class DurableEvent:
    schema: Literal[1]
    org_id: str
    channel_id: str
    epoch: str
    event_id: str
    actor_id: str
    sequence: str
    cursor: str
    kind: DurableEventKind
    state: MessageState


@dataclass
class EventPage:
    schema: Literal[1]
    org_id: str
    channel_id: str
    epoch: str
    events: list[DurableEvent]
    after_cursor: str
    through_cursor: str
    through_sequence: str
    next_cursor: str | None


@dataclass
class EventCursor:
    """``SubscribeOptions.start`` form for an explicit durable position."""

    cursor: str


SubscribeStart = Union[Literal["now", "earliest"], EventCursor]


@dataclass
class ConsumerContext:
    signal: AbortSignal
    source: EventSource


class DurableConsumer(Protocol):
    def __call__(self, event: DurableEvent, context: ConsumerContext) -> Any: ...


# --- Realtime payloads ------------------------------------------------------------------


@dataclass
class LiveAttachment(MessageAttachment):
    #: Delivery-time asset locations; they are not attachment identity.
    url: str | None = None
    thumbnail_url: str | None = None


@dataclass
class Message:
    id: str
    org_id: str
    channel_id: str
    sender_id: str
    content: str
    thread_root_id: str | None
    created_at: int
    priority: MessagePriority
    #: ``content`` is only a snippet when true; fetch ``messages.get_state()`` for full text.
    is_long: bool
    attachments: list[LiveAttachment]
    #: Digest of the immutable original full body, never of an edited body.
    body_digest: str | None = None


@dataclass
class TypingEvent:
    org_id: str
    channel_id: str
    actor_id: str
    timestamp: int
    ttl_ms: int


@dataclass
class PresenceEvent:
    org_id: str
    channel_id: str
    actor_id: str
    status: Literal["online", "away", "busy", "offline"]
    last_seen: int


@dataclass
class ReceiptEvent:
    org_id: str
    channel_id: str
    actor_id: str
    through: int


@dataclass
class CallInviteEvent:
    org_id: str
    channel_id: str
    meeting_id: str
    inviter_id: str
    invitee_id: str
    created_at: int


@dataclass
class MessageEvent:
    message: Message
    source: EventSource
    replayed: bool


@dataclass
class StateEvent:
    state: MessageState
    source: EventSource


@dataclass
class CheckpointEvent:
    channel_id: str
    consumer_id: str
    cursor: str
    sequence: str


@dataclass
class ConnectionEvent:
    channel_id: str
    state: ConnectionStatus
    attempt: int
    next_retry_at: float | None = None


@dataclass
class ResyncEvent:
    channel_id: str
    reason: str


@dataclass
class RecoveryCoverage:
    channel_id: str
    mode: Literal["best_effort", "durable"]
    reason: str
    pages: int
    states: int
    complete: bool
    limitations: list[str] = field(default_factory=list)
    #: Durable recovery only: the acknowledged cursor reached.
    cursor: str | None = None


@dataclass
class SessionRevokedEvent:
    channel_id: str
    reason: str


@dataclass
class AuthRequiredEvent:
    channel_id: str
    error: HumChatError


@dataclass
class HandlerErrorEvent:
    event: str
    error: HumChatError


@dataclass
class UnknownEvent:
    channel_id: str
    type: str


__all__ = [
    "AuthRequiredEvent",
    "AuthenticationCapabilities",
    "CallInviteEvent",
    "Channel",
    "ChannelList",
    "CheckpointEvent",
    "ConnectionEvent",
    "ConnectionStatus",
    "ConsumerContext",
    "Conversation",
    "ConversationMember",
    "DirectSendReceipt",
    "DurableConsumer",
    "DurableEvent",
    "DurableEventKind",
    "EventCursor",
    "EventPage",
    "EventSource",
    "HandlerErrorEvent",
    "HistoryPage",
    "Identity",
    "LiveAttachment",
    "Message",
    "MessageAttachment",
    "MessageEvent",
    "MessagePriority",
    "MessageReference",
    "MessageState",
    "MessagingCapabilities",
    "MultiplexedEvents",
    "MutationReceipt",
    "OrgMember",
    "OutgoingWebhooks",
    "PresenceEvent",
    "ReceiptEvent",
    "RecoveryCoverage",
    "ResyncEvent",
    "SdkCapabilities",
    "SdkKeyUserTokens",
    "SendReceipt",
    "SessionRevokedEvent",
    "StateEvent",
    "SubscribeStart",
    "TypingEvent",
    "UnknownEvent",
]
