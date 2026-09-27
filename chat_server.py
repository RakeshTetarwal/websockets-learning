import time
import uuid
from typing import Dict, List, Optional, Set

from models import Chat, Client, InboxEntry, Message, MessageType, User
from interfaces import (
    ChatRepository,
    MessageRepository,
    InboxRepository,
    SessionRegistry,
    MessageBroker,
    MediaStorageService,
    UserRepository,
    Connection,
)


class ChatServer:
    def __init__(
        self,
        server_id: str,
        chat_repo: ChatRepository,
        message_repo: MessageRepository,
        inbox_repo: InboxRepository,
        session_registry: SessionRegistry,
        message_broker: MessageBroker,
        media_service: MediaStorageService,
        user_repo: Optional[UserRepository] = None,
    ):
        self.server_id = server_id
        self.chat_repo = chat_repo
        self.message_repo = message_repo
        self.inbox_repo = inbox_repo
        self.session_registry = session_registry
        self.message_broker = message_broker
        self.media_service = media_service
        self.user_repo = user_repo

        # Local in-memory state for clients connected to this specific server
        self.local_connections: Dict[str, Connection] = {}
        self.connected_clients: Dict[str, Client] = {}

        # Subscribe to this server's dedicated broker channel
        self.message_broker.subscribe(self.server_id, self._on_broker_message)

    # ---------------- Connection Lifecycle ----------------

    def connect_client(self, client: Client, connection: Connection) -> None:
        """Establish WebSocket connection for a client and drain pending inbox messages."""
        if self.user_repo:
            self.user_repo.save_user(User(user_id=client.user_id, name=client.user_id))
            self.user_repo.save_client(client)

        self.local_connections[client.client_id] = connection
        self.connected_clients[client.client_id] = client
        self.session_registry.register_session(client.client_id, self.server_id)
        self.session_registry.update_last_seen(client.user_id, time.time())

        # Flush any pending messages that arrived while client was offline
        self._flush_inbox_to_client(client.client_id)

    def disconnect_client(self, client_id: str) -> None:
        """Tear down client connection and unregister session."""
        client = self.connected_clients.pop(client_id, None)
        self.local_connections.pop(client_id, None)
        self.session_registry.unregister_session(client_id)
        if client:
            self.session_registry.update_last_seen(client.user_id, time.time())

    # ---------------- Chat Operations ----------------

    def create_chat(self, user_id: str, participant_ids: List[str], chat_name: str) -> Chat:
        all_participants = set(participant_ids)
        all_participants.add(user_id)

        if len(all_participants) > 100:
            raise ValueError("Group size exceeds maximum limit of 100 users.")

        if self.user_repo:
            for pid in all_participants:
                self.user_repo.save_user(User(user_id=pid, name=pid))

        chat_id = str(uuid.uuid4())
        return self.chat_repo.create_chat(chat_id, chat_name, all_participants)

    def add_participant(self, chat_id: str, user_id: str) -> None:
        chat = self.chat_repo.get_chat(chat_id)
        if not chat:
            raise ValueError(f"Chat {chat_id} not found.")
        if len(chat.participant_ids) >= 100:
            raise ValueError("Chat has reached maximum capacity of 100 participants.")
        if self.user_repo:
            self.user_repo.save_user(User(user_id=user_id, name=user_id))
        self.chat_repo.add_participant(chat_id, user_id)

    def remove_participant(self, chat_id: str, user_id: str) -> None:
        self.chat_repo.remove_participant(chat_id, user_id)

    def request_media_upload_url(self, user_id: str, file_name: str) -> str:
        """Returns pre-signed S3 URL for uploading media without streaming binary over sockets."""
        return self.media_service.generate_presigned_url(user_id, file_name)

    # ---------------- Messaging Operations ----------------

    def send_message(
        self,
        sender_client_id: str,
        chat_id: str,
        content: str,
        message_type: MessageType = MessageType.TEXT,
    ) -> str:
        sender_client = self.connected_clients.get(sender_client_id)
        if not sender_client:
            raise PermissionError("Client is not connected to this server.")

        chat = self.chat_repo.get_chat(chat_id)
        if not chat or sender_client.user_id not in chat.participant_ids:
            raise PermissionError("User is not a participant of this chat.")

        message_id = str(uuid.uuid4())
        message = Message(
            message_id=message_id,
            chat_id=chat_id,
            sender_id=sender_client.user_id,
            content=content,
            message_type=message_type,
            timestamp=time.time(),
        )
        self.message_repo.save_message(message)

        # Batch recipients by destination server
        remote_server_batches: Dict[str, List[str]] = {}

        # Fan-out to recipients
        for participant_id in chat.participant_ids:
            if participant_id == sender_client.user_id:
                continue

            # Assume client_{user_id} convention for client identification
            recipient_client_id = f"client_{participant_id}"

            # Prepare inbox entry
            inbox_entry = InboxEntry(
                message_id=message_id,
                client_id=recipient_client_id,
                message=message,
            )

            # ATOMIC SINGLE TRANSACTION: Increment user_sequences AND insert into inbox in one DB call!
            seq_num = self.inbox_repo.add_entry(inbox_entry, user_id=participant_id)
            message.seq_num = seq_num

            # Sync Redis L1 cache
            self.session_registry.set_user_sequence(participant_id, seq_num)

            # Check if recipient is connected locally on THIS server
            if recipient_client_id in self.local_connections:
                # Fast path: in-memory push
                self._send_payload_to_local_client(recipient_client_id, message)
            else:
                # Check which remote server holds the recipient's connection
                target_server = self.session_registry.get_server_for_client(recipient_client_id)
                if target_server:
                    if target_server not in remote_server_batches:
                        remote_server_batches[target_server] = []
                    remote_server_batches[target_server].append(recipient_client_id)

        # Dispatch batched envelopes to respective remote servers
        for target_server, client_ids in remote_server_batches.items():
            envelope = {
                "message": {
                    "message_id": message.message_id,
                    "chat_id": message.chat_id,
                    "sender_id": message.sender_id,
                    "content": message.content,
                    "message_type": message.message_type.value,
                    "timestamp": message.timestamp,
                    "seq_num": message.seq_num,
                },
                "target_client_ids": client_ids,
            }
            self.message_broker.publish(target_server, envelope)

        return message_id

    def _on_broker_message(self, payload: dict) -> None:
        """Handle incoming routed envelope from broker and push to local client sockets."""
        raw_msg = payload.get("message", {})
        message = Message(
            message_id=raw_msg["message_id"],
            chat_id=raw_msg["chat_id"],
            sender_id=raw_msg["sender_id"],
            content=raw_msg["content"],
            message_type=MessageType(raw_msg["message_type"]),
            timestamp=raw_msg["timestamp"],
            seq_num=raw_msg.get("seq_num", 0),
        )

        for client_id in payload.get("target_client_ids", []):
            if client_id in self.local_connections:
                self._send_payload_to_local_client(client_id, message)

    def _send_payload_to_local_client(self, client_id: str, message: Message) -> None:
        conn = self.local_connections.get(client_id)
        if conn:
            conn.send({
                "type": "MESSAGE",
                "message_id": message.message_id,
                "chat_id": message.chat_id,
                "sender_id": message.sender_id,
                "content": message.content,
                "message_type": message.message_type.value,
                "timestamp": message.timestamp,
                "seq_num": message.seq_num,
            })

    def acknowledge_message(self, client_id: str, message_id: str) -> None:
        """Client ACKs receipt -> server deletes message from Inbox."""
        self.inbox_repo.remove_entry(client_id, message_id)

    # ---------------- Heartbeat & Sequence Gap Sync ----------------

    def ping_client(self, client_id: str) -> None:
        """Send Ping frame to client with current durable sequence watermark."""
        conn = self.local_connections.get(client_id)
        client = self.connected_clients.get(client_id)
        if conn and client:
            server_seq = self.session_registry.get_user_sequence(client.user_id)
            conn.send({
                "type": "PING",
                "server_seq": server_seq,
            })

    def handle_pong(self, client_id: str, client_seq_num: int) -> None:
        """
        Handle Pong response with sequence number gap detection.
        If client's sequence < server stored sequence, flush un-ACKed inbox messages.
        """
        client = self.connected_clients.get(client_id)
        if not client:
            return

        # Update last seen timestamp on successful pong
        self.session_registry.update_last_seen(client.user_id, time.time())

        # Check for sequence gap
        server_seq = self.session_registry.get_user_sequence(client.user_id)
        if client_seq_num < server_seq:
            # Gap detected! Flush pending messages from Inbox
            self._flush_inbox_to_client(client_id)

    def _flush_inbox_to_client(self, client_id: str) -> None:
        """Drain undelivered messages from Inbox for a client."""
        pending_entries = self.inbox_repo.get_pending_entries(client_id)
        for entry in pending_entries:
            self._send_payload_to_local_client(client_id, entry.message)

    def get_last_seen(self, user_id: str) -> Optional[float]:
        return self.session_registry.get_last_seen(user_id)
