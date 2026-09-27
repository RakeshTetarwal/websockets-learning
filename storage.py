import json
import threading
from typing import Callable, Dict, List, Optional, Set
import httpx
import redis

from models import Chat, Client, InboxEntry, Message, MessageType, User
from interfaces import (
    UserRepository,
    ChatRepository,
    MessageRepository,
    InboxRepository,
    SessionRegistry,
    MessageBroker,
    MediaStorageService,
)


class PostgrestUserRepository(UserRepository):
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url)

    def save_user(self, user: User) -> None:
        headers = {"Prefer": "resolution=merge-duplicates"}
        payload = {
            "user_id": user.user_id,
            "name": user.name,
            "last_seen": user.last_seen,
        }
        res = self.client.post("/users", json=payload, headers=headers)
        res.raise_for_status()

        # Initialize sequence record if not present
        try:
            seq_payload = {"user_id": user.user_id, "last_seq_num": 0}
            self.client.post("/user_sequences", json=seq_payload, headers={"Prefer": "resolution=ignore-duplicates"})
        except Exception:
            pass

    def get_user(self, user_id: str) -> Optional[User]:
        res = self.client.get(f"/users?user_id=eq.{user_id}")
        res.raise_for_status()
        rows = res.json()
        if not rows:
            return None
        row = rows[0]
        return User(user_id=row["user_id"], name=row["name"], last_seen=row.get("last_seen", 0.0))

    def save_client(self, client: Client) -> None:
        headers = {"Prefer": "resolution=merge-duplicates"}
        payload = {
            "client_id": client.client_id,
            "user_id": client.user_id,
        }
        res = self.client.post("/clients", json=payload, headers=headers)
        res.raise_for_status()


class PostgrestChatRepository(ChatRepository):
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url)

    def create_chat(self, chat_id: str, chat_name: str, participant_ids: Set[str]) -> Chat:
        headers = {"Prefer": "return=minimal"}
        chat_payload = {"chat_id": chat_id, "chat_name": chat_name}
        res = self.client.post("/chats", json=chat_payload, headers=headers)
        res.raise_for_status()

        if participant_ids:
            part_payload = [{"chat_id": chat_id, "user_id": uid} for uid in participant_ids]
            p_res = self.client.post("/chat_participants", json=part_payload, headers=headers)
            p_res.raise_for_status()

        return Chat(chat_id=chat_id, chat_name=chat_name, participant_ids=set(participant_ids))

    def get_chat(self, chat_id: str) -> Optional[Chat]:
        res = self.client.get(f"/chats?chat_id=eq.{chat_id}")
        res.raise_for_status()
        rows = res.json()
        if not rows:
            return None
        chat_data = rows[0]

        p_res = self.client.get(f"/chat_participants?chat_id=eq.{chat_id}&select=user_id")
        p_res.raise_for_status()
        participants = {p["user_id"] for p in p_res.json()}

        return Chat(
            chat_id=chat_data["chat_id"],
            chat_name=chat_data["chat_name"],
            participant_ids=participants,
        )

    def add_participant(self, chat_id: str, user_id: str) -> None:
        headers = {"Prefer": "resolution=ignore-duplicates"}
        payload = {"chat_id": chat_id, "user_id": user_id}
        res = self.client.post("/chat_participants", json=payload, headers=headers)
        res.raise_for_status()

    def remove_participant(self, chat_id: str, user_id: str) -> None:
        res = self.client.delete(f"/chat_participants?chat_id=eq.{chat_id}&user_id=eq.{user_id}")
        res.raise_for_status()


class PostgrestMessageRepository(MessageRepository):
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url)

    def save_message(self, message: Message) -> None:
        headers = {"Prefer": "resolution=merge-duplicates"}
        payload = {
            "message_id": message.message_id,
            "chat_id": message.chat_id,
            "sender_id": message.sender_id,
            "content": message.content,
            "message_type": message.message_type.value,
            "timestamp": message.timestamp,
            "seq_num": message.seq_num,
        }
        res = self.client.post("/messages", json=payload, headers=headers)
        res.raise_for_status()

    def get_message(self, message_id: str) -> Optional[Message]:
        res = self.client.get(f"/messages?message_id=eq.{message_id}")
        res.raise_for_status()
        rows = res.json()
        if not rows:
            return None
        row = rows[0]
        return Message(
            message_id=row["message_id"],
            chat_id=row["chat_id"],
            sender_id=row["sender_id"],
            content=row["content"],
            message_type=MessageType(row["message_type"]),
            timestamp=row["timestamp"],
            seq_num=row.get("seq_num", 0),
        )


class PostgrestInboxRepository(InboxRepository):
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url)

    def add_entry(self, entry: InboxEntry, user_id: Optional[str] = None) -> int:
        if user_id:
            # Atomic single transaction: PostgreSQL increments user_sequences AND inserts into inbox together
            rpc_payload = {
                "p_message_id": entry.message_id,
                "p_client_id": entry.client_id,
                "p_chat_id": entry.message.chat_id,
                "p_sender_id": entry.message.sender_id,
                "p_content": entry.message.content,
                "p_message_type": entry.message.message_type.value,
                "p_timestamp": entry.message.timestamp,
                "p_user_id": user_id,
            }
            res = self.client.post("/rpc/add_inbox_entry_with_seq", json=rpc_payload)
            res.raise_for_status()
            return int(res.text.strip())

        headers = {"Prefer": "return=minimal"}
        payload = {
            "message_id": entry.message_id,
            "client_id": entry.client_id,
            "chat_id": entry.message.chat_id,
            "sender_id": entry.message.sender_id,
            "content": entry.message.content,
            "message_type": entry.message.message_type.value,
            "timestamp": entry.message.timestamp,
            "seq_num": entry.message.seq_num,
        }
        res = self.client.post("/inbox", json=payload, headers=headers)
        res.raise_for_status()
        return entry.message.seq_num

    def get_pending_entries(self, client_id: str) -> List[InboxEntry]:
        res = self.client.get(f"/inbox?client_id=eq.{client_id}&order=timestamp.asc")
        res.raise_for_status()
        rows = res.json()
        entries = []
        for r in rows:
            msg = Message(
                message_id=r["message_id"],
                chat_id=r["chat_id"],
                sender_id=r["sender_id"],
                content=r["content"],
                message_type=MessageType(r["message_type"]),
                timestamp=r["timestamp"],
                seq_num=r.get("seq_num", 0),
            )
            entries.append(InboxEntry(message_id=r["message_id"], client_id=r["client_id"], message=msg))
        return entries

    def remove_entry(self, client_id: str, message_id: str) -> None:
        res = self.client.delete(f"/inbox?client_id=eq.{client_id}&message_id=eq.{message_id}")
        res.raise_for_status()


class PostgrestMediaStorageService(MediaStorageService):
    """Returns the media upload endpoint for uploading media files into PostgreSQL via PostgREST."""
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url.rstrip("/")

    def generate_presigned_url(self, user_id: str, file_name: str) -> str:
        """Returns the PostgREST URL endpoint for uploading media into PostgreSQL."""
        return f"{self.base_url}/media"



class RedisSessionRegistry(SessionRegistry):
    def __init__(self, host: str = "localhost", port: int = 6380):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)

    def register_session(self, client_id: str, server_id: str) -> None:
        self.redis.set(f"session:{client_id}", server_id)

    def unregister_session(self, client_id: str) -> None:
        self.redis.delete(f"session:{client_id}")

    def get_server_for_client(self, client_id: str) -> Optional[str]:
        return self.redis.get(f"session:{client_id}")

    def get_user_sequence(self, user_id: str) -> int:
        val = self.redis.get(f"seq:{user_id}")
        return int(val) if val else 0

    def set_user_sequence(self, user_id: str, seq: int) -> None:
        self.redis.set(f"seq:{user_id}", str(seq))

    def update_last_seen(self, user_id: str, timestamp: float) -> None:
        self.redis.set(f"last_seen:{user_id}", str(timestamp))

    def get_last_seen(self, user_id: str) -> Optional[float]:
        val = self.redis.get(f"last_seen:{user_id}")
        return float(val) if val else None


class RedisMessageBroker(MessageBroker):
    def __init__(self, host: str = "localhost", port: int = 6380):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        self.handlers: Dict[str, Callable[[dict], None]] = {}
        self._thread: Optional[threading.Thread] = None
        self._running: bool = False

    def publish(self, target_server_id: str, payload: dict) -> None:
        channel = f"channel:{target_server_id}"
        self.redis.publish(channel, json.dumps(payload))

    def subscribe(self, server_id: str, handler: Callable[[dict], None]) -> None:
        channel = f"channel:{server_id}"
        self.handlers[channel] = handler
        self.pubsub.subscribe(channel)

        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()

    def _listen_loop(self) -> None:
        try:
            for message in self.pubsub.listen():
                if not self._running:
                    break
                if message and message["type"] == "message":
                    channel = message["channel"]
                    handler = self.handlers.get(channel)
                    if handler:
                        data = json.loads(message["data"])
                        handler(data)
        except Exception:
            pass

    def stop(self) -> None:
        self._running = False
        try:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        except Exception:
            pass
