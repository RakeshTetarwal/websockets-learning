from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Set
from models import Chat, Client, InboxEntry, Message, User


class Connection(ABC):
    @abstractmethod
    def send(self, data: dict) -> None:
        """Send data frame to client connection."""
        pass


class UserRepository(ABC):
    @abstractmethod
    def save_user(self, user: User) -> None:
        pass

    @abstractmethod
    def get_user(self, user_id: str) -> Optional[User]:
        pass

    @abstractmethod
    def save_client(self, client: Client) -> None:
        pass


class ChatRepository(ABC):
    @abstractmethod
    def create_chat(self, chat_id: str, chat_name: str, participant_ids: Set[str]) -> Chat:
        pass

    @abstractmethod
    def get_chat(self, chat_id: str) -> Optional[Chat]:
        pass

    @abstractmethod
    def add_participant(self, chat_id: str, user_id: str) -> None:
        pass

    @abstractmethod
    def remove_participant(self, chat_id: str, user_id: str) -> None:
        pass


class MessageRepository(ABC):
    @abstractmethod
    def save_message(self, message: Message) -> None:
        pass

    @abstractmethod
    def get_message(self, message_id: str) -> Optional[Message]:
        pass


class InboxRepository(ABC):
    @abstractmethod
    def add_entry(self, entry: InboxEntry, user_id: Optional[str] = None) -> int:
        pass

    @abstractmethod
    def get_pending_entries(self, client_id: str) -> List[InboxEntry]:
        pass

    @abstractmethod
    def remove_entry(self, client_id: str, message_id: str) -> None:
        pass


class SessionRegistry(ABC):
    @abstractmethod
    def register_session(self, client_id: str, server_id: str) -> None:
        pass

    @abstractmethod
    def unregister_session(self, client_id: str) -> None:
        pass

    @abstractmethod
    def get_server_for_client(self, client_id: str) -> Optional[str]:
        pass

    @abstractmethod
    def get_user_sequence(self, user_id: str) -> int:
        pass

    @abstractmethod
    def set_user_sequence(self, user_id: str, seq: int) -> None:
        pass

    @abstractmethod
    def update_last_seen(self, user_id: str, timestamp: float) -> None:
        pass

    @abstractmethod
    def get_last_seen(self, user_id: str) -> Optional[float]:
        pass


class MessageBroker(ABC):
    @abstractmethod
    def publish(self, target_server_id: str, payload: dict) -> None:
        pass

    @abstractmethod
    def subscribe(self, server_id: str, handler: Callable[[dict], None]) -> None:
        pass


class MediaStorageService(ABC):
    @abstractmethod
    def generate_presigned_url(self, user_id: str, file_name: str) -> str:
        pass

