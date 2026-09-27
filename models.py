from dataclasses import dataclass, field
from enum import Enum
from typing import List, Set
import time
import uuid


class MessageType(str, Enum):
    TEXT = "TEXT"
    MEDIA = "MEDIA"


class ClientAction(str, Enum):
    CONNECTED = "connected"
    CREATE_CHAT = "createChat"
    ADD_PARTICIPANT = "addParticipant"
    REMOVE_PARTICIPANT = "removeParticipant"
    REQUEST_MEDIA_URL = "requestMediaUrl"
    SEND_MESSAGE = "sendMessage"
    ACKNOWLEDGE_MESSAGE = "acknowledgeMessage"
    PONG = "pong"
    GET_LAST_SEEN = "getLastSeen"



class FrameType(str, Enum):
    ACK = "ACK"
    MESSAGE = "MESSAGE"
    PING = "PING"
    ERROR = "ERROR"


class AckStatus(str, Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


@dataclass
class User:
    user_id: str
    name: str
    last_seen: float = field(default_factory=time.time)


@dataclass
class Client:
    client_id: str
    user_id: str
    local_seq_num: int = 0


@dataclass
class Chat:
    chat_id: str
    chat_name: str
    participant_ids: Set[str] = field(default_factory=set)


@dataclass
class Message:
    message_id: str
    chat_id: str
    sender_id: str
    content: str
    message_type: MessageType
    timestamp: float = field(default_factory=time.time)
    seq_num: int = 0


@dataclass
class InboxEntry:
    message_id: str
    client_id: str
    message: Message
