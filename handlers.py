from abc import ABC, abstractmethod
from typing import Dict, Optional, Type
from models import Client, ClientAction, FrameType, AckStatus, MessageType
from chat_server import ChatServer


class ActionHandler(ABC):
    """Abstract base class for all WebSocket client action handlers."""

    @abstractmethod
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        """Execute the action and return an optional response frame dictionary."""
        pass


class CreateChatHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        chat_name = payload.get("chat_name", "New Chat")
        participant_ids = payload.get("participant_ids", [])
        try:
            chat = server.create_chat(
                user_id=client.user_id,
                participant_ids=participant_ids,
                chat_name=chat_name,
            )
            return {
                "type": FrameType.ACK,
                "action": ClientAction.CREATE_CHAT,
                "status": AckStatus.SUCCESS,
                "chat_id": chat.chat_id,
                "chat_name": chat.chat_name,
                "participants": list(chat.participant_ids),
            }
        except Exception as e:
            return {
                "type": FrameType.ACK,
                "action": ClientAction.CREATE_CHAT,
                "status": AckStatus.ERROR,
                "message": str(e),
            }


class AddParticipantHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        chat_id = payload.get("chat_id")
        target_user_id = payload.get("user_id")
        try:
            server.add_participant(chat_id, target_user_id)
            return {
                "type": FrameType.ACK,
                "action": ClientAction.ADD_PARTICIPANT,
                "status": AckStatus.SUCCESS,
            }
        except Exception as e:
            return {
                "type": FrameType.ACK,
                "action": ClientAction.ADD_PARTICIPANT,
                "status": AckStatus.ERROR,
                "message": str(e),
            }


class RemoveParticipantHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        chat_id = payload.get("chat_id")
        target_user_id = payload.get("user_id")
        try:
            server.remove_participant(chat_id, target_user_id)
            return {
                "type": FrameType.ACK,
                "action": ClientAction.REMOVE_PARTICIPANT,
                "status": AckStatus.SUCCESS,
            }
        except Exception as e:
            return {
                "type": FrameType.ACK,
                "action": ClientAction.REMOVE_PARTICIPANT,
                "status": AckStatus.ERROR,
                "message": str(e),
            }


class RequestMediaUrlHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        file_name = payload.get("file_name", "upload.bin")
        url = server.request_media_upload_url(client.user_id, file_name)
        return {
            "type": FrameType.ACK,
            "action": ClientAction.REQUEST_MEDIA_URL,
            "status": AckStatus.SUCCESS,
            "presigned_url": url,
        }


class SendMessageHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        chat_id = payload.get("chat_id")
        content = payload.get("content")
        msg_type_str = payload.get("message_type", "TEXT").upper()
        msg_type = MessageType.MEDIA if msg_type_str == MessageType.MEDIA else MessageType.TEXT
        try:
            message_id = server.send_message(
                sender_client_id=client.client_id,
                chat_id=chat_id,
                content=content,
                message_type=msg_type,
            )
            return {
                "type": FrameType.ACK,
                "action": ClientAction.SEND_MESSAGE,
                "status": AckStatus.SUCCESS,
                "message_id": message_id,
            }
        except Exception as e:
            return {
                "type": FrameType.ACK,
                "action": ClientAction.SEND_MESSAGE,
                "status": AckStatus.ERROR,
                "message": str(e),
            }


class AcknowledgeMessageHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        message_id = payload.get("message_id")
        server.acknowledge_message(client.client_id, message_id)
        return {
            "type": FrameType.ACK,
            "action": ClientAction.ACKNOWLEDGE_MESSAGE,
            "status": AckStatus.SUCCESS,
            "message_id": message_id,
        }


class PongHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        seq_num = payload.get("seq_num", 0)
        client.local_seq_num = seq_num
        server.handle_pong(client.client_id, client_seq_num=seq_num)
        return None  # Pong heartbeat requires no immediate return frame


class GetLastSeenHandler(ActionHandler):
    def execute(self, server: ChatServer, client: Client, payload: dict) -> Optional[dict]:
        target_user_id = payload.get("user_id")
        ts = server.get_last_seen(target_user_id)
        return {
            "type": FrameType.ACK,
            "action": ClientAction.GET_LAST_SEEN,
            "status": AckStatus.SUCCESS,
            "user_id": target_user_id,
            "last_seen": ts,
        }


class ActionHandlerFactory:
    """Factory to instantiate and resolve ActionHandler instances based on ClientAction."""

    _handlers: Dict[ClientAction, Type[ActionHandler]] = {
        ClientAction.CREATE_CHAT: CreateChatHandler,
        ClientAction.ADD_PARTICIPANT: AddParticipantHandler,
        ClientAction.REMOVE_PARTICIPANT: RemoveParticipantHandler,
        ClientAction.REQUEST_MEDIA_URL: RequestMediaUrlHandler,
        ClientAction.SEND_MESSAGE: SendMessageHandler,
        ClientAction.ACKNOWLEDGE_MESSAGE: AcknowledgeMessageHandler,
        ClientAction.PONG: PongHandler,
        ClientAction.GET_LAST_SEEN: GetLastSeenHandler,
    }

    @classmethod
    def get_handler(cls, action: ClientAction) -> Optional[ActionHandler]:
        handler_cls = cls._handlers.get(action)
        if handler_cls:
            return handler_cls()
        return None

    @classmethod
    def register_handler(cls, action: ClientAction, handler_cls: Type[ActionHandler]) -> None:
        cls._handlers[action] = handler_cls
