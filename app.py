import asyncio
import os
import socket
import time
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from contextlib import asynccontextmanager

from models import Client, ClientAction, FrameType
from storage import (
    PostgrestUserRepository,
    PostgrestChatRepository,
    PostgrestMessageRepository,
    PostgrestInboxRepository,
    PostgrestMediaStorageService,
    RedisSessionRegistry,
    RedisMessageBroker,
)
from interfaces import Connection
from chat_server import ChatServer
from handlers import ActionHandlerFactory


class FastAPIWebSocketConnection(Connection):
    """Thread-safe WebSocket connection adapter for FastAPI."""
    def __init__(self, websocket: WebSocket, loop: asyncio.AbstractEventLoop, client_id: str):
        self.websocket = websocket
        self.loop = loop
        self.client_id = client_id
        self.is_open = True

    def send(self, data: dict) -> None:
        if not self.is_open:
            return
        asyncio.run_coroutine_threadsafe(self._async_send(data), self.loop)

    async def _async_send(self, data: dict) -> None:
        try:
            if self.is_open:
                await self.websocket.send_json(data)
        except Exception:
            self.is_open = False


# Server configuration from environment variables (defaults to unique container hostname)
SERVER_ID = os.environ.get("SERVER_ID") or socket.gethostname()
POSTGREST_URL = os.environ.get("POSTGREST_URL", "http://localhost:3000")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6380"))
PING_INTERVAL = int(os.environ.get("PING_INTERVAL", "15"))
PING_TIMEOUT = int(os.environ.get("PING_TIMEOUT", "10"))

# Initialize infrastructure components
user_repo = PostgrestUserRepository(base_url=POSTGREST_URL)
chat_repo = PostgrestChatRepository(base_url=POSTGREST_URL)
message_repo = PostgrestMessageRepository(base_url=POSTGREST_URL)
inbox_repo = PostgrestInboxRepository(base_url=POSTGREST_URL)
session_registry = RedisSessionRegistry(host=REDIS_HOST, port=REDIS_PORT)
broker = RedisMessageBroker(host=REDIS_HOST, port=REDIS_PORT)
media_service = PostgrestMediaStorageService(base_url=POSTGREST_URL)

chat_server = ChatServer(
    server_id=SERVER_ID,
    chat_repo=chat_repo,
    message_repo=message_repo,
    inbox_repo=inbox_repo,
    session_registry=session_registry,
    message_broker=broker,
    media_service=media_service,
    user_repo=user_repo,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"🚀 {SERVER_ID} started and subscribed to Redis channel:{SERVER_ID}")
    yield
    print(f"🛑 Shutting down {SERVER_ID}...")
    broker.stop()


app = FastAPI(title=f"WhatsApp Chat Server ({SERVER_ID})", lifespan=lifespan)


@app.get("/health")
def health_check():
    return {"status": "UP", "server_id": SERVER_ID}


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
    user_id: Optional[str] = Query(None),
):
    await websocket.accept()
    loop = asyncio.get_running_loop()

    actual_user_id = user_id if user_id else client_id.replace("client_", "")
    client = Client(client_id=client_id, user_id=actual_user_id, local_seq_num=0)
    conn = FastAPIWebSocketConnection(websocket=websocket, loop=loop, client_id=client_id)

    chat_server.connect_client(client, conn)

    # Inform client of successful connection with durable sequence watermark
    server_seq = chat_server.session_registry.get_user_sequence(actual_user_id)
    await websocket.send_json({
        "type": FrameType.ACK,
        "action": ClientAction.CONNECTED,
        "server_id": SERVER_ID,
        "client_id": client_id,
        "user_id": actual_user_id,
        "server_seq": server_seq,
    })

    last_pong_time = time.time()

    async def server_ping_probe() -> None:
        nonlocal last_pong_time
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                if not conn.is_open:
                    break

                # Verify if client failed to reply to previous ping within timeout window
                if time.time() - last_pong_time > (PING_INTERVAL + PING_TIMEOUT):
                    conn.is_open = False
                    try:
                        await websocket.close(code=1001, reason="Heartbeat timeout")
                    except Exception:
                        pass
                    break

                # Send challenge ping with current server sequence watermark
                chat_server.ping_client(client_id)
        except asyncio.CancelledError:
            pass

    probe_task = asyncio.create_task(server_ping_probe())

    try:
        while True:
            payload = await websocket.receive_json()
            raw_action = payload.get("action")

            # Validate action using ClientAction Enum
            try:
                action = ClientAction(raw_action)
            except (ValueError, TypeError):
                await websocket.send_json({
                    "type": FrameType.ERROR,
                    "message": f"Invalid or unknown action: {raw_action}",
                })
                continue

            # Update pong timestamp to reset health deadline
            if action == ClientAction.PONG:
                last_pong_time = time.time()

            # Resolve handler using ActionHandlerFactory
            handler = ActionHandlerFactory.get_handler(action)
            if not handler:
                await websocket.send_json({
                    "type": FrameType.ERROR,
                    "message": f"No handler registered for action: {action}",
                })
                continue

            # Execute command handler
            response = handler.execute(chat_server, client, payload)
            if response:
                await websocket.send_json(response)

    except (WebSocketDisconnect, Exception):
        pass
    finally:
        probe_task.cancel()
        conn.is_open = False
        chat_server.disconnect_client(client_id)
