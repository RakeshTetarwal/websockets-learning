from fastapi.testclient import TestClient
from app import app
from models import ClientAction
import httpx
import redis



def reset_environment():
    """Clear test data from PostgREST and Redis for a clean test run."""
    client = httpx.Client(base_url="http://localhost:3000")
    client.delete("/inbox?id=gte.0")
    client.delete("/messages?message_id=neq.none")
    client.delete("/chat_participants?chat_id=neq.none")
    client.delete("/chats?chat_id=neq.none")
    client.delete("/clients?client_id=neq.none")
    client.delete("/user_sequences?user_id=neq.none")
    client.delete("/users?user_id=neq.none")

    r = redis.Redis(host="localhost", port=6380)
    r.flushdb()


def run_websocket_tests():
    print("==================================================")
    print("🌐 TESTING REAL WEBSOCKET APIS (FastAPI WebSockets)")
    print("==================================================\n")

    reset_environment()
    print("🧹 Database and Redis flushed.")

    client = TestClient(app)

    # 1. Connect Alice and Bob over WebSockets
    with client.websocket_connect("/ws/client_alice?user_id=alice") as ws_alice, \
         client.websocket_connect("/ws/client_bob?user_id=bob") as ws_bob:



        bob_welcome = ws_bob.receive_json()
        print("✅ Bob WebSocket connected:", bob_welcome)
        assert bob_welcome["action"] == ClientAction.CONNECTED

        # 2. Alice calls 'createChat' via WebSocket
        
        ws_alice.send_json({
            "action": "createChat",
            "chat_name": "WebSocket Discussion",
            "participant_ids": ["bob"],
        })
        # Both receive initial connection ACK
        alice_welcome = ws_alice.receive_json()
        print("✅ Alice WebSocket connected:", alice_welcome)
        assert alice_welcome["action"] == ClientAction.CONNECTED
        print("\n--- 💬 Action: createChat ---")
        chat_ack = ws_alice.receive_json()
        print("Received createChat ACK:", chat_ack)
        assert chat_ack["status"] == "SUCCESS"
        chat_id = chat_ack["chat_id"]

        # 3. Alice requests media pre-signed URL via WebSocket
        print("\n--- 📸 Action: requestMediaUrl ---")
        ws_alice.send_json({
            "action": "requestMediaUrl",
            "file_name": "photo.jpg",
        })
        media_ack = ws_alice.receive_json()
        print("Received requestMediaUrl ACK:", media_ack)
        assert media_ack["status"] == "SUCCESS"
        assert "media" in media_ack["presigned_url"]

        # 4. Alice sends message via WebSocket
        print("\n--- ✉️ Action: sendMessage ---")
        ws_alice.send_json({
            "action": "sendMessage",
            "chat_id": chat_id,
            "content": "Hello over WebSockets!",
            "message_type": "TEXT",
        })

        # Alice receives sent ACK
        send_ack = ws_alice.receive_json()
        print("Alice received sendMessage ACK:", send_ack)
        assert send_ack["status"] == "SUCCESS"
        message_id = send_ack["message_id"]

        # Bob receives incoming MESSAGE frame in real time
        bob_msg = ws_bob.receive_json()
        print("📥 Bob received incoming frame over WebSocket:", bob_msg)
        assert bob_msg["type"] == "MESSAGE"
        assert bob_msg["content"] == "Hello over WebSockets!"
        assert bob_msg["seq_num"] == 1

        # 5. Bob sends 'acknowledgeMessage' via WebSocket
        print("\n--- ✅ Action: acknowledgeMessage ---")
        ws_bob.send_json({
            "action": "acknowledgeMessage",
            "message_id": message_id,
        })
        ack_resp = ws_bob.receive_json()
        print("Bob received acknowledgeMessage ACK:", ack_resp)
        assert ack_resp["status"] == "SUCCESS"

        # 6. Bob sends 'pong' heartbeat with sequence number via WebSocket
        print("\n--- 💓 Action: pong ---")
        ws_bob.send_json({
            "action": "pong",
            "seq_num": 1,
        })

        # 7. Alice checks 'getLastSeen' via WebSocket
        print("\n--- 🕒 Action: getLastSeen ---")
        ws_alice.send_json({
            "action": "getLastSeen",
            "user_id": "bob",
        })
        last_seen_ack = ws_alice.receive_json()
        print("Alice received getLastSeen ACK:", last_seen_ack)
        assert last_seen_ack["status"] == "SUCCESS"
        assert last_seen_ack["last_seen"] is not None

    print("\n==================================================")
    print("🎉 ALL WEBSOCKET APIS TESTED & PASSED!")
    print("==================================================")


if __name__ == "__main__":
    run_websocket_tests()
