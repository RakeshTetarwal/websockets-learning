# test_chat.py


from app import app

# pyrefly: ignore [missing-import]
from starlette.testclient import TestClient

client = TestClient(app)

with client.websocket_connect("/ws/client_alice?user_id=alice") as alice_ws, \
        client.websocket_connect("/ws/client_bob?user_id=bob") as bob_ws:
        alice_connection_ack = alice_ws.receive_json()
        bob_connection_ack = bob_ws.receive_json()

        print(alice_connection_ack)
        print(bob_connection_ack)

    
