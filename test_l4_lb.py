import asyncio
import json
import websockets


async def main():
    print("==================================================")
    print("⚖️ TESTING HAProxy L4 LOAD BALANCER & CHAT SERVERS")
    print("==================================================\n")

    uri_alice = "ws://localhost:8000/ws/client_alice?user_id=alice"
    uri_bob = "ws://localhost:8000/ws/client_bob?user_id=bob"

    # Connect Alice and Bob through HAProxy L4 Load Balancer (port 8000)
    async with websockets.connect(uri_alice) as ws_alice:
        welcome_alice = json.loads(await ws_alice.recv())
        print(f"✅ Alice connected via L4 LB -> Routed to backend: {welcome_alice['server_id']}")

        async with websockets.connect(uri_bob) as ws_bob:
            welcome_bob = json.loads(await ws_bob.recv())
            print(f"✅ Bob connected via L4 LB -> Routed to backend: {welcome_bob['server_id']}")

            # Notice HAProxy L4 round-robin distributes connections across different chat servers
            print(f"\n📊 Connections distributed: Alice -> {welcome_alice['server_id']} | Bob -> {welcome_bob['server_id']}")

            # 1. Alice creates chat with Bob
            print("\n--- 💬 Alice creates chat ---")
            await ws_alice.send(json.dumps({
                "action": "createChat",
                "chat_name": "Distributed System Sync",
                "participant_ids": ["bob"]
            }))
            chat_ack = json.loads(await ws_alice.recv())
            print(f"Chat created: '{chat_ack['chat_name']}' (ID: {chat_ack['chat_id']})")
            chat_id = chat_ack["chat_id"]

            # 2. Alice sends message
            print("\n--- ✉️ Alice sends message ---")
            await ws_alice.send(json.dumps({
                "action": "sendMessage",
                "chat_id": chat_id,
                "content": "Real-time message delivered across servers via L4 LB + Redis + PostgREST!",
                "message_type": "TEXT"
            }))
            send_ack = json.loads(await ws_alice.recv())
            print("Alice received message ACK from server:", send_ack)

            # 3. Bob receives message in real time across the cluster
            print("\n--- 📥 Bob receives message ---")
            bob_msg = json.loads(await ws_bob.recv())
            print(f"Bob received frame: [{bob_msg['type']}] '{bob_msg['content']}' (Seq: {bob_msg['seq_num']})")

            # 4. Bob sends delivery acknowledgement
            print("\n--- ✅ Bob acknowledges message ---")
            await ws_bob.send(json.dumps({
                "action": "acknowledgeMessage",
                "message_id": bob_msg["message_id"]
            }))
            ack_resp = json.loads(await ws_bob.recv())
            print("Bob ACK response:", ack_resp)

            # 5. Bob sends Pong heartbeat
            await ws_bob.send(json.dumps({
                "action": "pong",
                "seq_num": bob_msg["seq_num"]
            }))

            # 6. Alice queries Last Seen
            print("\n--- 🕒 Alice queries Bob's Last Seen ---")
            await ws_alice.send(json.dumps({
                "action": "getLastSeen",
                "user_id": "bob"
            }))
            last_seen_resp = json.loads(await ws_alice.recv())
            print(f"Bob last seen timestamp: {last_seen_resp['last_seen']}")

    print("\n==================================================")
    print("🎉 FULL L4 BALANCED DEPLOYMENT VERIFIED!")
    print("==================================================")


if __name__ == "__main__":
    asyncio.run(main())
