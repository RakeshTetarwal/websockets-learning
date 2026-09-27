# Distributed Real-Time Chat Engine (WhatsApp Architecture)

A production-grade, horizontally scalable, distributed real-time chat infrastructure built with **Python (FastAPI & WebSockets)**, **HAProxy (Layer 4 TCP Load Balancing)**, **PostgreSQL & PostgREST (ACID Persistence)**, and **Redis (Pub/Sub & In-Memory Session Registry)**.

---

## 🏛️ System Architecture

```
                                  [ Client A ]         [ Client B ]
                                       │                    │
                                       ▼                    ▼
                        ┌──────────────────────────────────────────┐
                        │        HAProxy Layer 4 Load Balancer     │
                        │               (Port :8000)               │
                        └───────────────────┬──────────────────────┘
                                            │
                             ┌──────────────┴──────────────┐
                             ▼                             ▼
                ┌─────────────────────────┐   ┌─────────────────────────┐
                │   Chat Server Node 1    │   │   Chat Server Node 2    │
                │    (FastAPI/Uvicorn)    │   │    (FastAPI/Uvicorn)    │
                └───────────┬─────────────┘   └─────────────┬───────────┘
                            │                               │
             ┌──────────────┴───────────────────────────────┴──────────────┐
             ▼                                                             ▼
┌───────────────────────────┐                               ┌───────────────────────────┐
│           Redis           │                               │   PostgreSQL + PostgREST  │
│  - Session Registry       │                               │  - Atomic Sequences       │
│  - Inter-Server Pub/Sub   │                               │  - Messages & Chats       │
│  - L1 Sequence Cache      │                               │  - Store-and-Forward      │
│  - Last Seen Watermarks   │                               │    Inbox Mailbox          │
└───────────────────────────┘                               └───────────────────────────┘
```

---

## 🚀 Key Architectural Highlights

### 1. Atomic Monotonic Sequences (Dual-Write Prevention)
To prevent the classic distributed dual-write bug (where a sequence counter advances in memory but the database write fails), the system generates sequence numbers and persists inbox entries in a **single ACID PostgreSQL transaction**:
* Stored Procedure: `add_inbox_entry_with_seq`
* Atomically increments the persistent `user_sequences` table and inserts into the recipient's `inbox` queue in one database operation.
* Zero risk of sequence "ghost gaps" or orphaned counters upon server crash.

### 2. Two-Tier Ping/Pong Protocol
* **Layer 1: Transport Keepalive (RFC 6455)**
  * Low-level control frames (`0x9` PING and `0xA` PONG) handled by Uvicorn to prevent NAT/firewall idle connection drops.
* **Layer 2: Application Heartbeats (WhatsApp Protocol)**
  * JSON frames carrying `server_seq` watermarks.
  * Fast-path: Reads from Redis in-memory cache (<0.2ms).
  * Gap Detection: If `client_seq < server_seq`, the server immediately queries PostgreSQL and flushes missed messages from the `Inbox`.

### 3. Layer 4 TCP Load Balancing (HAProxy)
* Preserves persistent full-duplex WebSocket connections.
* Round-robin distributes client connections across multiple backend chat servers.
* Servers route messages across instances using dedicated Redis Pub/Sub channels (`channel:{target_server_id}`).

### 4. Decoupled Command Pattern Handlers
* Clean extensible command dispatcher (`ActionHandlerFactory`).
* Discrete handlers for `createChat`, `sendMessage`, `acknowledgeMessage`, `pong`, `getLastSeen`, and `requestMediaUrl`.

---

## 📂 Repository Structure

```
├── app.py                 # FastAPI application, WebSocket lifecycle, and dependency wiring
├── chat_server.py         # Core chat orchestration, fanout, sequence assignment, and mailbox delivery
├── handlers.py            # Command pattern action handlers (CreateChat, SendMessage, Ack, etc.)
├── models.py              # Core dataclasses and protocol enums (User, Client, Chat, Message, InboxEntry)
├── interfaces.py          # Abstract interfaces for repositories, session registries, and brokers
├── storage.py             # PostgREST repositories and pure Redis session & pub/sub broker
├── haproxy.cfg            # HAProxy Layer 4 TCP load balancer configuration
├── init.sql               # PostgreSQL schema, stored procedures, and access grants
├── docker-compose.yml     # Multi-container orchestration (Postgres, PostgREST, Redis, HAProxy, 2x Chat Servers)
├── Dockerfile             # High-performance Python container image with uv
├── requirements.txt       # Production dependencies
│
├── test_websocket.py      # Starlette TestClient in-memory WebSocket test suite
├── test_l4_lb.py          # Real TCP integration test through HAProxy L4 load balancer
├── test_chat.py           # Multi-client interactive test script
├── test_futures.py        # Educational test exploring asyncio.Future vs concurrent.futures.Future
└── test_dns.py            # Async DNS resolution benchmark
```

---

## 🛠️ Quickstart & Deployment

### 1. Prerequisites
* [Docker & Docker Compose](https://www.docker.com/)
* [Python 3.11+](https://www.python.org/)
* [uv](https://github.com/astral-sh/uv) (recommended) or `pip`

### 2. Launch the Infrastructure
Start the entire 6-container cluster (PostgreSQL, PostgREST, Redis, HAProxy, and 2 Chat Server nodes):

```bash
docker compose up -d --build
```

Verify that all containers are healthy:
```bash
docker ps
```

| Service | Port | Description |
| :--- | :--- | :--- |
| **HAProxy L4 Load Balancer** | `:8000` | Public WebSocket entrypoint (`ws://localhost:8000/ws/{client_id}`) |
| **PostgREST** | `:3000` | REST API layer over PostgreSQL |
| **PostgreSQL** | `:5432` | Relational database |
| **Redis** | `:6380` | In-memory session registry and broker |

---

## 🧪 Running Tests

### Integration Test: Layer 4 HAProxy Multi-Server Cluster
Tests end-to-end distributed messaging across separate chat server instances routed through HAProxy:

```bash
python test_l4_lb.py
```

### Local WebSocket API Test Suite
Tests all action handlers, offline inbox store-and-forward, and heartbeats:

```bash
python test_websocket.py
```

### Async Internals & Futures Benchmark
Demonstrates event loop internals and thread pool bridging:

```bash
python test_futures.py
```

---

## 📜 Protocol Frame Specification

### Client Connect Handshake
* **Request:** `ws://localhost:8000/ws/{client_id}?user_id={user_id}`
* **Server ACK:**
```json
{
  "type": "ACK",
  "action": "connected",
  "server_id": "chat_server_1",
  "client_id": "client_alice",
  "user_id": "alice",
  "server_seq": 42
}
```

### Send Message
* **Client Request:**
```json
{
  "action": "sendMessage",
  "chat_id": "c1f7b889-...",
  "content": "Hello Bob!",
  "message_type": "TEXT"
}
```
* **Recipient Delivery Frame:**
```json
{
  "type": "MESSAGE",
  "message_id": "903ebf32-...",
  "chat_id": "c1f7b889-...",
  "sender_id": "alice",
  "content": "Hello Bob!",
  "message_type": "TEXT",
  "timestamp": 1790502824.94,
  "seq_num": 43
}
```

### Heartbeat (Ping / Pong)
* **Server Ping:** `{"type": "PING", "server_seq": 43}`
* **Client Pong:** `{"action": "pong", "seq_num": 43}`
*(If `client_seq < server_seq`, the server automatically flushes pending messages from the `Inbox` table).*
