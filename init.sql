CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'web_anon') THEN
        CREATE ROLE web_anon NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO web_anon;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    last_seen DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
);

-- Clients table
CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE
);

-- Chats table
CREATE TABLE IF NOT EXISTS chats (
    chat_id TEXT PRIMARY KEY,
    chat_name TEXT NOT NULL
);

-- Chat Participants table
CREATE TABLE IF NOT EXISTS chat_participants (
    chat_id TEXT REFERENCES chats(chat_id) ON DELETE CASCADE,
    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
    PRIMARY KEY (chat_id, user_id)
);

-- Messages table
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    chat_id TEXT REFERENCES chats(chat_id) ON DELETE CASCADE,
    sender_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    message_type TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    seq_num BIGINT DEFAULT 0
);

-- Inbox table (Store-and-forward queue for undelivered messages)
CREATE TABLE IF NOT EXISTS inbox (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    content TEXT NOT NULL,
    message_type TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    seq_num BIGINT DEFAULT 0
);

-- Media table (Stores media files inside PostgreSQL as BYTEA)
CREATE TABLE IF NOT EXISTS media (
    file_id TEXT PRIMARY KEY,
    user_id TEXT,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    data BYTEA,
    created_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
);

-- User Sequences table (Durable, persistent monotonic sequence per user)
CREATE TABLE IF NOT EXISTS user_sequences (
    user_id TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    last_seq_num BIGINT NOT NULL DEFAULT 0
);

-- Stored procedure for atomic sequence increments
CREATE OR REPLACE FUNCTION increment_user_sequence(p_user_id TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    new_seq BIGINT;
BEGIN
    INSERT INTO user_sequences (user_id, last_seq_num)
    VALUES (p_user_id, 1)
    ON CONFLICT (user_id)
    DO UPDATE SET last_seq_num = user_sequences.last_seq_num + 1
    RETURNING last_seq_num INTO new_seq;

    RETURN new_seq;
END;
$$;

-- Atomic transaction: Increments user sequence and inserts into inbox in a single ACID transaction
CREATE OR REPLACE FUNCTION add_inbox_entry_with_seq(
    p_message_id TEXT,
    p_client_id TEXT,
    p_chat_id TEXT,
    p_sender_id TEXT,
    p_content TEXT,
    p_message_type TEXT,
    p_timestamp DOUBLE PRECISION,
    p_user_id TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    new_seq BIGINT;
BEGIN
    -- Ensure recipient user exists in users table
    INSERT INTO users (user_id, name)
    VALUES (p_user_id, p_user_id)
    ON CONFLICT (user_id) DO NOTHING;

    -- 1. Atomically increment sequence for recipient user
    INSERT INTO user_sequences (user_id, last_seq_num)
    VALUES (p_user_id, 1)
    ON CONFLICT (user_id)
    DO UPDATE SET last_seq_num = user_sequences.last_seq_num + 1
    RETURNING last_seq_num INTO new_seq;

    -- 2. Insert into inbox with that exact sequence number
    INSERT INTO inbox (
        message_id,
        client_id,
        chat_id,
        sender_id,
        content,
        message_type,
        timestamp,
        seq_num
    )
    VALUES (
        p_message_id,
        p_client_id,
        p_chat_id,
        p_sender_id,
        p_content,
        p_message_type,
        p_timestamp,
        new_seq
    );

    -- 3. Return the allocated sequence number
    RETURN new_seq;
END;
$$;

-- Grant full table & sequence access to web_anon
GRANT ALL ON ALL TABLES IN SCHEMA public TO web_anon;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO web_anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO web_anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO web_anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO web_anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO web_anon;


