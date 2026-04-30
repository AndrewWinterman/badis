# Badis Design

**Goal:** Build a Redis-compatible server using BadgerDB for storage and hashicorp/raft for replication.

## Architecture

1.  **Network Layer:** Accepts TCP connections, parses RESP (Redis Serialization Protocol) using an existing library (e.g., `tidwall/redcon` or similar).
2.  **Raft Layer (`hashicorp/raft`):** Handles consensus. Write commands are proposed to the Raft cluster. Read commands can be served locally (with stale reads) or routed through Raft for strong consistency.
3.  **Storage Layer (BadgerDB):** Serves as both the FSM (Finite State Machine) and the Raft LogStore/StableStore. Applies committed log entries to FSM keys.
4.  **Command Execution:** Translates Redis commands to BadgerDB operations.

## Components

*   **Server:** Manages TCP listener, RESP parsing, and routing commands.
*   **RaftNode:** Wraps `hashicorp/raft`, manages peer connections, elections, and log replication.
*   **Store (FSM):** Implements `raft.FSM`. Reads/writes to BadgerDB.
    *   Keys/Values: Directly map to BadgerDB keys.
    *   Hashes/Lists/Sets: Need schema encoding in BadgerDB (e.g., `hash:key:field` -> `value`).

## Data Flow (Write)

1.  Client sends `SET foo bar`.
2.  Server parses RESP.
3.  Server proposes command to RaftNode.
4.  Raft replicates log to peers.
5.  Upon commit, Raft applies log to FSM (Store).
6.  Store executes `txn.Set([]byte("foo"), []byte("bar"))` in BadgerDB.
7.  Server responds `+OK` to client.

## Data Flow (Read)

1.  Client sends `GET foo`.
2.  Server parses RESP.
3.  (Option 1: Stale) Store reads from BadgerDB directly.
4.  (Option 2: Strong) Server proposes read index to Raft, then reads BadgerDB.
5.  Server formats BadgerDB result as RESP and sends to client.

## Initial Supported Commands

*   **Key/Value:** GET, SET, DEL, EXISTS
*   **Hashes:** HGET, HSET, HDEL, HGETALL
*   **Lists:** LPUSH, LPOP, LRANGE
*   **Sets:** SADD, SREM, SMEMBERS

## Trade-offs & Decisions

*   **RESP Parser:** Using an existing library speeds up development but might limit custom connection handling.
*   **Data Structures in BadgerDB:** Implementing Hashes/Lists/Sets requires careful key design to avoid scanning the entire DB. We'll use prefixes (e.g., `H:{key}:{field}`, `L:{key}:{seq}`, `S:{key}:{member}`).

## Testing

*   Unit tests for command parsing and BadgerDB encoding.
*   Integration tests spinning up 3-node clusters and running Redis-cli commands against them.