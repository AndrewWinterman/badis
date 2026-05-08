# Integrated Router and Gossip Cluster Design

## Goal
Integrate proxy routing directly into all Data Nodes, replacing the standalone proxy. Add a gossip protocol (`memberlist`) for cluster topology awareness and support dynamic data migration.

## 1. Architecture & Roles

The system uses a single binary with two logical roles:
*   **Config Node:** Participates in a dedicated Raft group (`config-raft`). Acts as the source of truth for the global routing table (Slot Map) and migration state.
*   **Data Node:** Participates in a data Raft group (`shard-N-raft`). Stores actual KV data in BadgerDB.
*   **Hybrid Node:** A single process can be both (e.g., first 3 nodes handle config and act as Shard 1).

**Routing:** Every node (Config or Data) accepts client RESP connections. The node hashes the key, checks its local cached Slot Map, and routes the request to the correct Data Leader (or handles it locally).

## 2. Gossip and Topology Sync

*   **Gossip Ring:** All nodes join a global `hashicorp/memberlist` gossip pool.
*   **Local Cache:** Every node maintains a read-only cache of the `SlotMap` (Slot -> Shard -> IP).
*   **Sync Mechanism:**
    1.  Config Raft updates `SlotMap` and increments `ConfigVersion`.
    2.  Config node broadcasts new `ConfigVersion` via gossip.
    3.  Data nodes receive gossip update, see higher version, and pull new `SlotMap` via internal RPC from a Config node.
*   **Stale Cache Fallback:** If a Data Node routes using a stale cache, the receiving node rejects it (internal `MOVED` error). The router fetches the fresh config and retries.

## 3. Data Migration (Using Redis Replication Protocol)

*   **Trigger:** Admin initiates migration (Slot X: Shard A -> Shard B) via Config Raft.
*   **Protocol:** Use standard Redis replication protocol (`SYNC` / `PSYNC` style).
    1.  Shard B (Target) connects to Shard A (Source) pretending to be a replica for the specific slot.
    2.  Shard A generates a BadgerDB snapshot for Slot X and streams it as a bulk string (like an RDB file).
    3.  Shard A buffers incoming writes for Slot X during transfer.
    4.  Shard A streams buffered writes to Shard B using standard RESP write commands.
    5.  Once caught up, Config Raft updates `SlotMap` owner to Shard B.
    6.  Shard A drops Slot X data.

## 4. Components & Configuration

*   **Remove:** Delete `BADIS_PROXY_MODE`. Every node gets router logic.
*   **`cluster` pkg:** Wraps `memberlist`. Handles join, failure detection, and version broadcast.
*   **`config` pkg:** Raft FSM for metadata. Stores `SlotMap`.
*   **`router` pkg:** Parses RESP, hashes keys, checks local cache. Forwards via TCP pool if remote.
*   **`migration` pkg:** Handles Redis-style `SYNC` for slots between shards.
*   **Env Vars:** Add `BADIS_GOSSIP_PORT` (e.g., 7946) and `BADIS_JOIN` (comma-separated IPs for bootstrap).
