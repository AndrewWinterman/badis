# Consistent Hashing and Sharding Evaluation for Badis

## 1. Architecture for Sharding (Multiple Raft Groups)
To scale out linearly, Badis will transition from a single Raft cluster to a **Multi-Raft architecture**.
* **Independent Shards:** The data space is partitioned across multiple independent Raft clusters (shards).
* **Shard Composition:** Each shard consists of its own Raft consensus group (e.g., 3 or 5 nodes with a designated Leader and Followers).
* **Isolation:** Shards do not share state. A node can participate in one or multiple Raft groups (often called a Multi-Raft node), but logically, the consensus logs and state machines are strictly isolated.
* **Coordination:** A lightweight external coordinator or a designated configuration shard is required to maintain the global routing table (which shard owns which hash slots).

## 2. Routing Strategy: The Proxy Layer Approach
We evaluate a **Proxy-based routing** strategy over client-side or server-side (Redis Cluster) routing.

### Mechanism
* **Stateless Proxies:** A layer of stateless proxy services sits between the clients and the Badis Raft clusters.
* **Protocol Translation:** Clients connect to the proxy using the standard standalone Redis protocol (RESP). They do not need to be cluster-aware.
* **Request Forwarding:** The proxy parses the RESP command, extracts the key, hashes it, consults its cached routing table, and forwards the request to the *Leader* of the appropriate Raft shard.
* **Response:** The proxy receives the response from the Raft shard and relays it back to the client.

### Advantages of Proxy Routing
* **Client Compatibility:** 100% compatibility with existing standalone Redis clients (no need for Redis Cluster client support).
* **Connection Management:** Proxies can multiplex connections to the backend Raft nodes, reducing connection overhead on the storage layer.
* **Simplified Storage Nodes:** Raft nodes only care about consensus and storage, not connection routing or cluster gossiping.

### Disadvantages
* **Latency:** Introduces an extra network hop.
* **Bottleneck:** Proxies require high network I/O and CPU to parse/forward packets at line rate.

## 3. Consistent Hashing Ring Implementation Details
To map keys to Raft shards evenly, Badis will use a **Consistent Hashing Ring** with virtual nodes.

* **Hash Function:** Use a fast, non-cryptographic hash function with good avalanche properties, such as **MurmurHash3** or **xxHash**.
* **Ring Space:** The output space (e.g., 32-bit integer, 0 to 4,294,967,295) forms a logical ring.
* **Virtual Nodes (VNodes):** To prevent data skew when the number of physical shards is small, each physical Raft shard is represented by multiple Virtual Nodes on the ring (e.g., 256 or 512 vnodes per physical shard).
* **Key Mapping:** A key is hashed to a point on the ring. The system moves clockwise to find the nearest VNode, which maps back to the physical Raft shard responsible for that data.
* **Metadata Sync:** Proxies pull the VNode-to-Shard mapping from the central configuration store (or configuration Raft group) and cache it locally.

## 4. Trade-offs and Challenges

### Cross-Shard Transactions
* **Issue:** Commands that touch multiple keys (e.g., `MSET`, `MGET`, `DEL`, or Lua scripts) might resolve to keys residing on different Raft shards.
* **Solution:** 
  1. **Strict Rejection (Recommended):** The proxy rejects cross-shard operations with a `CROSSSLOT` error, forcing developers to use Hash Tags (e.g., `user:{100}:profile` and `user:{100}:settings` ensures `{100}` is the hashed component).
  2. **Proxy Scatter/Gather:** For simple multi-key reads (`MGET`), the proxy can split the request, fetch from multiple shards in parallel, and reassemble the response.
  3. **Distributed Transactions:** Implementing Two-Phase Commit (2PC) across Raft groups for multi-key writes is highly complex and severely impacts latency. This should be avoided.

### Resharding and Data Migration
* **Issue:** Adding or removing a Raft shard requires moving VNodes and transferring the underlying data without downtime.
* **Solution:**
  * **Pre-sharding:** Start with a fixed number of logical slots (e.g., 16384 like Redis) mapped to the Raft shards, rather than raw consistent hashing. This makes state transfer easier to track per slot.
  * **Migration State:** During a slot migration, the proxy must be aware of the "migrating" state.
  * Writes to the migrating slot are either routed to the target shard or double-written.
  * Reads are attempted on the target first, falling back to the source if the key hasn't migrated yet.
  * Raft nodes need a mechanism to take a snapshot of a specific VNode/Slot and stream it to the new Raft group.
