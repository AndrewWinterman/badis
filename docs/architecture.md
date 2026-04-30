# Badis Architecture: Raft and BadgerDB Integration

Badis is a Redis-compatible distributed key-value store. It achieves high availability and strong consistency by replicating data across multiple nodes using the **Raft consensus algorithm** (via HashiCorp's Raft library). 

To persist data efficiently, Badis uses **BadgerDB**, an embeddable, fast key-value database written in Go. A unique aspect of Badis's design is how it leverages a *single* BadgerDB instance to serve three distinct roles required by Raft: the **FSM**, the **LogStore**, and the **StableStore**.

## The Three Pillars of Raft Storage

HashiCorp's Raft implementation requires the host application to provide specific storage interfaces. Badis implements all of these on top of BadgerDB using key prefixes to isolate the data.

### 1. LogStore (Prefix: `L`)
The LogStore is an append-only log of Raft commands (e.g., "SET key=value"). 
* **Purpose:** When a client sends a write request to the leader, the leader appends this command to its LogStore before replicating it to followers. Followers also append it to their LogStores.
* **Interaction:** Raft constantly reads from and writes to the LogStore during replication. Once a log entry is safely replicated to a quorum of nodes, it is considered "committed."
* **Badger Implementation:** Logs are serialized using MessagePack and stored in Badger with an `L` prefix followed by the log index (an 8-byte integer). This allows efficient sequential reads and reverse iteration (to find the `LastIndex`).

### 2. StableStore (Prefix: `S`)
The StableStore holds critical, infrequently changing Raft metadata.
* **Purpose:** It stores the `CurrentTerm` (to detect stale leaders) and `VotedFor` (to prevent voting twice in the same term). 
* **Interaction:** Raft updates this store during leader elections. It must be strictly durable to ensure safety guarantees across node crashes.
* **Badger Implementation:** Stored as simple key-value pairs with an `S` prefix.

### 3. FSM - Finite State Machine (Prefix: `D` or un-prefixed)
The FSM represents the actual state of the application—the Redis data.
* **Purpose:** Once a log entry in the LogStore is committed, Raft "applies" it to the FSM. 
* **Interaction:** The `FSM.Apply()` method takes the committed log entry, deserializes the command (e.g., `SET foo bar`), and mutates the actual dataset. Read requests (like `GET`) typically query the FSM directly.
* **Badger Implementation:** Redis keys are stored in Badger. Because Badis uses a single database, the FSM logic executes standard Badger transactions to update the state.

## The Lifecycle of a Command

Here is how the components interact during a standard write operation (e.g., `SET mykey myval`):

1. **Proposal:** The network server receives the command and calls `raft.Apply(command)`.
2. **Log Append:** The Raft leader assigns the command an index and writes it to the **LogStore** in BadgerDB.
3. **Replication:** The leader sends the log entry to followers via RPC. Followers write it to their own **LogStore**s.
4. **Commit:** Once a quorum of nodes acknowledge the log, the leader marks it as committed.
5. **FSM Application:** Raft passes the committed log entry to the `Apply(log *raft.Log)` method of the **FSM**. 
6. **Data Mutated:** The FSM interprets the `SET` command and writes `mykey=myval` into BadgerDB.
7. **Response:** The server returns `+OK` to the client.

## Log Compaction (Snapshots)

If the LogStore grew indefinitely, it would consume all disk space. Raft handles this via snapshots.
1. Raft periodically asks the FSM to take a snapshot of its current state.
2. Badis uses BadgerDB's built-in backup functionality to stream the FSM data to Raft's SnapshotStore.
3. Once the snapshot is saved, Raft calls `DeleteRange` on the **LogStore** to remove the old logs that are now safely captured in the snapshot, reclaiming space in BadgerDB.