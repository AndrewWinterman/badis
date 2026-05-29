# Raft Wiring & Cluster Chaos Testing Design

## Goal
Wire up the missing Hashicorp Raft initialization in `main.go` so nodes actually replicate data. Finish the Jepsen-style linearizability test to prove consistency under network partitions/node crashes. Document the scale-up process.

## 1. Architecture: Gossip vs Raft Ports
The system uses three distinct network layers:
1. **Redis Protocol (`BADIS_PORT`):** Client traffic (e.g., `6379`).
2. **Gossip Protocol (`BADIS_GOSSIP_PORT`):** Global cluster membership and `SlotMap` routing (e.g., `7946`).
3. **Raft Protocol (`BADIS_RAFT_PORT`):** Local shard consensus and data replication. (New variable, default `8300`).

## 2. Shard Assignment Strategy
To prevent the entire database from forming one massive, unscalable Raft cluster, nodes must be assigned to specific shards.
*   **Variable:** Introduce `BADIS_SHARD_ID` (default: `shard-1`).
*   **Isolation:** A node only participates in Raft consensus with other nodes sharing the exact same `BADIS_SHARD_ID`.

## 3. Raft Bootstrapping Flow
When a node starts in `main.go`:
1.  Initialize TCP Transport on `BADIS_RAFT_PORT`.
2.  Start Gossip on `BADIS_GOSSIP_PORT`.
3.  Check Gossip state:
    *   **Auto-Bootstrap:** If no existing peers in the Gossip ring share the same `BADIS_SHARD_ID`, assume this is the first node of the shard. Call `raft.BootstrapCluster`.
    *   **Join Existing:** If peers *do* exist for this `BADIS_SHARD_ID`, discover the Raft leader. Connect and request to be added to the Raft replica set via `AddVoter`.

## 4. Scaling Up (Adding Nodes)
*   **Scaling a Shard (Higher Availability):** Start a new node with an existing `BADIS_SHARD_ID` (e.g., `shard-1`). It joins the Raft replica set, increasing fault tolerance.
*   **Adding a Shard (Higher Capacity):** Start a group of nodes with a new `BADIS_SHARD_ID` (e.g., `shard-2`). They form a new Raft cluster. The config layer detects the new shard and automatically triggers a background slot migration, streaming data from existing shards to the new one to rebalance the load.

## 5. Chaos Testing (Linearizability)
Complete the Jepsen-style test in `test/chaos`:
*   Spawn 3 nodes sharing `BADIS_SHARD_ID="shard-1"`.
*   Connect `go-redis` clients and generate a high-concurrency stream of `SET` and `GET` operations. Log every request and response.
*   Inject a fault by abruptly killing the Raft leader (`SIGKILL` equivalent).
*   Wait for the cluster to elect a new leader and heal.
*   Pass the operation history to `github.com/anishathalye/porcupine` to mathematically verify the database remained strongly consistent (linearizable) despite the crash.