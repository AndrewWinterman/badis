# Badis

Badis is a highly available, Redis-compatible distributed key-value store written in Go. It combines the ease of use of the Redis protocol with strong consistency via Raft consensus and high-performance persistent storage via BadgerDB.

## Architecture

Badis nodes operate as both data servers and intelligent routers within a fully decentralized cluster.

### Integrated Router & Data Node
Each Badis node listens for Redis commands, replicates them across its replica set using Hashicorp Raft, and applies operations to a local BadgerDB storage engine.
- **Protocol:** Uses `tidwall/redcon` to parse the Redis Serialization Protocol (RESP), allowing you to use standard Redis clients (e.g., `redis-cli`, `go-redis`).
- **Consensus:** Uses Hashicorp Raft to ensure that all data is strongly consistent, fault-tolerant, and safely replicated to a quorum of nodes before acknowledging a write.
- **Storage:** Persists state to disk using BadgerDB, a fast, embeddable LSM-tree based key-value database.
- **Routing & Gossip:** Nodes use Hashicorp `memberlist` (a gossip protocol) to discover peers and synchronize a global routing table (`SlotMap`). When a node receives a command for a key that belongs to a different shard, it automatically forwards the request to the correct node.

## Features & Behavior

- **Drop-in Redis Compatibility:** Badis implements core Redis string commands. You can swap Redis out for Badis in your application for the supported commands without changing client code.
- **Supported Commands:**
  - `GET key`
  - `DEL key`
  - `PING`, `HELLO`
  - `SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp]`
- **TTL & Expiry:** Full support for standard Redis TTL options on keys.
- **Persistence:** Unlike default Redis (which is primarily in-memory), Badis is persistent by default through BadgerDB.
- **High Availability:** If a leader node fails, the Raft cluster will automatically elect a new leader and continue serving requests without data loss.

## Usage & Configuration

Badis is configured entirely through Environment Variables.

| Environment Variable | Description | Default |
| :--- | :--- | :--- |
| `BADIS_PORT` | The port the server listens on for Redis commands. | `:6379` |
| `BADIS_DATA_DIR` | Directory to store BadgerDB data and Raft logs. | `badis-data` |
| `BADIS_GOSSIP_PORT` | The port used for the `memberlist` gossip protocol. | `7946` |
| `BADIS_RAFT_PORT` | The port used for the Raft consensus protocol. | `8300` |
| `BADIS_JOIN` | Comma-separated list of node addresses (IP:GossipPort) to join an existing cluster. | `""` |
| `BADIS_SHARD_ID` | The ID of the shard this node belongs to. | `shard-1` |

### Running a Node

```bash
# Start a standalone Badis server (automatically bootstraps shard-1)
BADIS_PORT=":6379" BADIS_GOSSIP_PORT="7946" BADIS_RAFT_PORT="8300" BADIS_DATA_DIR="./node-1" BADIS_SHARD_ID="shard-1" go run main.go

# Start a second node and join the cluster (joins existing shard-1 Raft replica set)
BADIS_PORT=":6380" BADIS_GOSSIP_PORT="7947" BADIS_RAFT_PORT="8301" BADIS_JOIN="localhost:7946" BADIS_DATA_DIR="./node-2" BADIS_SHARD_ID="shard-1" go run main.go
```

### Cluster Membership & Sharding

Badis uses a two-tier topology combining global gossip and local consensus:

- **Gossip (Global):** All nodes join a single `memberlist` gossip ring. This layer shares the global routing table (`SlotMap`), allowing any node to discover which shard owns a specific key and which node is the current Raft leader for that shard.
- **Raft (Local):** Nodes are grouped into independent shards via `BADIS_SHARD_ID`. A node only participates in the Raft consensus for its specific shard. This ensures strong consistency for its assigned data without the overhead of replicating every cluster operation globally.

**Joining / Scaling:**
- **Scaling a Shard (High Availability):** Start a new node with an existing `BADIS_SHARD_ID` (e.g., `shard-1`), providing an existing node in `BADIS_JOIN`. It connects to gossip, discovers the `shard-1` Raft leader, and requests to join the replica set to sync data.
- **Adding a Shard (Capacity):** Start 3 new nodes with a new `BADIS_SHARD_ID` (e.g., `shard-2`). They form a new Raft cluster. The global cluster detects the new shard via gossip and automatically triggers slot migration, streaming data from existing shards to rebalance the load.

**Leaving:** 
- **Graceful:** Sending a `SIGTERM` triggers a graceful leave broadcast. The node steps down from Raft leadership (if applicable) and remaining nodes immediately update their routing tables to direct traffic away.
- **Failures:** Gossip protocol health checks automatically detect ungraceful crashes. Dead nodes are evicted from the global routing table, while the local Raft cluster automatically elects a new leader for the affected shard without data loss.

## End-to-End Testing

Badis includes a black-box end-to-end (e2e) test suite written in Go (`test/e2e/e2e_test.go`). The tests interact with the cluster over the standard Redis protocol using `go-redis`. 

To run e2e tests in Kubernetes:
1. The test binary (`badis-e2e`) is compiled and packaged in the `badis` Docker image.
2. Run `scripts/deploy.sh --context <your-context>` (add `--kind` if using a local Kind cluster). This builds the image, applies the Kubernetes manifests, and spawns a dedicated test Pod.
3. The test Pod routes traffic through the cluster, verifying end-to-end functionality before exiting 0 on success.

## Built With
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value DB in Go.
- [Hashicorp Raft](https://github.com/hashicorp/raft) - Distributed consensus.
- [Redcon](https://github.com/tidwall/redcon) - Custom Redis server framework.
- [Consistent](https://github.com/buraksezer/consistent) - Consistent hashing with bounded loads.