# Badis

Badis is a highly available, Redis-compatible distributed key-value store written in Go. It combines the ease of use of the Redis protocol with strong consistency via Raft consensus and high-performance persistent storage via BadgerDB.

## Architecture

Badis nodes operate as both data servers and intelligent routers within a cluster. The standalone proxy node has been removed in favor of a fully decentralized architecture.

### Integrated Router & Data Node
Each Badis node listens for Redis commands, replicates them across its replica set using Hashicorp Raft, and applies operations to a local BadgerDB storage engine.
- **Protocol:** Uses `tidwall/redcon` to parse the Redis Serialization Protocol (RESP), allowing you to use standard Redis clients (e.g., `redis-cli`, `go-redis`).
- **Consensus:** Uses Hashicorp Raft to ensure that all data is strongly consistent, fault-tolerant, and safely replicated to a quorum of nodes before acknowledging a write.
- **Storage:** Persists state to disk using BadgerDB, a fast, embeddable LSM-tree based key-value database.
- **Routing & Gossip:** Nodes use Hashicorp `memberlist` (a gossip protocol) to discover peers and synchronize a global routing table (`SlotMap`). When a node receives a command for a key that belongs to a different shard, it automatically proxies the request to the correct node.

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
| `BADIS_JOIN` | Comma-separated list of node addresses (IP:GossipPort) to join an existing cluster. | `""` |

### Running a Node

```bash
# Start a standalone Badis server
BADIS_PORT=":6379" BADIS_GOSSIP_PORT="7946" BADIS_DATA_DIR="./node-1" go run main.go

# Start a second node and join the cluster
BADIS_PORT=":6380" BADIS_GOSSIP_PORT="7947" BADIS_JOIN="localhost:7946" BADIS_DATA_DIR="./node-2" go run main.go
```

## Built With
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value DB in Go.
- [Hashicorp Raft](https://github.com/hashicorp/raft) - Distributed consensus.
- [Redcon](https://github.com/tidwall/redcon) - Custom Redis server framework.
- [Consistent](https://github.com/buraksezer/consistent) - Consistent hashing with bounded loads.