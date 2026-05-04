# Badis

Badis is a highly available, Redis-compatible distributed key-value store written in Go. It combines the ease of use of the Redis protocol with strong consistency via Raft consensus and high-performance persistent storage via BadgerDB.

## Architecture

Badis operates in two primary modes: **Server** and **Proxy**.

### Server Node
The core Badis engine. A Server node listens for Redis commands, replicates them across a cluster using Hashicorp Raft, and applies the operations to a local BadgerDB storage engine. 
- **Protocol:** Uses `tidwall/redcon` to parse the Redis Serialization Protocol (RESP), allowing you to use standard Redis clients (e.g., `redis-cli`, `go-redis`).
- **Consensus:** Uses Hashicorp Raft to ensure that all data is strongly consistent, fault-tolerant, and safely replicated to a quorum of nodes before acknowledging a write.
- **Storage:** Persists state to disk using BadgerDB, a fast, embeddable LSM-tree based key-value database.

### Proxy Node (Sharding)
A stateless routing layer used to horizontally scale your Badis deployment.
- **Consistent Hashing:** Uses `buraksezer/consistent` and `xxhash` to deterministically distribute keys across multiple backend Badis Server nodes or Raft clusters.
- **Connection Pooling:** Maintains efficient connection pools to backend shards and handles the routing of Redis commands completely transparently to the client.

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
| `BADIS_PORT` | The port the server or proxy listens on. | `:6379` |
| `BADIS_DATA_DIR` | Directory to store BadgerDB data and Raft logs. | `badis-data` |
| `BADIS_PROXY_MODE` | Set to `true` to run as a stateless routing proxy. | `false` |
| `BADIS_SHARDS` | Comma-separated list of backend addresses (required for proxy). | `""` |

### Running a Server Node

```bash
# Start a standalone Badis server
BADIS_PORT=":6379" BADIS_DATA_DIR="./node-1" go run main.go
```

### Running a Proxy Node

```bash
# Start a proxy routing to multiple clusters
BADIS_PROXY_MODE="true" BADIS_SHARDS="localhost:6379,localhost:6380,localhost:6381" BADIS_PORT=":6382" go run main.go
```

## Built With
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value DB in Go.
- [Hashicorp Raft](https://github.com/hashicorp/raft) - Distributed consensus.
- [Redcon](https://github.com/tidwall/redcon) - Custom Redis server framework.
- [Consistent](https://github.com/buraksezer/consistent) - Consistent hashing with bounded loads.