# Badis Proxy Design

**Goal:** Implement a consistent hashing proxy mode within the Badis binary to route requests to backend Raft shards.

## Architecture

1.  **Binary Integration:** The proxy will run from the same `badis` binary, activated by an environment variable (`BADIS_PROXY_MODE=true`).
2.  **Configuration:** The proxy needs a list of backend shards to route to. We will provide this via an environment variable `BADIS_SHARDS` (comma-separated list of addresses, e.g., `badis-0.badis-headless:6379,badis-1.badis-headless:6379`).
3.  **Hashing Ring:** We will use `github.com/buraksezer/consistent` to manage the hashing ring with virtual nodes (vnodes) for even distribution.
4.  **Routing Logic:**
    *   The proxy listens on a TCP port (e.g., `6379`).
    *   It parses incoming RESP commands using `tidwall/redcon`.
    *   It extracts the key from commands like `GET <key>` or `SET <key> <val>`.
    *   It hashes the key to find the responsible shard address.
    *   It forwards the raw RESP command to the backend shard and proxies the response back to the client.
5.  **Kubernetes Deployment:**
    *   We will add a new `Deployment` for the proxy to `k8s/badis.jsonnet`.
    *   We will expose the proxy via a `Service` (which replaces the direct client service to the shards).

## Components

*   `proxy/proxy.go`: Contains the `Server` struct for the proxy, setting up the `redcon` listener and the consistent hash ring.
*   `proxy/router.go`: Logic to map keys to shards and forward network traffic.
*   `main.go`: Updated to check `BADIS_PROXY_MODE` and start either the storage server or the proxy server.
*   `k8s/badis.jsonnet`: Updated to include the Proxy Deployment and Service.

## Data Flow

1.  Client connects to Proxy `Service`.
2.  Proxy receives `SET foo bar`.
3.  Proxy parses `foo`, looks up shard in `consistent` ring.
4.  Ring returns `badis-1.badis-headless:6379`.
5.  Proxy opens a connection (or uses a pool) to `badis-1`, sends raw `SET foo bar`.
6.  `badis-1` processes via Raft/BadgerDB, returns `+OK`.
7.  Proxy forwards `+OK` to Client.