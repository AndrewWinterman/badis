# Badis Kubernetes Deployment Design

**Goal:** Deploy Badis to Kubernetes using `kubecfg` and jsonnet.

## Architecture

1.  **Docker Image:** A multi-stage `Dockerfile` to build the Go binary and run it in an Alpine/Scratch container.
2.  **StatefulSet:** Raft nodes require stable network identities and persistent storage. We will use a `StatefulSet` with 3 replicas.
3.  **Services:**
    *   **Headless Service:** `badis-headless` for Raft peer discovery (required by StatefulSet).
    *   **Client Service:** `badis` (ClusterIP) for Redis clients to connect to the leader.
4.  **Storage:** `volumeClaimTemplates` in the StatefulSet to provide persistent BadgerDB storage (`/data/badis-data`) for each pod.
5.  **Jsonnet:** Define the Kubernetes resources using standard jsonnet arrays/objects, structured for `kubecfg update`.

## Components

*   `Dockerfile`: Multi-stage Go build.
*   `k8s/badis.jsonnet`: The main entrypoint for `kubecfg`.
*   `k8s/lib/statefulset.libsonnet`: Helper for the StatefulSet definition.
*   `k8s/lib/service.libsonnet`: Helper for Services.

## Data Flow & Network

*   Pods get stable names: `badis-0`, `badis-1`, `badis-2`.
*   Clients connect to `badis:6379`.
*   (Note: The Go code currently hardcodes `badis-data` as DB path and doesn't fully expose Raft peer clustering via CLI flags. We will need to modify `main.go` to accept ENV vars or flags for port, DB path, and Raft peers, but the *focus of this plan* is the jsonnet/Docker infrastructure to support it).

## Approach

*   **Dockerfile:** Standard Alpine build.
*   **Jsonnet Structure:** Keep it simple and raw. `kubecfg` expects an array or object of Kubernetes resources. We will generate an array containing:
    1. `Service` (Client)
    2. `Service` (Headless)
    3. `StatefulSet`

## Missing Link (Go Code)
To actually run in K8s, `main.go` must read environment variables to set the Raft advertise address (using the Pod IP or StatefulSet DNS name) and join the cluster. I will add a small step to the implementation plan to make `main.go` read `BADIS_PORT`, `BADIS_DATA_DIR`, and `BADIS_RAFT_PEERS` to make the deployment work.

Does this look right?