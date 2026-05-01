# Badis E2E Black-Box Test Design

**Goal:** Create a Kubernetes Pod that acts as an external client, testing Badis functionality through the standard `redis` protocol via the Proxy Service.

## Architecture

1.  **Test Suite (Go):** A standalone Go application (`test/e2e/main.go`) that connects to a given Redis address (e.g., `badis:6379`) using `github.com/redis/go-redis/v9`. It will execute a suite of commands (`SET`, `GET`, `DEL`, Lists, Hashes, Sets) and panic or `log.Fatal` if any assertion fails.
2.  **Docker Image:** We will modify the existing `Dockerfile` to build two binaries (`badis` and `badis-e2e`) using a multi-stage build, so we can use `winterman/badis:latest` but override the `CMD` for the test pod to `["./badis-e2e"]`. (Alternative: a separate `Dockerfile.e2e`).
3.  **Kubernetes Resource:** We will add a new `runTests=false` TLA argument to `k8s/badis.jsonnet`. When `true`, it generates a `Pod` (or `Job`) named `badis-e2e-test` that runs the `badis-e2e` binary.

## Data Flow

1.  User runs `kubecfg update k8s/badis.jsonnet --tla-code runTests=true`.
2.  The `badis-e2e-test` Pod starts.
3.  Pod waits briefly or connects with retries to the `badis` service.
4.  Pod sends Redis commands to the `badis` Service IP.
5.  Traffic routes through Proxy -> Backend Shards.
6.  Pod verifies responses and exits 0 on success, or 1 on failure.

## Trade-offs
*   **Pod vs Job:** A `Pod` will just show `Completed` or `Error`. A `Job` gives slightly better retry/backoff semantics natively in K8s, but since you suggested a Pod, a Pod is simpler to define.
*   **Same Image vs New Image:** Building the e2e binary into the same `winterman/badis:latest` image adds a few MBs but keeps deployment simple (one image to push).

Does this look right? Once approved, I'll write the implementation plan.