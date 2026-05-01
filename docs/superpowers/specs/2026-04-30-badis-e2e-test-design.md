# Badis E2E Black-Box Test Design

**Goal:** Create a Kubernetes Pod that acts as an external client, testing Badis functionality through the standard `redis` protocol via the Proxy Service.

## Architecture

1.  **Test Suite (Go):** Standard Go tests (`test/e2e/e2e_test.go`) that connect to a given Redis address (e.g., `badis:6379`) using `github.com/redis/go-redis/v9`. We use the `testing` package for standard assertions, subtests (`t.Run`), and reporting.
2.  **Docker Image:** We will modify the existing `Dockerfile` to compile the test binary in the builder stage (`go test -c -o badis-e2e ./test/e2e`) and copy it to the final Alpine image. The test Pod will override the `CMD` to run `["./badis-e2e", "-test.v"]`.
3.  **Kubernetes Resource:** We will add a new `runTests=false` TLA argument to `k8s/badis.jsonnet`. When `true`, it generates a `Pod` named `badis-e2e-test` that runs the `badis-e2e` binary.

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