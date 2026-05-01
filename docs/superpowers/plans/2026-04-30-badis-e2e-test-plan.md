# Badis E2E Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a black-box end-to-end test suite using Go's `testing` package that runs against the Badis cluster in Kubernetes.

**Architecture:** A standalone test package (`test/e2e/e2e_test.go`) connects to the cluster via `BADIS_ADDR` using `go-redis`. The `Dockerfile` compiles the test binary (`badis-e2e`) using `go test -c`. The jsonnet configuration gets a new `runTests` TLA to conditionally spawn a test Pod that executes the test binary.

**Tech Stack:** Go, Docker, Jsonnet, Kubernetes (`kubecfg`).

---

### Task 1: Write E2E Test Suite

**Files:**
- Create: `test/e2e/e2e_test.go`

- [ ] **Step 1: Write the test code**

```go
// test/e2e/e2e_test.go
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBadisE2E(t *testing.T) {
	addr := os.Getenv("BADIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wait for cluster to be ready
	var err error
	for i := 0; i < 30; i++ {
		err = client.Ping(ctx).Err()
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("Could not connect to Badis at %s: %v", addr, err)
	}

	t.Run("SET and GET", func(t *testing.T) {
		err := client.Set(ctx, "e2e:key1", "val1", 0).Err()
		if err != nil {
			t.Fatalf("Failed to SET: %v", err)
		}

		val, err := client.Get(ctx, "e2e:key1").Result()
		if err != nil {
			t.Fatalf("Failed to GET: %v", err)
		}
		if val != "val1" {
			t.Errorf("Expected val1, got %s", val)
		}
	})

	t.Run("DEL", func(t *testing.T) {
		err := client.Set(ctx, "e2e:key2", "val2", 0).Err()
		if err != nil {
			t.Fatalf("Failed to SET: %v", err)
		}

		deleted, err := client.Del(ctx, "e2e:key2").Result()
		if err != nil {
			t.Fatalf("Failed to DEL: %v", err)
		}
		if deleted != 1 {
			t.Errorf("Expected 1 key deleted, got %d", deleted)
		}

		_, err = client.Get(ctx, "e2e:key2").Result()
		if err != redis.Nil {
			t.Fatalf("Expected redis.Nil, got %v", err)
		}
	})
}
```

- [ ] **Step 2: Verify it builds**

Run: `go test -c -o badis-e2e ./test/e2e`
Expected: Creates `badis-e2e` binary.

- [ ] **Step 3: Commit**

```bash
git add test/e2e/e2e_test.go
git commit -m "test: add e2e test suite"
```

### Task 2: Update Dockerfile to include E2E Test Binary

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Modify Dockerfile**

Update the Dockerfile to compile the test binary and copy it into the final image.

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o badis .
# Compile the test binary
RUN CGO_ENABLED=0 GOOS=linux go test -c -o badis-e2e ./test/e2e

FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN adduser -D -g '' appuser && mkdir -p /data && chown -R appuser:appuser /data
USER appuser
WORKDIR /root/
COPY --from=builder /app/badis .
COPY --from=builder /app/badis-e2e .

ENV BADIS_DATA_DIR=/data
ENV BADIS_PORT=:6379
EXPOSE 6379 6380
VOLUME ["/data"]

CMD ["./badis"]
```

- [ ] **Step 2: Build the Image to verify**

Run: `docker build -t winterman/badis:latest .`
Expected: Successful build.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "build: compile and include e2e test binary in docker image"
```

### Task 3: Add E2E Test Pod to Jsonnet

**Files:**
- Modify: `k8s/badis.jsonnet`

- [ ] **Step 1: Update Jsonnet File**

Update `k8s/badis.jsonnet` to add the `runTests` TLA and conditionally generate the Test Pod.

```jsonnet
// k8s/badis.jsonnet
function(
  namespace='default',
  replicas=3,
  proxyReplicas=2,
  volumeSize='5Gi',
  storageClass=null,
  runTests=false // New Parameter
)
  local name = 'badis';
  local proxyName = 'badis-proxy';
  local testName = 'badis-e2e-test';
  local image = 'winterman/badis:latest';

  local selector = { app: name };
  local proxySelector = { app: proxyName };

  // Helper to generate shard connection strings
  local shards = std.join(',', [name + '-' + i + '.' + name + '-headless:6379' for i in std.range(0, replicas - 1)]);

  local baseResources = [
    // ... (Keep existing 4 resources: Headless Service, Client Service, StatefulSet, Proxy Deployment)
    // NOTE: Keep the exact existing content here for the first 4 array elements
  ];

  local testPod = [
    {
      apiVersion: 'v1',
      kind: 'Pod',
      metadata: {
        name: testName,
        namespace: namespace,
      },
      spec: {
        restartPolicy: 'Never',
        containers: [
          {
            name: 'e2e',
            image: image,
            imagePullPolicy: 'IfNotPresent',
            command: ['./badis-e2e', '-test.v'],
            env: [
              { name: 'BADIS_ADDR', value: name + ':6379' },
            ],
          },
        ],
      },
    }
  ];

  baseResources + (if runTests then testPod else [])
```

*(Note to subagent: Ensure you properly incorporate the existing 4 resources into `baseResources`. Do not delete them).*

- [ ] **Step 2: Validate Jsonnet without Tests**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet`
Expected: Valid JSON output without a Pod named `badis-e2e-test`.

- [ ] **Step 3: Validate Jsonnet with Tests**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet --tla-code runTests=true`
Expected: Valid JSON output ending with the `badis-e2e-test` Pod.

- [ ] **Step 4: Commit**

```bash
git add k8s/badis.jsonnet
git commit -m "feat: add e2e test pod generation to jsonnet configuration"
```