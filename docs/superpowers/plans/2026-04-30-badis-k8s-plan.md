# Badis Kubernetes Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. IMPORTANT: You must also use the `go` skill when implementing Go code.

**Goal:** Deploy Badis to Kubernetes using `kubecfg` (jsonnet), adding necessary environment variables and a Docker image.

**Architecture:** A multi-stage Docker build for the Go binary, deployed via a Kubernetes StatefulSet (3 replicas) using `kubecfg`. Includes headless and clusterIP services. `main.go` will be updated to read configuration from environment variables instead of hardcoding values.

**Tech Stack:** Go, Docker, Jsonnet, Kubernetes (`kubecfg`).

---

### Task 1: Update Go Configuration for Kubernetes

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Read Configuration from Environment Variables**

```go
// main.go
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/winterman/badis/server"
	"github.com/winterman/badis/store"
)

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
	port := getEnvOrDefault("BADIS_PORT", ":6379")

	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize FSM: %v", err)
	}

	srv := server.NewServer(port, fsm)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	srv.Stop()
}
```

- [ ] **Step 2: Run tests to verify it still builds and passes**

Run: `go build && go test ./...`
Expected: Passes.

- [ ] **Step 3: Commit**

```bash
git add main.go
git commit -m "feat: use env vars for configuration"
```

### Task 2: Create Docker Build

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Write .dockerignore**

```text
# .dockerignore
.git
.worktrees
badis-data
```

- [ ] **Step 2: Write Dockerfile**

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o badis .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/badis .

ENV BADIS_DATA_DIR=/data
ENV BADIS_PORT=:6379
EXPOSE 6379 6380

CMD ["./badis"]
```

- [ ] **Step 3: Build the Docker Image**

Run: `docker build -t winterman/badis:latest .`
Expected: Successful build.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add Dockerfile"
```

### Task 3: Create Kubernetes Jsonnet Configuration

**Files:**
- Create: `k8s/badis.jsonnet`

- [ ] **Step 1: Write k8s/badis.jsonnet**

```jsonnet
// k8s/badis.jsonnet
local name = 'badis';
local namespace = 'default';
local replicas = 3;
local image = 'winterman/badis:latest';

local selector = {
  app: name,
};

[
  // 1. Headless Service for Raft Peer Discovery
  {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: name + '-headless',
      namespace: namespace,
      labels: selector,
    },
    spec: {
      clusterIP: 'None',
      selector: selector,
      ports: [
        { name: 'redis', port: 6379, targetPort: 6379 },
        { name: 'raft', port: 6380, targetPort: 6380 },
      ],
    },
  },
  // 2. Client Service
  {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: name,
      namespace: namespace,
      labels: selector,
    },
    spec: {
      selector: selector,
      ports: [
        { name: 'redis', port: 6379, targetPort: 6379 },
      ],
    },
  },
  // 3. StatefulSet
  {
    apiVersion: 'apps/v1',
    kind: 'StatefulSet',
    metadata: {
      name: name,
      namespace: namespace,
      labels: selector,
    },
    spec: {
      serviceName: name + '-headless',
      replicas: replicas,
      selector: { matchLabels: selector },
      template: {
        metadata: { labels: selector },
        spec: {
          containers: [
            {
              name: name,
              image: image,
              imagePullPolicy: 'IfNotPresent',
              ports: [
                { containerPort: 6379, name: 'redis' },
                { containerPort: 6380, name: 'raft' },
              ],
              env: [
                {
                  name: 'BADIS_DATA_DIR',
                  value: '/data/badis-data',
                },
                {
                  name: 'BADIS_PORT',
                  value: ':6379',
                },
                {
                  name: 'POD_NAME',
                  valueFrom: {
                    fieldRef: { fieldPath: 'metadata.name' },
                  },
                },
              ],
              volumeMounts: [
                {
                  name: 'data',
                  mountPath: '/data',
                },
              ],
            },
          ],
        },
      },
      volumeClaimTemplates: [
        {
          metadata: { name: 'data' },
          spec: {
            accessModes: ['ReadWriteOnce'],
            resources: {
              requests: { storage: '5Gi' },
            },
          },
        },
      ],
    },
  },
]
```

- [ ] **Step 2: Validate jsonnet file**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet`
Expected: Valid JSON output containing the Services and StatefulSet.

- [ ] **Step 3: Commit**

```bash
git add k8s/
git commit -m "feat: add kubecfg jsonnet deployment"
```