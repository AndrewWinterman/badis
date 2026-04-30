# Badis Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. IMPORTANT: You must also use the `go` skill when implementing Go code.

**Goal:** Implement a consistent hashing proxy in Go to route Redis requests to backend shards, and deploy it via Kubernetes.

**Architecture:** A new proxy package uses `buraksezer/consistent` to hash keys and `go-redis/redis` (or plain TCP) to forward requests. The main binary decides whether to run the proxy or the storage server based on environment variables. The `k8s/badis.jsonnet` is updated to deploy the proxy and update service routing.

**Tech Stack:** Go, buraksezer/consistent, Kubernetes (kubecfg).

---

### Task 1: Setup Consistent Hashing Router

**Files:**
- Create: `proxy/router.go`
- Create: `proxy/router_test.go`

- [ ] **Step 1: Get dependencies**

```bash
go get github.com/buraksezer/consistent
go get github.com/cespare/xxhash/v2
```

- [ ] **Step 2: Write failing test for router**

```go
// proxy/router_test.go
package proxy

import (
	"testing"
)

func TestRouter_LocateKey(t *testing.T) {
	shards := []string{"node1:6379", "node2:6379", "node3:6379"}
	router := NewRouter(shards)

	// Ensure consistent hashing works
	shard1 := router.LocateKey([]byte("user:123"))
	shard2 := router.LocateKey([]byte("user:123"))

	if shard1 != shard2 {
		t.Fatalf("Expected same shard for same key, got %s and %s", shard1, shard2)
	}

	if shard1 == "" {
		t.Fatal("Expected a shard, got empty string")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./proxy -v`
Expected: FAIL

- [ ] **Step 4: Implement Router**

```go
// proxy/router.go
package proxy

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type Shard string

func (s Shard) String() string {
	return string(s)
}

type Router struct {
	ring *consistent.Consistent
}

func NewRouter(shardAddrs []string) *Router {
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	
	ring := consistent.New(nil, cfg)
	for _, addr := range shardAddrs {
		ring.Add(Shard(addr))
	}

	return &Router{ring: ring}
}

func (r *Router) LocateKey(key []byte) string {
	member := r.ring.LocateKey(key)
	if member == nil {
		return ""
	}
	return member.String()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./proxy -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum proxy/
git commit -m "feat: add consistent hashing router"
```

### Task 2: Implement Proxy Server

**Files:**
- Create: `proxy/server.go`
- Create: `proxy/server_test.go`

- [ ] **Step 1: Write failing test**

```go
// proxy/server_test.go
package proxy

import (
	"testing"
	"time"
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/tidwall/redcon"
)

func TestProxyServer(t *testing.T) {
	// Mock backend shard
	backend := redcon.NewServer(":0", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("BACKEND_OK")
	}, func(conn redcon.Conn) bool { return true }, nil)
	go backend.ListenAndServe()
	time.Sleep(100*time.Millisecond) // Wait for bind
	backendAddr := backend.Addr().String()
	defer backend.Close()

	// Start Proxy
	router := NewRouter([]string{backendAddr})
	proxy := NewServer(":0", router)
	go proxy.Start()
	time.Sleep(100*time.Millisecond)
	defer proxy.Stop()

	// Test via Proxy
	rdb := redis.NewClient(&redis.Options{Addr: proxy.addr})
	val, err := rdb.Get(context.Background(), "somekey").Result()
	if err != nil {
		t.Fatal(err)
	}
	if val != "BACKEND_OK" {
		t.Fatalf("Expected BACKEND_OK, got %s", val)
	}
}
```

- [ ] **Step 2: Run test (fails)**

Run: `go test ./proxy -v`
Expected: FAIL

- [ ] **Step 3: Implement Server**

```go
// proxy/server.go
package proxy

import (
	"log"
	"strings"
	"context"
	"time"

	"github.com/tidwall/redcon"
	"github.com/go-redis/redis/v9"
)

type Server struct {
	addr    string
	router  *Router
	server  *redcon.Server
	clients map[string]*redis.Client
}

func NewServer(addr string, router *Router) *Server {
	s := &Server{
		addr:    addr,
		router:  router,
		clients: make(map[string]*redis.Client),
	}
	s.server = redcon.NewServer(addr, s.handleCmd, func(conn redcon.Conn) bool { return true }, nil)
	return s
}

func (s *Server) getClient(addr string) *redis.Client {
	if client, ok := s.clients[addr]; ok {
		return client
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	s.clients[addr] = client
	return client
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	key := cmd.Args[1]
	shardAddr := s.router.LocateKey(key)
	if shardAddr == "" {
		conn.WriteError("ERR no shards available")
		return
	}

	client := s.getClient(shardAddr)
	
	// Convert args to interface{} slice for go-redis Do
	var args []interface{}
	for _, arg := range cmd.Args {
		args = append(args, string(arg))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.Do(ctx, args...).Result()
	if err != nil {
		if err == redis.Nil {
			conn.WriteNull()
			return
		}
		// If the backend returns a Redis error (like ERR), go-redis wraps it.
		// We want to pass it back raw. For now, simple string handling.
		if strings.HasPrefix(err.Error(), "ERR") {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR proxy error: " + err.Error())
		}
		return
	}

	// Simplistic response encoding based on type
	switch v := res.(type) {
	case string:
		conn.WriteBulkString(v)
	case []byte:
		conn.WriteBulk(v)
	case int64:
		conn.WriteInt64(v)
	default:
		// Fallback for simple responses (like +OK)
		conn.WriteString("OK")
	}
}

func (s *Server) Start() error {
	log.Printf("Starting proxy on %s", s.addr)
	return s.server.ListenAndServe()
}

func (s *Server) Stop() {
	for _, c := range s.clients {
		c.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}
```

- [ ] **Step 4: Run test (passes)**

Run: `go test ./proxy -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add proxy/
git commit -m "feat: implement proxy server"
```

### Task 3: Integrate Proxy into Main Binary

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Update main to check BADIS_PROXY_MODE**

```go
// main.go (Update)
package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/winterman/badis/proxy"
	"github.com/winterman/badis/server"
	"github.com/winterman/badis/store"
)

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

type StartStopper interface {
	Start() error
	Stop()
}

func main() {
	port := getEnvOrDefault("BADIS_PORT", ":6379")
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	isProxy := getEnvOrDefault("BADIS_PROXY_MODE", "false")

	var srv StartStopper

	if isProxy == "true" {
		shardsStr := getEnvOrDefault("BADIS_SHARDS", "")
		if shardsStr == "" {
			log.Fatal("BADIS_SHARDS must be set in proxy mode")
		}
		shards := strings.Split(shardsStr, ",")
		router := proxy.NewRouter(shards)
		srv = proxy.NewServer(port, router)
	} else {
		dbPath := getEnvOrDefault("BADIS_DATA_DIR", "badis-data")
		fsm, err := store.NewFSM(dbPath)
		if err != nil {
			log.Fatalf("Failed to initialize FSM: %v", err)
		}
		defer fsm.Close()
		srv = server.NewServer(port, fsm)
	}

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errChan <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Fatalf("Server failed: %v", err)
	case <-quit:
		log.Println("Shutting down...")
	}

	srv.Stop()
}
```

- [ ] **Step 2: Run tests**

Run: `go build && go test ./...`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add main.go
git commit -m "feat: support running as proxy via env var"
```

### Task 4: Update Kubernetes Deployment Configuration

**Files:**
- Modify: `k8s/badis.jsonnet`

- [ ] **Step 1: Add Proxy Deployment to Jsonnet**

```jsonnet
// k8s/badis.jsonnet
local name = 'badis';
local proxyName = 'badis-proxy';
local namespace = 'default';
local replicas = 3;
local proxyReplicas = 2;
local image = 'winterman/badis:latest';

local selector = { app: name };
local proxySelector = { app: proxyName };

// Helper to generate shard connection strings
local shards = std.join(',', [name + '-' + i + '.' + name + '-headless:6379' for i in std.range(0, replicas - 1)]);

[
  // 1. Headless Service for Shards
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
  // 2. Client Service (Now points to proxy)
  {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: name,
      namespace: namespace,
      labels: proxySelector, // route to proxy
    },
    spec: {
      selector: proxySelector,
      ports: [
        { name: 'redis', port: 6379, targetPort: 6379 },
      ],
    },
  },
  // 3. Shard StatefulSet
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
          securityContext: { fsGroup: 10001 },
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
                { name: 'BADIS_DATA_DIR', value: '/data/badis-data' },
                { name: 'BADIS_PORT', value: ':6379' },
              ],
              volumeMounts: [
                { name: 'data', mountPath: '/data' },
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
            resources: { requests: { storage: '5Gi' } },
          },
        },
      ],
    },
  },
  // 4. Proxy Deployment
  {
    apiVersion: 'apps/v1',
    kind: 'Deployment',
    metadata: {
      name: proxyName,
      namespace: namespace,
      labels: proxySelector,
    },
    spec: {
      replicas: proxyReplicas,
      selector: { matchLabels: proxySelector },
      template: {
        metadata: { labels: proxySelector },
        spec: {
          containers: [
            {
              name: proxyName,
              image: image,
              imagePullPolicy: 'IfNotPresent',
              ports: [
                { containerPort: 6379, name: 'redis' },
              ],
              env: [
                { name: 'BADIS_PROXY_MODE', value: 'true' },
                { name: 'BADIS_PORT', value: ':6379' },
                { name: 'BADIS_SHARDS', value: shards },
              ],
            },
          ],
        },
      },
    },
  },
]
```

- [ ] **Step 2: Validate jsonnet file**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet`
Expected: Valid JSON output.

- [ ] **Step 3: Commit**

```bash
git add k8s/
git commit -m "feat: add proxy deployment to jsonnet"
```