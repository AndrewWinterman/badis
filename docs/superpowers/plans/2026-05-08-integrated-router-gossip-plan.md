# Integrated Router and Gossip Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. IMPORTANT: You must also use the `go` skill when implementing Go code.

**Goal:** Fold proxy routing into data nodes using `memberlist` for gossip-based topology sync and auto-rebalancing.

**Architecture:** Single Badis binary acts as both router and data shard. `memberlist` gossips topology versions. Local caches handle line-rate routing.

**Tech Stack:** Go, `hashicorp/memberlist`, `buraksezer/consistent`, `tidwall/redcon`.

---

### Task 1: Add memberlist wrapper (`cluster` package)

**Files:**
- Create: `cluster/gossip.go`
- Create: `cluster/gossip_test.go`
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Install memberlist dependency**

```bash
go get github.com/hashicorp/memberlist
```

- [ ] **Step 2: Write failing test for memberlist initialization**

```go
// cluster/gossip_test.go
package cluster

import (
	"testing"
)

func TestGossip_StartAndJoin(t *testing.T) {
	node1, err := NewGossip("127.0.0.1:0", "node1", nil)
	if err != nil {
		t.Fatalf("failed to start node1: %v", err)
	}
	defer node1.Shutdown()

	node2, err := NewGossip("127.0.0.1:0", "node2", []string{node1.BindAddr()})
	if err != nil {
		t.Fatalf("failed to start node2: %v", err)
	}
	defer node2.Shutdown()

	if len(node1.Members()) != 2 {
		t.Errorf("expected 2 members, got %d", len(node1.Members()))
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./cluster -v`
Expected: FAIL with "undefined: NewGossip"

- [ ] **Step 4: Write minimal implementation**

```go
// cluster/gossip.go
package cluster

import (
	"fmt"
	"net"
	"strconv"

	"github.com/hashicorp/memberlist"
)

type Gossip struct {
	list *memberlist.Memberlist
}

func NewGossip(bindAddr, nodeName string, joinAddrs []string) (*Gossip, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = nodeName

	host, portStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	port, _ := strconv.Atoi(portStr)
	config.BindAddr = host
	config.BindPort = port

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	if len(joinAddrs) > 0 {
		_, err = list.Join(joinAddrs)
		if err != nil {
			return nil, err
		}
	}

	return &Gossip{list: list}, nil
}

func (g *Gossip) BindAddr() string {
	return fmt.Sprintf("%s:%d", g.list.LocalNode().Addr, g.list.LocalNode().Port)
}

func (g *Gossip) Members() []*memberlist.Node {
	return g.list.Members()
}

func (g *Gossip) Shutdown() error {
	return g.list.Shutdown()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./cluster -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum cluster/
git commit -m "feat: add hashicorp memberlist wrapper for gossip"
```

### Task 2: Implement Slot Map Configuration FSM (`config` package)

**Files:**
- Create: `config/slotmap.go`
- Create: `config/slotmap_test.go`

- [ ] **Step 1: Write failing test for SlotMap cache**

```go
// config/slotmap_test.go
package config

import "testing"

func TestSlotMap_Owner(t *testing.T) {
	sm := NewSlotMap()
	sm.SetOwner(100, "shard-1", "10.0.0.1:6379")

	shard, ip := sm.GetOwner(100)
	if shard != "shard-1" || ip != "10.0.0.1:6379" {
		t.Errorf("expected shard-1 at 10.0.0.1:6379, got %s at %s", shard, ip)
	}

	version := sm.Version()
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./config -v`
Expected: FAIL with "undefined: NewSlotMap"

- [ ] **Step 3: Write minimal implementation**

```go
// config/slotmap.go
package config

import "sync"

type SlotMap struct {
	mu      sync.RWMutex
	version uint64
	slots   map[uint16]SlotInfo
}

type SlotInfo struct {
	ShardID string
	IP      string
}

func NewSlotMap() *SlotMap {
	return &SlotMap{
		slots: make(map[uint16]SlotInfo),
	}
}

func (s *SlotMap) SetOwner(slot uint16, shardID, ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slots[slot] = SlotInfo{ShardID: shardID, IP: ip}
	s.version++
}

func (s *SlotMap) GetOwner(slot uint16) (string, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info := s.slots[slot]
	return info.ShardID, info.IP
}

func (s *SlotMap) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./config -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add config/
git commit -m "feat: add slot map configuration cache"
```

### Task 3: Migrate Proxy Router to Integrated Router

**Files:**
- Rename/Modify: `proxy/router.go` -> `router/router.go`
- Modify: `router/router_test.go`
- Delete: `proxy/proxy.go`

- [ ] **Step 1: Write failing test for integrated router**

```go
// router/router_test.go
package router

import (
	"testing"
	"github.com/buraksezer/consistent"
	"badis/config"
)

func TestRouter_LocateKeySlot(t *testing.T) {
    sm := config.NewSlotMap()
    // Assume 16384 slots. Map slot 100 to local, 200 to remote.
    sm.SetOwner(100, "local-shard", "127.0.0.1:6379")
    
    cfg := consistent.Config{
		PartitionCount:    16384,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(nil, cfg)
	
	r := NewRouter(ring, sm, "local-shard")
	
    // We need a helper to hash a key and find its slot.
    slot := r.KeyToSlot([]byte("user:123"))
    if slot < 0 || slot >= 16384 {
        t.Errorf("invalid slot: %d", slot)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./router -v`
Expected: FAIL due to missing package `router` / `NewRouter`

- [ ] **Step 3: Write minimal implementation**

```go
// router/router.go
package router

import (
	"badis/config"
	"github.com/buraksezer/consistent"
	"hash/fnv"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}

type Router struct {
	ring      *consistent.Consistent
	slotMap   *config.SlotMap
	localName string
}

func NewRouter(ring *consistent.Consistent, slotMap *config.SlotMap, localName string) *Router {
	return &Router{
		ring:      ring,
		slotMap:   slotMap,
		localName: localName,
	}
}

func (r *Router) KeyToSlot(key []byte) uint16 {
    // Hashes key, returns partition ID modulo 16384
	hash := fnv.New64a()
	hash.Write(key)
	return uint16(hash.Sum64() % 16384)
}

func (r *Router) LocateKey(key []byte) (string, string, bool) {
    slot := r.KeyToSlot(key)
    shardID, ip := r.slotMap.GetOwner(slot)
    isLocal := shardID == r.localName
    return shardID, ip, isLocal
}
```

- [ ] **Step 4: Cleanup old proxy files and run test**

```bash
rm -rf proxy/
go test ./router -v
```

- [ ] **Step 5: Commit**

```bash
git add router/ proxy/
git commit -m "refactor: replace standalone proxy with integrated router cache"
```

### Task 4: Integrate Gossip and Router into Main

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Write integration update to main**

Update `main.go` to remove `BADIS_PROXY_MODE`, initialize `cluster.NewGossip`, initialize `config.NewSlotMap()`, and start `router.NewRouter`.

```go
// main.go (partial snippet)
// Remove BADIS_PROXY_MODE checking.
// Read BADIS_GOSSIP_PORT and BADIS_JOIN.

gossipPort := os.Getenv("BADIS_GOSSIP_PORT")
if gossipPort == "" {
    gossipPort = "7946"
}
joinAddrs := os.Getenv("BADIS_JOIN")

gossipNode, err := cluster.NewGossip("0.0.0.1:"+gossipPort, nodeID, strings.Split(joinAddrs, ","))
if err != nil {
    slog.Error("failed to start gossip", "error", err)
    os.Exit(1)
}
defer gossipNode.Shutdown()

slotMap := config.NewSlotMap()
// ... router init ...
```

*(Note: Since this task alters main.go orchestration heavily, the implementation will be specific to the current state of `main.go`. Focus on ripping out `proxy.NewServer` logic and replacing the Redis listener to use the integrated router to either serve locally via Raft or dial out to remote.)*

- [ ] **Step 2: Build main**

```bash
go build -o badis main.go
```

- [ ] **Step 3: Commit**

```bash
git add main.go
git commit -m "feat: fold router and gossip into main badis binary"
```
