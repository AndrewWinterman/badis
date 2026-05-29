# Raft Wiring & Cluster Chaos Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up Hashicorp Raft replication in `main.go` and prove linearizability under faults via a Jepsen-style chaos test using `porcupine`.

**Architecture:** Use 3 network ports per node (Client, Gossip, Raft). Group nodes into independent Raft clusters by parsing a new `BADIS_SHARD_ID` env var. First node in a shard auto-bootstraps Raft; subsequent nodes discover the shard leader via Gossip and send an HTTP API request to join the Raft cluster. Use `go-redis` and `porcupine` to simulate concurrent traffic and verify consistency during a hard crash.

**Tech Stack:** Go, Hashicorp Raft, Hashicorp Memberlist, BadgerDB, Porcupine (Linearizability checking)

---

### Task 1: Environment Variables and Raft Setup Signature

Update `main.go` and `server/server.go` to handle the new `BADIS_RAFT_PORT` and `BADIS_SHARD_ID` environment variables.

**Files:**
- Modify: `main.go`
- Modify: `server/server.go`

- [ ] **Step 1: Read new env vars in main**

Modify `main.go` to read `BADIS_RAFT_PORT` (default "8300") and `BADIS_SHARD_ID` (default "shard-1").

```go
	gossipPort := getEnvOrDefault("BADIS_GOSSIP_PORT", "7946")
	raftPort := getEnvOrDefault("BADIS_RAFT_PORT", "8300")
	shardID := getEnvOrDefault("BADIS_SHARD_ID", "shard-1")
```

- [ ] **Step 2: Update Server struct and SetupRaft signature**

Modify `server/server.go` to inject `shardID` and the `SlotMap` reference into the Server so the HTTP handlers can look up leaders. Add `httpServer` for the Raft join API.

```go
type Server struct {
	addr    string
	mux     *redcon.ServeMux
	fsm     *store.FSM
	router  *router.Router
	raft    *raft.Raft
	shardID string
	clients map[string]*redis.Client
}

func NewServer(addr string, fsm *store.FSM, router *router.Router, shardID string) *Server {
	// ... pass shardID to struct
```

- [ ] **Step 3: Update SetupRaft logic**

Modify `SetupRaft` in `server/server.go` to include the `shardID` logic.

```go
func (s *Server) SetupRaft(localID string, raftBindAddr string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	addr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshotStore := raft.NewDiscardSnapshotStore()

	r, err := raft.NewRaft(config, s.fsm, s.fsm, s.fsm, snapshotStore, transport)
	if err != nil {
		return err
	}
	s.raft = r
	return nil
}
```

- [ ] **Step 4: Update main.go NewServer call**

Update `main.go` to pass `shardID` to `NewServer`.

```go
	srv := server.NewServer(port, fsm, r, shardID)
```

- [ ] **Step 5: Run unit tests**

Run: `go test ./...`
Expected: PASS (or minor compiler fixes if imports missing).

- [ ] **Step 6: Commit**

```bash
git add main.go server/server.go
git commit -m "feat: add raft port and shard id env vars"
```

---

### Task 2: Build the Raft Join API

Since Raft nodes need to be explicitly added to the replica set by the leader (`AddVoter`), we need a lightweight HTTP endpoint (or custom RESP command) to handle this. Let's use a simple built-in HTTP server on `BADIS_RAFT_PORT + 100` just for administration to keep it out of the Redis protocol parser.

**Files:**
- Modify: `server/server.go`
- Modify: `main.go`

- [ ] **Step 1: Add Join handler in server**

```go
func (s *Server) handleRaftJoin(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")
	addr := r.URL.Query().Get("addr")

	if s.raft.State() != raft.Leader {
		http.Error(w, "Not the leader", http.StatusBadRequest)
		return
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// Already joined
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		http.Error(w, f.Error().Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
```

- [ ] **Step 2: Start admin HTTP server**

In `server/server.go` `Start()` method:

```go
// Add to Start()
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/join", s.handleRaftJoin)
	// Parse RAFT_PORT to get admin port
	// (assume BADIS_RAFT_PORT env var is passed or accessible)
	// For simplicity, we can pass it down from main or hardcode an offset.
```

*(Self-correction: Passing `raftPort` directly to `Start()` or storing it in `Server` is better)*.

```go
func (s *Server) StartAdmin(adminPort string) {
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/join", s.handleRaftJoin)
	go http.ListenAndServe(adminPort, adminMux)
}
```

- [ ] **Step 3: Wire in main.go**

```go
	// In main.go
	adminPort := ":" + strconv.Itoa(mustParseInt(raftPort) + 100)
	srv.StartAdmin(adminPort)
```

- [ ] **Step 4: Commit**

```bash
git add server/server.go main.go
git commit -m "feat: add http admin api for raft joins"
```

---

### Task 3: Auto-Bootstrap vs Join Logic in main.go

Wire up the actual Raft initialization when the node boots. Look at the Gossip peers to decide whether to bootstrap a new cluster or ask to join an existing one.

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Implement Bootstrapping logic**

```go
	// In main.go, after setting up server but BEFORE srv.Start()
	
	err = srv.SetupRaft(nodeID, "0.0.0.0:"+raftPort)
	// handle err

	// Wait briefly for gossip to find peers
	time.Sleep(2 * time.Second)
	
	var shardLeaderAdminAddr string
	// (Needs logic to look at gossip peers and find existing shard nodes)
	// For now, if joinList is empty, bootstrap.
	
	if len(joinList) == 0 {
		// Auto-bootstrap
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress("127.0.0.1:" + raftPort),
				},
			},
		}
		srv.GetRaft().BootstrapCluster(configuration)
	} else {
		// Ask to join
		// Assume joinAddrs node exposes admin API on +100 port
		// Fire HTTP GET to /join?id=nodeID&addr=127.0.0.1:raftPort
	}
```

- [ ] **Step 2: Commit**

```bash
git add main.go
git commit -m "feat: wire up raft bootstrap and join"
```

---

### Task 4: Complete the Chaos Linearizability Test

Finish `test/chaos/linearizability_test.go` using `porcupine` and concurrent workers.

**Files:**
- Modify: `test/chaos/linearizability_test.go`

- [ ] **Step 1: Write concurrent worker loop**

```go
	var opsMu sync.Mutex
	
	// Start 5 concurrent clients
	for i := 0; i < 5; i++ {
		go func(c *redis.Client) {
			for j := 0; j < 50; j++ {
				start := time.Now()
				val := fmt.Sprintf("val-%d", rand.Intn(100))
				err := c.Set(ctx, "chaos-key", val, 0).Err()
				
				opsMu.Lock()
				operations = append(operations, porcupine.Operation{
					ClientId: i,
					Input: kvInput{op: 1, value: val},
					Call: start.UnixNano(),
					Return: time.Now().UnixNano(),
					Output: kvOutput{err: err},
				})
				opsMu.Unlock()
				
				time.Sleep(10 * time.Millisecond)
			}
		}(client)
	}
```

- [ ] **Step 2: Inject Fault and Wait**

```go
	time.Sleep(1 * time.Second)
	cluster.Nodes[0].Stop() // Kill leader
	time.Sleep(3 * time.Second) // Wait for election
```

- [ ] **Step 3: Run Porcupine Check**

```go
	isLinearizable := porcupine.Check(kvModel, operations)
	if !isLinearizable {
		t.Fatal("History is not linearizable!")
	}
```

- [ ] **Step 4: Run the Chaos Test**

Run: `RUN_CHAOS=1 go test ./test/chaos/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add test/chaos/linearizability_test.go
git commit -m "test: complete jepsen-style chaos test"
```
