# Badis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Redis-compatible, Raft-replicated server backed by BadgerDB.

**Architecture:** A network server accepting RESP, proposing writes via hashicorp/raft, and applying committed entries to a BadgerDB-backed FSM. BadgerDB also serves as Raft's LogStore and StableStore.

**Tech Stack:** Go, BadgerDB, hashicorp/raft, tidwall/redcon (for RESP).

---

### Task 1: Setup Project and Network Layer

**Files:**
- Create: `go.mod`
- Create: `main.go`
- Create: `server/server.go`
- Create: `server/server_test.go`

- [ ] **Step 1: Initialize Go module**

```bash
go mod init github.com/winterman/badis
go get github.com/tidwall/redcon
```

- [ ] **Step 2: Write failing test for basic server**

```go
// server/server_test.go
package server

import (
	"testing"
	"time"
	"github.com/go-redis/redis/v8" // Using go-redis for testing client
	"context"
)

func TestBasicServer(t *testing.T) {
	srv := NewServer(":6380")
	go srv.Start()
	time.Sleep(100 * time.Millisecond) // Wait for start
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}
```

- [ ] **Step 3: Run test (fails)**

Run: `go get github.com/go-redis/redis/v8 && go test ./server -v`
Expected: FAIL

- [ ] **Step 4: Implement basic redcon server**

```go
// server/server.go
package server

import (
	"log"
	"github.com/tidwall/redcon"
)

type Server struct {
	addr string
	mux  *redcon.ServeMux
}

func NewServer(addr string) *Server {
	mux := redcon.NewServeMux()
	mux.HandleFunc("ping", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("PONG")
	})
	return &Server{addr: addr, mux: mux}
}

func (s *Server) Start() error {
	log.Printf("Starting server on %s", s.addr)
	return redcon.ListenAndServe(s.addr, s.mux.ServeRESP,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)
}

func (s *Server) Stop() {
	// redcon doesn't have a clean stop in this simple mode, we'll refine later
}
```

- [ ] **Step 5: Run test (passes)**

Run: `go test ./server -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum server/ main.go
git commit -m "feat: basic RESP server with redcon"
```

### Task 2: Implement Storage FSM with BadgerDB (Key/Value)

**Files:**
- Create: `store/fsm.go`
- Create: `store/fsm_test.go`

- [ ] **Step 1: Get BadgerDB dependency**

```bash
go get github.com/dgraph-io/badger/v4
go get github.com/hashicorp/raft
```

- [ ] **Step 2: Write failing tests for KV operations**

```go
// store/fsm_test.go
package store

import (
	"testing"
	"os"
	"bytes"
	"encoding/json"
	"github.com/hashicorp/raft"
)

func TestFSM_Apply(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-test-fsm-*")
	defer os.RemoveAll(dbPath)

	fsm, err := NewFSM(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	// Propose SET
	cmd := Command{Op: "SET", Key: "foo", Args: [][]byte{[]byte("bar")}}
	data, _ := json.Marshal(cmd)
	log := &raft.Log{Data: data}
	
	resp := fsm.Apply(log)
	if resp != nil {
		t.Fatalf("Expected nil resp, got %v", resp)
	}

	// Verify SET
	val, err := fsm.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("bar")) {
		t.Fatalf("Expected bar, got %s", val)
	}
}
```

- [ ] **Step 3: Run test (fails)**

Run: `go test ./store -v`
Expected: FAIL

- [ ] **Step 4: Implement BadgerDB FSM**

```go
// store/fsm.go
package store

import (
	"encoding/json"
	"io"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type Command struct {
	Op   string
	Key  string
	Args [][]byte
}

type FSM struct {
	db *badger.DB
}

func NewFSM(path string) (*FSM, error) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.WARNING)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &FSM{db: db}, nil
}

func (f *FSM) Close() error { return f.db.Close() }

func (f *FSM) Get(key string) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil { return err }
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound { return nil, nil }
	return val, err
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil { return err }

	return f.db.Update(func(txn *badger.Txn) error {
		switch cmd.Op {
		case "SET":
			return txn.Set([]byte(cmd.Key), cmd.Args[0])
		case "DEL":
			return txn.Delete([]byte(cmd.Key))
		}
		return nil
	})
}

// Required Raft FSM methods
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &fsmSnapshot{}, nil }
func (f *FSM) Restore(io.ReadCloser) error { return nil }

type fsmSnapshot struct{}
func (s *fsmSnapshot) Persist(raft.SnapshotSink) error { return nil }
func (s *fsmSnapshot) Release() {}
```

- [ ] **Step 5: Run test (passes)**

Run: `go test ./store -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum store/
git commit -m "feat: badgerdb FSM implementation for raft"
```

### Task 3: Wire Server to FSM (Single Node)

**Files:**
- Modify: `server/server.go`
- Modify: `server/server_test.go`
- Create: `main.go`

- [ ] **Step 1: Write test for SET/GET**

```go
// server/server_test.go
// Add to imports: "github.com/winterman/badis/store", "os"

func TestSetGet(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-test-srv-*")
	defer os.RemoveAll(dbPath)
	fsm, _ := store.NewFSM(dbPath)

	srv := NewServer(":6381", fsm) // Update NewServer signature
	go srv.Start()
	time.Sleep(100 * time.Millisecond)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6381"})
	
	err := rdb.Set(context.Background(), "k1", "v1", 0).Err()
	if err != nil { t.Fatal(err) }

	val, err := rdb.Get(context.Background(), "k1").Result()
	if val != "v1" { t.Fatalf("Expected v1, got %v", val) }
}
```

- [ ] **Step 2: Run test (fails)**

Run: `go test ./server -v`
Expected: FAIL

- [ ] **Step 3: Implement SET/GET handlers**

```go
// server/server.go
// Update NewServer to take *store.FSM
// Note: We bypass Raft here for Task 3 just to wire DB to Server.

package server
import (
	"strings"
	"encoding/json"
	"github.com/tidwall/redcon"
	"github.com/winterman/badis/store"
	"github.com/hashicorp/raft"
)

type Server struct {
	addr string
	mux  *redcon.ServeMux
	fsm  *store.FSM
}

func NewServer(addr string, fsm *store.FSM) *Server {
	mux := redcon.NewServeMux()
	s := &Server{addr: addr, mux: mux, fsm: fsm}

	mux.HandleFunc("set", s.handleSet)
	mux.HandleFunc("get", s.handleGet)
	return s
}

func (s *Server) handleSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	// Bypass raft for now, simulate apply
	c := store.Command{Op: "SET", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}}
	data, _ := json.Marshal(c)
	s.fsm.Apply(&raft.Log{Data: data})
	conn.WriteString("OK")
}

func (s *Server) handleGet(conn redcon.Conn, cmd redcon.Command) {
	val, err := s.fsm.Get(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if val == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(val)
}
// Keep Ping and Start/Stop
```

- [ ] **Step 4: Run test (passes)**

Run: `go test ./server -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server/
git commit -m "feat: wire SET and GET to badgerdb"
```

### Task 4: Raft Integration & BadgerDB LogStore

**Files:**
- Create: `store/raft_store.go`
- Create: `store/raft_store_test.go`
- Modify: `server/server.go`

- [ ] **Step 1: Write tests for Raft LogStore/StableStore**

```go
// store/raft_store_test.go
package store

import (
	"testing"
	"os"
	"github.com/hashicorp/raft"
)

func TestBadgerRaftStore(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-raft-store-*")
	defer os.RemoveAll(dbPath)
	fsm, _ := NewFSM(dbPath)
	
	// Test LogStore
	log := &raft.Log{Index: 1, Term: 1, Data: []byte("test")}
	if err := fsm.StoreLog(log); err != nil { t.Fatal(err) }
	
	var out raft.Log
	if err := fsm.GetLog(1, &out); err != nil { t.Fatal(err) }
	if string(out.Data) != "test" { t.Fatal("log mismatch") }
}
```

- [ ] **Step 2: Implement LogStore/StableStore interfaces on FSM**

```go
// store/raft_store.go
package store

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// Implement raft.LogStore and raft.StableStore on FSM
// Use prefixes to separate FSM data (e.g. 'D') from Raft logs (e.g. 'L') and Stable store (e.g. 'S')

func (f *FSM) StoreLog(log *raft.Log) error {
    // encode log to bytes, write to Badger with 'L' + index prefix
    return nil // placeholder
}

func (f *FSM) GetLog(index uint64, log *raft.Log) error {
    return nil // placeholder
}
// ... implement remaining methods (FirstIndex, LastIndex, DeleteRange, Set, Get, SetUint64, GetUint64)
```

- [ ] **Step 3: Update Server to use Raft node**

*(Modify Server to initialize raft.NewRaft using FSM as FSM, LogStore, and StableStore. Route SET commands to raft.Apply().)*