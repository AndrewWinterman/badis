package store

import (
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func TestBadgerRaftStore(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-raft-store-*")
	defer os.RemoveAll(dbPath)
	fsm, err := NewFSM(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	// Test LogStore
	log := &raft.Log{Index: 1, Term: 1, Data: []byte("test")}
	if err := fsm.StoreLog(log); err != nil {
		t.Fatal(err)
	}

	var out raft.Log
	if err := fsm.GetLog(1, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "test" {
		t.Fatal("log mismatch")
	}

	if err := fsm.StoreLogs([]*raft.Log{
		{Index: 2, Term: 1, Data: []byte("test2")},
		{Index: 3, Term: 1, Data: []byte("test3")},
	}); err != nil {
		t.Fatal(err)
	}

	first, err := fsm.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("expected first index 1, got %d", first)
	}

	last, err := fsm.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if last != 3 {
		t.Fatalf("expected last index 3, got %d", last)
	}

	if err := fsm.DeleteRange(1, 2); err != nil {
		t.Fatal(err)
	}

	first, err = fsm.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first != 3 {
		t.Fatalf("expected first index 3, got %d", first)
	}

	// Test StableStore
	if err := fsm.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	val, err := fsm.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Fatal("value mismatch")
	}

	if err := fsm.SetUint64([]byte("key2"), 42); err != nil {
		t.Fatal(err)
	}
	valU, err := fsm.GetUint64([]byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if valU != 42 {
		t.Fatal("uint64 mismatch")
	}
}
