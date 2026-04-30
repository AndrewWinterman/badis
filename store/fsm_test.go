package store

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

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
