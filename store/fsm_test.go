package store

import (
	"bytes"
	"os"
	"testing"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

func encodeCommand(cmd Command) []byte {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	_ = enc.Encode(cmd)
	return buf.Bytes()
}

func TestFSM_Apply(t *testing.T) {
	dbPath, err := os.MkdirTemp("", "badis-test-fsm-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dbPath) }()

	fsm, err := NewFSM(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fsm.Close() }()

	// Propose SET
	cmd := Command{Op: "SET", Key: "foo", Args: [][]byte{[]byte("bar")}}
	data := encodeCommand(cmd)
	log := &raft.Log{Data: data}

	resp := fsm.Apply(log)
	if resp != nil {
		t.Fatalf("Expected nil resp, got %v", resp)
	}

	// Verify SET
	val, err := fsm.GetString("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("bar")) {
		t.Fatalf("Expected bar, got %s", val)
	}

	// Test SET missing args
	cmd = Command{Op: "SET", Key: "foo2"}
	data = encodeCommand(cmd)
	resp = fsm.Apply(&raft.Log{Data: data})
	if resp == nil {
		t.Fatal("Expected error for SET with no args")
	}

	// Test DEL
	cmd = Command{Op: "DEL", Key: "foo"}
	data = encodeCommand(cmd)
	resp = fsm.Apply(&raft.Log{Data: data})
	if resp != nil {
		t.Fatalf("Expected nil resp, got %v", resp)
	}

	// Verify DEL
	val, err = fsm.GetString("foo")
	if err != nil && val != nil {
		t.Fatalf("Expected nil val, got %v", val)
	}

	// Test unknown op
	cmd = Command{Op: "UNKNOWN", Key: "foo"}
	data = encodeCommand(cmd)
	resp = fsm.Apply(&raft.Log{Data: data})
	if resp == nil {
		t.Fatal("Expected error for UNKNOWN op")
	}
}
