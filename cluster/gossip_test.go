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
