package proxy

import (
	"testing"
)

func TestRouter_LocateKey(t *testing.T) {
	shards := []string{"node1:6379", "node2:6379", "node3:6379"}
	router := NewRouter(shards)

	tests := []struct {
		name string
		key  string
	}{
		{"user 123", "user:123"},
		{"user 456", "user:456"},
		{"user 789", "user:789"},
		{"order 1", "order:1"},
		{"order 2", "order:2"},
		{"product x", "product:x"},
	}

	distribution := make(map[string]int)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard1 := router.LocateKey([]byte(tt.key))
			shard2 := router.LocateKey([]byte(tt.key))

			if shard1 != shard2 {
				t.Fatalf("Expected same shard for same key %q, got %s and %s", tt.key, shard1, shard2)
			}

			if shard1 == "" {
				t.Fatal("Expected a shard, got empty string")
			}
			distribution[shard1]++
		})
	}

	// Verify that keys are actually distributed across different shards
	if len(distribution) < 2 {
		t.Errorf("Expected keys to be distributed across multiple shards, but got: %v", distribution)
	}
}
