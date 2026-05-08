// router/router_test.go
package router

import (
	"github.com/winterman/badis/config"
	"testing"

	"github.com/buraksezer/consistent"
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
