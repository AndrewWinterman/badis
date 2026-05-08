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
