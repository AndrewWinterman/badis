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
