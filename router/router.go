// router/router.go
package router

import (
	"hash/fnv"

	"github.com/buraksezer/consistent"
	"github.com/winterman/badis/config"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}

type Router struct {
	ring      *consistent.Consistent
	slotMap   *config.SlotMap
	localName string
}

func NewRouter(ring *consistent.Consistent, slotMap *config.SlotMap, localName string) *Router {
	return &Router{
		ring:      ring,
		slotMap:   slotMap,
		localName: localName,
	}
}

func (r *Router) KeyToSlot(key []byte) uint16 {
	// Hashes key, returns partition ID modulo 16384
	hash := fnv.New64a()
	hash.Write(key)
	return uint16(hash.Sum64() % 16384)
}

func (r *Router) LocateKey(key []byte) (string, string, bool) {
	slot := r.KeyToSlot(key)
	shardID, ip := r.slotMap.GetOwner(slot)
	isLocal := shardID == r.localName
	return shardID, ip, isLocal
}
