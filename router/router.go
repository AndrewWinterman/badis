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

// crc16 calculates the CRC16 checksum for Redis Cluster compatibility
func crc16(data []byte) uint16 {
	crc := uint16(0)
	for _, b := range data {
		crc = crc ^ (uint16(b) << 8)
		for i := 0; i < 8; i++ {
			if (crc & 0x8000) != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc = crc << 1
			}
		}
	}
	return crc
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
	// Hashes key, returns partition ID modulo 16384 using CRC16
	return crc16(key) % 16384
}

func (r *Router) LocateKey(key []byte) (string, string, bool) {
	slot := r.KeyToSlot(key)
	shardID, ip := r.slotMap.GetOwner(slot)
	isLocal := shardID == r.localName
	return shardID, ip, isLocal
}
