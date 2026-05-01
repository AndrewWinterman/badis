package proxy

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type shard string

func (s shard) String() string {
	return string(s)
}

const (
	defaultPartitionCount    = 271
	defaultReplicationFactor = 20
	defaultLoad              = 1.25
)

type Router struct {
	ring *consistent.Consistent
}

func NewRouter(shardAddrs []string) *Router {
	cfg := consistent.Config{
		PartitionCount:    defaultPartitionCount,
		ReplicationFactor: defaultReplicationFactor,
		Load:              defaultLoad,
		Hasher:            hasher{},
	}

	ring := consistent.New(nil, cfg)
	for _, addr := range shardAddrs {
		ring.Add(shard(addr))
	}

	return &Router{ring: ring}
}

func (r *Router) LocateKey(key []byte) string {
	member := r.ring.LocateKey(key)
	if member == nil {
		return ""
	}
	return member.String()
}
