package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func getBenchClient(b *testing.B) *redis.Client {
	addr := os.Getenv("BADIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		PoolSize: 100, // High pool size for concurrent benchmarks
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("Failed to connect to Badis: %v", err)
	}
	return client
}

func BenchmarkSet(b *testing.B) {
	client := getBenchClient(b)
	defer client.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:set:%d", i)
			client.Set(ctx, key, "value", 0)
			i++
		}
	})
}

func BenchmarkGet(b *testing.B) {
	client := getBenchClient(b)
	defer client.Close()
	ctx := context.Background()

	// Pre-populate a key
	client.Set(ctx, "bench:get:key", "value", 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.Get(ctx, "bench:get:key")
		}
	})
}
