// test/e2e/e2e_test.go
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBadisE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping E2E tests...")
	}

	addr := os.Getenv("BADIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	// Wait for cluster to be ready
	var err error
	for i := 0; i < 30; i++ {
		err = client.Ping(ctx).Err()
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("Could not connect to Badis at %s: %v", addr, err)
	}

	t.Run("SET and GET", func(t *testing.T) {
		t.Cleanup(func() {
			client.Del(context.Background(), "e2e:key1")
		})

		err := client.Set(ctx, "e2e:key1", "val1", 0).Err()
		if err != nil {
			t.Fatalf("Failed to SET: %v", err)
		}

		val, err := client.Get(ctx, "e2e:key1").Result()
		if err != nil {
			t.Fatalf("Failed to GET: %v", err)
		}
		if val != "val1" {
			t.Errorf("Expected val1, got %s", val)
		}
	})

	t.Run("DEL", func(t *testing.T) {
		err := client.Set(ctx, "e2e:key2", "val2", 0).Err()
		if err != nil {
			t.Fatalf("Failed to SET: %v", err)
		}

		deleted, err := client.Del(ctx, "e2e:key2").Result()
		if err != nil {
			t.Fatalf("Failed to DEL: %v", err)
		}
		if deleted != 1 {
			t.Errorf("Expected 1 key deleted, got %d", deleted)
		}

		_, err = client.Get(ctx, "e2e:key2").Result()
		if err != redis.Nil {
			t.Fatalf("Expected redis.Nil, got %v", err)
		}
	})
}
