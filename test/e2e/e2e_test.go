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

	t.Run("SET Modifiers", func(t *testing.T) {
		// Clean up keys at the end
		t.Cleanup(func() {
			client.Del(context.Background(), "e2e:key_nx", "e2e:key_xx", "e2e:key_get", "e2e:key_ex")
		})

		// --- NX ---
		ok, err := client.Do(ctx, "SET", "e2e:key_nx", "val_nx", "NX").Result()
		if err != nil || ok != "OK" {
			t.Fatalf("SET NX on new key failed: err=%v, res=%v", err, ok)
		}
		
		// Attempting to overwrite with NX should fail
		_, err = client.Do(ctx, "SET", "e2e:key_nx", "val_nx_2", "NX").Result()
		if err != redis.Nil {
			t.Fatalf("SET NX on existing key should return redis.Nil, err=%v", err)
		}

		// --- XX ---
		_, err = client.Do(ctx, "SET", "e2e:key_xx", "val_xx", "XX").Result()
		if err != redis.Nil {
			t.Fatalf("SET XX on non-existent key should return redis.Nil, err=%v", err)
		}

		client.Set(ctx, "e2e:key_xx", "val_initial", 0)
		ok, err = client.Do(ctx, "SET", "e2e:key_xx", "val_xx", "XX").Result()
		if err != nil || ok != "OK" {
			t.Fatalf("SET XX on existing key failed: err=%v, res=%v", err, ok)
		}
		
		// --- GET ---
		// Set existing value
		client.Set(ctx, "e2e:key_get", "val1", 0)
		// Set and get the old value
		oldVal, err := client.Do(ctx, "SET", "e2e:key_get", "val2", "GET").Result()
		if err != nil || oldVal != "val1" {
			t.Fatalf("SET GET returned incorrect old value: got %v, err: %v", oldVal, err)
		}

		// --- EX ---
		err = client.Do(ctx, "SET", "e2e:key_ex", "val_ex", "EX", 1).Err()
		if err != nil {
			t.Fatalf("SET EX failed: %v", err)
		}
		// Let it expire
		time.Sleep(1500 * time.Millisecond)
		_, err = client.Get(ctx, "e2e:key_ex").Result()
		if err != redis.Nil {
			t.Fatalf("Expected key to expire (redis.Nil), got err: %v", err)
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
