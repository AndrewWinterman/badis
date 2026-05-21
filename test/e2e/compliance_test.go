package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// setupComplianceClient initializes the Redis client for compliance tests
func setupComplianceClient(t *testing.T) (*redis.Client, context.Context) {
	t.Helper()
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping compliance tests (RUN_E2E not set)")
	}

	addr := os.Getenv("BADIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	
	t.Cleanup(func() {
		client.Close()
	})

	ctx := context.Background()

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

	return client, ctx
}

func TestCompliance_Strings(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("MSET and MGET", func(t *testing.T) {
		client.Del(ctx, "comp:str:1", "comp:str:2")
		t.Cleanup(func() { client.Del(ctx, "comp:str:1", "comp:str:2") })

		err := client.MSet(ctx, "comp:str:1", "val1", "comp:str:2", "val2").Err()
		if err != nil {
			t.Fatalf("MSET failed: %v", err)
		}

		vals, err := client.MGet(ctx, "comp:str:1", "comp:str:2").Result()
		if err != nil {
			t.Fatalf("MGET failed: %v", err)
		}
		if len(vals) != 2 || vals[0] != "val1" || vals[1] != "val2" {
			t.Fatalf("MGET returned unexpected values: %v", vals)
		}
	})

	t.Run("INCR and DECR", func(t *testing.T) {
		client.Del(ctx, "comp:str:counter")
		t.Cleanup(func() { client.Del(ctx, "comp:str:counter") })

		val, err := client.Incr(ctx, "comp:str:counter").Result()
		if err != nil || val != 1 {
			t.Fatalf("INCR failed: got %d, err: %v", val, err)
		}

		val, err = client.Decr(ctx, "comp:str:counter").Result()
		if err != nil || val != 0 {
			t.Fatalf("DECR failed: got %d, err: %v", val, err)
		}
	})
}

func TestCompliance_Keys(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("EXISTS", func(t *testing.T) {
		client.Del(ctx, "comp:key:exists")
		t.Cleanup(func() { client.Del(ctx, "comp:key:exists") })

		exists, err := client.Exists(ctx, "comp:key:exists").Result()
		if err != nil || exists != 0 {
			t.Fatalf("EXISTS should be 0, got %d, err: %v", exists, err)
		}

		client.Set(ctx, "comp:key:exists", "val", 0)
		exists, err = client.Exists(ctx, "comp:key:exists").Result()
		if err != nil || exists != 1 {
			t.Fatalf("EXISTS should be 1, got %d, err: %v", exists, err)
		}
	})

	t.Run("EXPIRE and TTL", func(t *testing.T) {
		client.Del(ctx, "comp:key:expire")
		t.Cleanup(func() { client.Del(ctx, "comp:key:expire") })

		client.Set(ctx, "comp:key:expire", "val", 0)
		ok, err := client.Expire(ctx, "comp:key:expire", 10*time.Second).Result()
		if err != nil || !ok {
			t.Fatalf("EXPIRE failed: %v", err)
		}

		ttl, err := client.TTL(ctx, "comp:key:expire").Result()
		if err != nil || ttl <= 0 {
			t.Fatalf("TTL failed or invalid: ttl=%v, err=%v", ttl, err)
		}
	})
}

func TestCompliance_Hashes(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("HSET, HGET, HGETALL", func(t *testing.T) {
		client.Del(ctx, "comp:hash:1")
		t.Cleanup(func() { client.Del(ctx, "comp:hash:1") })

		err := client.HSet(ctx, "comp:hash:1", "field1", "val1", "field2", "val2").Err()
		if err != nil {
			t.Fatalf("HSET failed: %v", err)
		}

		val, err := client.HGet(ctx, "comp:hash:1", "field1").Result()
		if err != nil || val != "val1" {
			t.Fatalf("HGET failed: got %s, err: %v", val, err)
		}

		all, err := client.HGetAll(ctx, "comp:hash:1").Result()
		if err != nil {
			t.Fatalf("HGETALL failed: %v", err)
		}
		if all["field1"] != "val1" || all["field2"] != "val2" {
			t.Fatalf("HGETALL returned unexpected map: %v", all)
		}
	})
}

func TestCompliance_Lists(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("LPUSH, RPUSH, LPOP, RPOP, LRANGE", func(t *testing.T) {
		client.Del(ctx, "comp:list:1")
		t.Cleanup(func() { client.Del(ctx, "comp:list:1") })

		err := client.LPush(ctx, "comp:list:1", "val1", "val2").Err() // val2, val1
		if err != nil {
			t.Fatalf("LPUSH failed: %v", err)
		}

		err = client.RPush(ctx, "comp:list:1", "val3").Err() // val2, val1, val3
		if err != nil {
			t.Fatalf("RPUSH failed: %v", err)
		}

		vals, err := client.LRange(ctx, "comp:list:1", 0, -1).Result()
		if err != nil || len(vals) != 3 {
			t.Fatalf("LRANGE failed: got %v, err: %v", vals, err)
		}
		if vals[0] != "val2" || vals[1] != "val1" || vals[2] != "val3" {
			t.Fatalf("LRANGE returned unexpected order: %v", vals)
		}

		val, err := client.LPop(ctx, "comp:list:1").Result()
		if err != nil || val != "val2" {
			t.Fatalf("LPOP failed: got %v, err: %v", val, err)
		}
	})
}

func TestCompliance_Sets(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("SADD, SMEMBERS, SISMEMBER", func(t *testing.T) {
		client.Del(ctx, "comp:set:1")
		t.Cleanup(func() { client.Del(ctx, "comp:set:1") })

		err := client.SAdd(ctx, "comp:set:1", "member1", "member2").Err()
		if err != nil {
			t.Fatalf("SADD failed: %v", err)
		}

		isMember, err := client.SIsMember(ctx, "comp:set:1", "member1").Result()
		if err != nil || !isMember {
			t.Fatalf("SISMEMBER failed: got %v, err: %v", isMember, err)
		}

		members, err := client.SMembers(ctx, "comp:set:1").Result()
		if err != nil || len(members) != 2 {
			t.Fatalf("SMEMBERS failed: got %v, err: %v", members, err)
		}
	})
}

func TestCompliance_Lua(t *testing.T) {
	client, ctx := setupComplianceClient(t)

	t.Run("EVAL simple return", func(t *testing.T) {
		val, err := client.Eval(ctx, `return "hello lua"`, []string{}).Result()
		if err != nil || val != "hello lua" {
			t.Fatalf("EVAL failed: got %v, err: %v", val, err)
		}
	})

	t.Run("EVAL redis.call", func(t *testing.T) {
		client.Del(ctx, "comp:lua:1")
		t.Cleanup(func() { client.Del(ctx, "comp:lua:1") })

		// Set a key and read it
		val, err := client.Eval(ctx, `
			redis.call("SET", KEYS[1], ARGV[1])
			return redis.call("GET", KEYS[1])
		`, []string{"comp:lua:1"}, "lua_value").Result()
		if err != nil || val != "lua_value" {
			t.Fatalf("EVAL redis.call failed: got %v, err: %v", val, err)
		}
	})

	t.Run("SCRIPT LOAD and EVALSHA", func(t *testing.T) {
		script := `return ARGV[1]`
		sha, err := client.ScriptLoad(ctx, script).Result()
		if err != nil || sha == "" {
			t.Fatalf("SCRIPT LOAD failed: got %v, err: %v", sha, err)
		}

		val, err := client.EvalSha(ctx, sha, []string{}, "loaded_value").Result()
		if err != nil || val != "loaded_value" {
			t.Fatalf("EVALSHA failed: got %v, err: %v", val, err)
		}
	})
}

