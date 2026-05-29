package server

import (
	"context"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/winterman/badis/config"
	"github.com/winterman/badis/router"
	"github.com/winterman/badis/store"
)

func getFreePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func newTestRedisClient(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "badis-server-cmds-*")
	if err != nil {
		t.Fatal(err)
	}

	fsm, err := store.NewFSM(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}

	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer("127.0.0.1:0", fsm, r, "shard-1")
	go func() { _ = srv.Start() }()

	for i := 0; i < 50 && srv.redconServer == nil; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if srv.redconServer == nil {
		srv.Stop()
		_ = fsm.Close()
		_ = os.RemoveAll(dir)
		t.Fatal("server failed to start")
	}

	rdb := redis.NewClient(&redis.Options{Addr: srv.redconServer.Addr().String()})
	ctx := context.Background()
	var pingErr error
	for i := 0; i < 50; i++ {
		pingErr = rdb.Ping(ctx).Err()
		if pingErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pingErr != nil {
		rdb.Close()
		srv.Stop()
		_ = fsm.Close()
		_ = os.RemoveAll(dir)
		t.Fatalf("server failed to respond: %v", pingErr)
	}

	cleanup := func() {
		_ = rdb.Close()
		srv.Stop()
		_ = fsm.Close()
		_ = os.RemoveAll(dir)
	}
	return rdb, cleanup
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		set[v] = true
	}
	return set
}

func TestServer_KeyCommands(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if err := rdb.Set(ctx, "key:string", "value", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := rdb.Do(ctx, "TOUCH", "key:string", "missing").Int(); err != nil || n != 1 {
		t.Fatalf("TOUCH expected 1, got n=%d err=%v", n, err)
	}
	if typ, err := rdb.Do(ctx, "TYPE", "key:string").Text(); err != nil || typ != "string" {
		t.Fatalf("TYPE string expected string, got %q err=%v", typ, err)
	}
	if ttl, err := rdb.Do(ctx, "PTTL", "key:string").Int64(); err != nil || ttl <= 0 {
		t.Fatalf("PTTL expected positive ttl, got %d err=%v", ttl, err)
	}
	if n, err := rdb.Do(ctx, "PERSIST", "key:string").Int(); err != nil || n != 1 {
		t.Fatalf("PERSIST expected 1, got n=%d err=%v", n, err)
	}
	if ttl, err := rdb.Do(ctx, "PTTL", "key:string").Int64(); err != nil || ttl != -1 {
		t.Fatalf("PTTL after persist expected -1, got %d err=%v", ttl, err)
	}
	if keys, err := rdb.Do(ctx, "KEYS", "key:*").StringSlice(); err != nil || !reflect.DeepEqual(keys, []string{"key:string"}) {
		t.Fatalf("KEYS expected [key:string], got %v err=%v", keys, err)
	}
	if n, err := rdb.Do(ctx, "UNLINK", "key:string").Int(); err != nil || n != 1 {
		t.Fatalf("UNLINK expected 1, got n=%d err=%v", n, err)
	}

	if err := rdb.Set(ctx, "glob:literal/one", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "glob:literal:two", "2", 0).Err(); err != nil {
		t.Fatal(err)
	}
	keys, err := rdb.Do(ctx, "KEYS", "glob:literal*one").StringSlice()
	if err != nil {
		t.Fatalf("KEYS glob failed: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"glob:literal/one"}) {
		t.Fatalf("KEYS should match / with *, got %v", keys)
	}
}

func TestServer_StringCommands(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if n, err := rdb.Do(ctx, "APPEND", "str", "foo").Int(); err != nil || n != 3 {
		t.Fatalf("APPEND expected 3, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "APPEND", "str", "bar").Int(); err != nil || n != 6 {
		t.Fatalf("APPEND second expected 6, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "STRLEN", "str").Int(); err != nil || n != 6 {
		t.Fatalf("STRLEN expected 6, got n=%d err=%v", n, err)
	}
	if old, err := rdb.Do(ctx, "GETSET", "str", "7").Text(); err != nil || old != "foobar" {
		t.Fatalf("GETSET expected foobar, got %q err=%v", old, err)
	}
	if n, err := rdb.Do(ctx, "INCRBY", "str", "5").Int64(); err != nil || n != 12 {
		t.Fatalf("INCRBY expected 12, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "DECRBY", "str", "2").Int64(); err != nil || n != 10 {
		t.Fatalf("DECRBY expected 10, got n=%d err=%v", n, err)
	}
	if res, err := rdb.Do(ctx, "SETEX", "expiring", "1", "v").Text(); err != nil || res != "OK" {
		t.Fatalf("SETEX expected OK, got %q err=%v", res, err)
	}
	if ttl, err := rdb.Do(ctx, "PTTL", "expiring").Int64(); err != nil || ttl <= 0 {
		t.Fatalf("SETEX expected positive pttl, got %d err=%v", ttl, err)
	}
	if err := rdb.Do(ctx, "SETEX", "bad", "0", "v").Err(); err == nil {
		t.Fatal("SETEX zero seconds expected error")
	}
	if err := rdb.Do(ctx, "SETEX", "bad", "-1", "v").Err(); err == nil {
		t.Fatal("SETEX negative seconds expected error")
	}
}

func TestServer_HashCommands(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if n, err := rdb.Do(ctx, "HSET", "hash", "a", "1", "b", "2").Int(); err != nil || n != 2 {
		t.Fatalf("HSET expected 2, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "HSETNX", "hash", "a", "x").Int(); err != nil || n != 0 {
		t.Fatalf("HSETNX existing expected 0, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "HSETNX", "hash", "c", "3").Int(); err != nil || n != 1 {
		t.Fatalf("HSETNX new expected 1, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "HEXISTS", "hash", "c").Int(); err != nil || n != 1 {
		t.Fatalf("HEXISTS expected 1, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "HLEN", "hash").Int(); err != nil || n != 3 {
		t.Fatalf("HLEN expected 3, got n=%d err=%v", n, err)
	}
	if vals, err := rdb.Do(ctx, "HMGET", "hash", "a", "missing", "c").Slice(); err != nil || string(vals[0].(string)) != "1" || vals[1] != nil || vals[2].(string) != "3" {
		t.Fatalf("HMGET unexpected vals=%v err=%v", vals, err)
	}
	keys, err := rdb.Do(ctx, "HKEYS", "hash").StringSlice()
	if err != nil {
		t.Fatalf("HKEYS failed: %v", err)
	}
	if !reflect.DeepEqual(stringSet(keys), map[string]bool{"a": true, "b": true, "c": true}) {
		t.Fatalf("HKEYS unexpected %v", keys)
	}
	vals, err := rdb.Do(ctx, "HVALS", "hash").StringSlice()
	if err != nil {
		t.Fatalf("HVALS failed: %v", err)
	}
	if !reflect.DeepEqual(stringSet(vals), map[string]bool{"1": true, "2": true, "3": true}) {
		t.Fatalf("HVALS unexpected %v", vals)
	}
	if n, err := rdb.Do(ctx, "HDEL", "hash", "a", "missing").Int(); err != nil || n != 1 {
		t.Fatalf("HDEL expected 1, got n=%d err=%v", n, err)
	}
}

func TestServer_ListCommands(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if n, err := rdb.Do(ctx, "LPUSHX", "list", "x").Int(); err != nil || n != 0 {
		t.Fatalf("LPUSHX missing expected 0, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "RPUSH", "list", "a", "b", "c", "b").Int(); err != nil || n != 4 {
		t.Fatalf("RPUSH expected 4, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "LPUSHX", "list", "z").Int(); err != nil || n != 5 {
		t.Fatalf("LPUSHX existing expected 5, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "RPUSHX", "list", "y").Int(); err != nil || n != 6 {
		t.Fatalf("RPUSHX existing expected 6, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "LLEN", "list").Int(); err != nil || n != 6 {
		t.Fatalf("LLEN expected 6, got n=%d err=%v", n, err)
	}
	if val, err := rdb.Do(ctx, "LINDEX", "list", "1").Text(); err != nil || val != "a" {
		t.Fatalf("LINDEX expected a, got %q err=%v", val, err)
	}
	if n, err := rdb.Do(ctx, "LREM", "list", "0", "b").Int(); err != nil || n != 2 {
		t.Fatalf("LREM expected 2, got n=%d err=%v", n, err)
	}
	if res, err := rdb.Do(ctx, "LTRIM", "list", "0", "2").Text(); err != nil || res != "OK" {
		t.Fatalf("LTRIM expected OK, got %q err=%v", res, err)
	}
	if vals, err := rdb.Do(ctx, "LPOP", "list", "2").StringSlice(); err != nil || !reflect.DeepEqual(vals, []string{"z", "a"}) {
		t.Fatalf("LPOP count expected [z a], got %v err=%v", vals, err)
	}
	if val, err := rdb.Do(ctx, "RPOP", "list").Text(); err != nil || val != "c" {
		t.Fatalf("RPOP expected c, got %q err=%v", val, err)
	}

	if n, err := rdb.Do(ctx, "RPUSH", "rpop-count", "a", "b", "c").Int(); err != nil || n != 3 {
		t.Fatalf("RPUSH rpop-count expected 3, got n=%d err=%v", n, err)
	}
	if vals, err := rdb.Do(ctx, "RPOP", "rpop-count", "2").StringSlice(); err != nil || !reflect.DeepEqual(vals, []string{"c", "b"}) {
		t.Fatalf("RPOP count expected [c b], got %v err=%v", vals, err)
	}

	if n, err := rdb.Do(ctx, "RPUSH", "empty-pop", "x").Int(); err != nil || n != 1 {
		t.Fatalf("RPUSH empty-pop expected 1, got n=%d err=%v", n, err)
	}
	if vals, err := rdb.Do(ctx, "LPOP", "empty-pop", "1").StringSlice(); err != nil || !reflect.DeepEqual(vals, []string{"x"}) {
		t.Fatalf("LPOP full expected [x], got %v err=%v", vals, err)
	}
	if typ, err := rdb.Do(ctx, "TYPE", "empty-pop").Text(); err != nil || typ != "none" {
		t.Fatalf("TYPE after full pop expected none, got %q err=%v", typ, err)
	}
	if n, err := rdb.Do(ctx, "LPUSHX", "empty-pop", "y").Int(); err != nil || n != 0 {
		t.Fatalf("LPUSHX after full pop expected 0, got n=%d err=%v", n, err)
	}

	if n, err := rdb.Do(ctx, "RPUSH", "empty-trim", "x").Int(); err != nil || n != 1 {
		t.Fatalf("RPUSH empty-trim expected 1, got n=%d err=%v", n, err)
	}
	if res, err := rdb.Do(ctx, "LTRIM", "empty-trim", "2", "1").Text(); err != nil || res != "OK" {
		t.Fatalf("LTRIM empty expected OK, got %q err=%v", res, err)
	}
	if n, err := rdb.Do(ctx, "EXISTS", "empty-trim").Int(); err != nil || n != 0 {
		t.Fatalf("EXISTS after empty trim expected 0, got n=%d err=%v", n, err)
	}

	if n, err := rdb.Do(ctx, "RPUSH", "empty-rem", "x").Int(); err != nil || n != 1 {
		t.Fatalf("RPUSH empty-rem expected 1, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "LREM", "empty-rem", "0", "x").Int(); err != nil || n != 1 {
		t.Fatalf("LREM full expected 1, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "RPUSHX", "empty-rem", "y").Int(); err != nil || n != 0 {
		t.Fatalf("RPUSHX after full rem expected 0, got n=%d err=%v", n, err)
	}
}

func TestServer_SetCommands(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if n, err := rdb.Do(ctx, "SADD", "set", "a", "b", "c").Int(); err != nil || n != 3 {
		t.Fatalf("SADD expected 3, got n=%d err=%v", n, err)
	}
	if n, err := rdb.Do(ctx, "SCARD", "set").Int(); err != nil || n != 3 {
		t.Fatalf("SCARD expected 3, got n=%d err=%v", n, err)
	}
	vals, err := rdb.Do(ctx, "SMISMEMBER", "set", "a", "x", "c").Slice()
	if err != nil || !reflect.DeepEqual(vals, []interface{}{int64(1), int64(0), int64(1)}) {
		t.Fatalf("SMISMEMBER expected [1 0 1], got %v err=%v", vals, err)
	}
	if n, err := rdb.Do(ctx, "SREM", "set", "b", "x").Int(); err != nil || n != 1 {
		t.Fatalf("SREM expected 1, got n=%d err=%v", n, err)
	}
	if member, err := rdb.Do(ctx, "SRANDMEMBER", "set").Text(); err != nil || !stringSet([]string{"a", "c"})[member] {
		t.Fatalf("SRANDMEMBER unexpected member=%q err=%v", member, err)
	}
	if members, err := rdb.Do(ctx, "SPOP", "set", "2").StringSlice(); err != nil || len(members) != 2 {
		t.Fatalf("SPOP count expected 2 members, got %v err=%v", members, err)
	}
	if n, err := rdb.Do(ctx, "SCARD", "set").Int(); err != nil || n != 0 {
		t.Fatalf("SCARD after SPOP expected 0, got n=%d err=%v", n, err)
	}
	if err := rdb.Do(ctx, "SPOP", "missing").Err(); err != redis.Nil {
		t.Fatalf("SPOP missing expected redis.Nil, got %v", err)
	}
	if err := rdb.Do(ctx, "SPOP", "set", "-1").Err(); err == nil {
		t.Fatal("SPOP negative count expected error")
	}

	if n, err := rdb.Do(ctx, "SADD", "set2", "2", "1").Int(); err != nil || n != 2 {
		t.Fatalf("SADD set2 expected 2, got n=%d err=%v", n, err)
	}
	dupes, err := rdb.Do(ctx, "SRANDMEMBER", "set2", "-3").StringSlice()
	if err != nil || len(dupes) != 3 {
		t.Fatalf("SRANDMEMBER negative count expected 3 values, got %v err=%v", dupes, err)
	}
	for _, member := range dupes {
		if !stringSet([]string{"1", "2"})[member] {
			t.Fatalf("SRANDMEMBER negative count returned unexpected member %q", member)
		}
	}
	members, err := rdb.Do(ctx, "SMEMBERS", "set2").StringSlice()
	if err != nil {
		t.Fatalf("SMEMBERS set2 failed: %v", err)
	}
	sort.Strings(members)
	if !reflect.DeepEqual(members, []string{"1", "2"}) {
		t.Fatalf("SMEMBERS set2 unexpected %v", members)
	}
}

func TestServer_CommandMutationsPreserveTTL(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name   string
		key    string
		setup  func() error
		mutate func() error
	}{
		{
			name: "append",
			key:  "ttl:append",
			setup: func() error {
				return rdb.Set(ctx, "ttl:append", "1", time.Minute).Err()
			},
			mutate: func() error {
				return rdb.Do(ctx, "APPEND", "ttl:append", "2").Err()
			},
		},
		{
			name: "incrby",
			key:  "ttl:incr",
			setup: func() error {
				return rdb.Set(ctx, "ttl:incr", "1", time.Minute).Err()
			},
			mutate: func() error {
				return rdb.Do(ctx, "INCRBY", "ttl:incr", "2").Err()
			},
		},
		{
			name: "hset",
			key:  "ttl:hash",
			setup: func() error {
				if err := rdb.Do(ctx, "HSET", "ttl:hash", "a", "1").Err(); err != nil {
					return err
				}
				return rdb.Expire(ctx, "ttl:hash", time.Minute).Err()
			},
			mutate: func() error {
				return rdb.Do(ctx, "HSET", "ttl:hash", "b", "2").Err()
			},
		},
		{
			name: "lpush",
			key:  "ttl:list",
			setup: func() error {
				if err := rdb.Do(ctx, "RPUSH", "ttl:list", "a").Err(); err != nil {
					return err
				}
				return rdb.Expire(ctx, "ttl:list", time.Minute).Err()
			},
			mutate: func() error {
				return rdb.Do(ctx, "LPUSH", "ttl:list", "b").Err()
			},
		},
		{
			name: "sadd",
			key:  "ttl:set",
			setup: func() error {
				if err := rdb.Do(ctx, "SADD", "ttl:set", "a").Err(); err != nil {
					return err
				}
				return rdb.Expire(ctx, "ttl:set", time.Minute).Err()
			},
			mutate: func() error {
				return rdb.Do(ctx, "SADD", "ttl:set", "b").Err()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			if err := tc.mutate(); err != nil {
				t.Fatalf("mutate failed: %v", err)
			}
			ttl, err := rdb.Do(ctx, "PTTL", tc.key).Int64()
			if err != nil || ttl <= 0 {
				t.Fatalf("PTTL after %s expected positive, got %d err=%v", tc.name, ttl, err)
			}
		})
	}
}

func TestServer_WrongTypeErrors(t *testing.T) {
	rdb, cleanup := newTestRedisClient(t)
	defer cleanup()
	ctx := context.Background()

	if err := rdb.Do(ctx, "HSET", "wrong:hash", "field", "value").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Get(ctx, "wrong:hash").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("GET hash expected WRONGTYPE, got %v", err)
	}
	if err := rdb.Do(ctx, "APPEND", "wrong:hash", "x").Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("APPEND hash expected WRONGTYPE, got %v", err)
	}

	if err := rdb.Set(ctx, "wrong:string", "value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]interface{}{
		{"HSET", "wrong:string", "field", "value"},
		{"HGET", "wrong:string", "field"},
		{"LPUSH", "wrong:string", "value"},
		{"LRANGE", "wrong:string", "0", "-1"},
		{"SADD", "wrong:string", "value"},
		{"SMEMBERS", "wrong:string"},
	} {
		err := rdb.Do(ctx, args...).Err()
		if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Fatalf("%v expected WRONGTYPE, got %v", args, err)
		}
	}
}

func TestBasicServer(t *testing.T) {
	dbPath, err := os.MkdirTemp("", "badis-test-srv-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dbPath) }()

	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	addr := getFreePort(t)
	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer(addr, fsm, r, "shard-1")
	go func() { _ = srv.Start() }()
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	var pingErr error
	for i := 0; i < 50; i++ {
		pingErr = rdb.Ping(context.Background()).Err()
		if pingErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if pingErr != nil {
		t.Fatalf("Ping failed: %v", pingErr)
	}
}

func TestSetGet(t *testing.T) {
	dbPath, err := os.MkdirTemp("", "badis-test-srv2-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dbPath) }()

	fsm, err := store.NewFSM(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	addr := getFreePort(t)
	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer(addr, fsm, r, "shard-1")
	go func() { _ = srv.Start() }()
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	var pingErr error
	for i := 0; i < 50; i++ {
		pingErr = rdb.Ping(context.Background()).Err()
		if pingErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pingErr != nil {
		t.Fatalf("Server failed to start: %v", pingErr)
	}

	err = rdb.Set(context.Background(), "k1", "v1", 0).Err()
	if err != nil {
		t.Fatal(err)
	}

	val, err := rdb.Get(context.Background(), "k1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if val != "v1" {
		t.Fatalf("Expected v1, got %v", val)
	}
}

func TestSetModifiers(t *testing.T) {
	dir, err := os.MkdirTemp("", "badis-server-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fsm, err := store.NewFSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer("127.0.0.1:0", fsm, r, "shard-1")
	go srv.Start()
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	addr := srv.redconServer.Addr().String()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()

	// Wait for start
	for i := 0; i < 50; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// SETNX
	res, err := rdb.Do(ctx, "SET", "nxkey", "1", "NX").Result()
	if err != nil {
		t.Fatalf("SET NX error: %v", err)
	}
	if res != "OK" {
		t.Fatalf("SET NX expected OK, got %v", res)
	}

	// SETNX again - should return nil
	res, err = rdb.Do(ctx, "SET", "nxkey", "2", "NX").Result()
	if err != redis.Nil {
		t.Fatalf("SET NX on existing key expected redis.Nil, got err=%v, res=%v", err, res)
	}
}

func TestServer_ClientName(t *testing.T) {
	dir, err := os.MkdirTemp("", "badis-server-client-name-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fsm, err := store.NewFSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer("127.0.0.1:0", fsm, r, "shard-1")
	go srv.Start()
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	addr := srv.redconServer.Addr().String()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn := rdb.Conn()
	defer conn.Close()

	if err := conn.ClientSetName(ctx, "foobar").Err(); err != nil {
		t.Fatalf("CLIENT SETNAME failed: %v", err)
	}

	name, err := conn.ClientGetName(ctx).Result()
	if err != nil {
		t.Fatalf("CLIENT GETNAME failed: %v", err)
	}
	if name != "foobar" {
		t.Fatalf("CLIENT GETNAME expected foobar, got %q", name)
	}
}

func TestServer_ClientInfo(t *testing.T) {
	dir, err := os.MkdirTemp("", "badis-server-client-info-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fsm, err := store.NewFSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer("127.0.0.1:0", fsm, r, "shard-1")
	go srv.Start()
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	addr := srv.redconServer.Addr().String()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn := rdb.Conn()
	defer conn.Close()

	if err := conn.ClientSetInfo(ctx, redis.WithLibraryVersion("1.2.3")).Err(); err != nil {
		t.Fatalf("CLIENT SETINFO failed: %v", err)
	}

	info, err := conn.ClientInfo(ctx).Result()
	if err != nil {
		t.Fatalf("CLIENT INFO failed: %v", err)
	}
	if info.LibVer != "1.2.3" {
		t.Fatalf("CLIENT INFO expected lib-ver 1.2.3, got %q", info.LibVer)
	}
}

func TestServer_FlushDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "badis-server-flushdb-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fsm, err := store.NewFSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()

	sm := config.NewSlotMap()
	r := router.NewRouter(nil, sm, "test-node")
	srv := NewServer("127.0.0.1:0", fsm, r, "shard-1")
	go srv.Start()
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	addr := srv.redconServer.Addr().String()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := rdb.Set(ctx, "flushdb:key", "value", 0).Err(); err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if err := rdb.Do(ctx, "HSET", "flushdb:hash", "field", "value").Err(); err != nil {
		t.Fatalf("HSET failed: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FLUSHDB failed: %v", err)
	}
	if err := rdb.Get(ctx, "flushdb:key").Err(); err != redis.Nil {
		t.Fatalf("GET after FLUSHDB expected redis.Nil, got %v", err)
	}
	if typ, err := rdb.Do(ctx, "TYPE", "flushdb:hash").Text(); err != nil || typ != "none" {
		t.Fatalf("TYPE hash after FLUSHDB expected none, got %q err=%v", typ, err)
	}
	if err := rdb.Set(ctx, "flushdb:hash", "value", 0).Err(); err != nil {
		t.Fatalf("SET after FLUSHDB failed: %v", err)
	}
	if typ, err := rdb.Do(ctx, "TYPE", "flushdb:hash").Text(); err != nil || typ != "string" {
		t.Fatalf("TYPE after string rewrite expected string, got %q err=%v", typ, err)
	}
}
