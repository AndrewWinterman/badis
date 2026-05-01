package server

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	srv := NewServer(addr, fsm)
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
	srv := NewServer(addr, fsm)
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

	srv := NewServer("127.0.0.1:0", fsm)
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
