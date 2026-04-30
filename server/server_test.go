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
	defer ln.Close()
	return ln.Addr().String()
}

func TestBasicServer(t *testing.T) {
	dbPath, err := os.MkdirTemp("", "badis-test-srv-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

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
	defer os.RemoveAll(dbPath)

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
