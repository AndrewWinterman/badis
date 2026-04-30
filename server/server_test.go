package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/winterman/badis/store"
)

func TestBasicServer(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-test-srv-*")
	defer os.RemoveAll(dbPath)
	fsm, _ := store.NewFSM(dbPath)

	srv := NewServer(":6380", fsm)
	go func() { _ = srv.Start() }()
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	defer func() { _ = rdb.Close() }()

	var err error
	for i := 0; i < 10; i++ {
		err = rdb.Ping(context.Background()).Err()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestSetGet(t *testing.T) {
	dbPath, _ := os.MkdirTemp("", "badis-test-srv2-*")
	defer os.RemoveAll(dbPath)
	fsm, _ := store.NewFSM(dbPath)

	srv := NewServer(":6381", fsm)
	go func() { _ = srv.Start() }()
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6381"})
	defer func() { _ = rdb.Close() }()

	// Wait for server to start
	for i := 0; i < 10; i++ {
		if rdb.Ping(context.Background()).Err() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	err := rdb.Set(context.Background(), "k1", "v1", 0).Err()
	if err != nil {
		t.Fatal(err)
	}

	val, err := rdb.Get(context.Background(), "k1").Result()
	if val != "v1" {
		t.Fatalf("Expected v1, got %v", val)
	}
}
