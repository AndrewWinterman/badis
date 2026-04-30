package server

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestBasicServer(t *testing.T) {
	srv := NewServer(":6380")
	go srv.Start()
	time.Sleep(100 * time.Millisecond) // Wait for start
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}
