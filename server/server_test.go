package server

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBasicServer(t *testing.T) {
	srv := NewServer(":6380")
	go srv.Start()
	defer srv.Stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	defer rdb.Close()

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
