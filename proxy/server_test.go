package proxy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

func TestProxyServer(t *testing.T) {
	// Mock backend shard
	backend := redcon.NewServer(":0", func(conn redcon.Conn, cmd redcon.Command) {
		switch strings.ToUpper(string(cmd.Args[0])) {
		case "HELLO":
			conn.WriteRaw([]byte("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n"))
		case "GET":
			conn.WriteBulkString("BACKEND_OK")
		default:
			conn.WriteString("OK")
		}
	}, func(conn redcon.Conn) bool { return true }, nil)
	go backend.ListenAndServe()
	time.Sleep(100 * time.Millisecond) // Wait for bind
	backendAddr := backend.Addr().String()
	defer backend.Close()

	// Start Proxy
	router := NewRouter([]string{backendAddr})
	proxy := NewServer(":0", router)
	go proxy.Start()
	time.Sleep(100 * time.Millisecond)
	defer proxy.Stop()

	// Test via Proxy
	rdb := redis.NewClient(&redis.Options{Addr: proxy.Addr(), Protocol: 2})
	val, err := rdb.Get(context.Background(), "somekey").Result()
	if err != nil {
		t.Fatal(err)
	}
	if val != "BACKEND_OK" {
		t.Fatalf("Expected BACKEND_OK, got %s", val)
	}
}
