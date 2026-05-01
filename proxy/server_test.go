package proxy

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

func TestProxyServer(t *testing.T) {
	// Bind listener manually to avoid race conditions with :0
	backendLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backendAddr := backendLn.Addr().String()

	// Mock backend shard
	backend := redcon.NewServer(backendAddr, func(conn redcon.Conn, cmd redcon.Command) {
		switch strings.ToUpper(string(cmd.Args[0])) {
		case "HELLO":
			conn.WriteRaw([]byte("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n"))
		case "GET":
			conn.WriteBulkString("BACKEND_OK")
		default:
			conn.WriteString("OK")
		}
	}, func(conn redcon.Conn) bool { return true }, nil)

	go backend.Serve(backendLn)
	defer backend.Close()

	// Bind proxy listener manually
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := proxyLn.Addr().String()

	// Start Proxy
	router := NewRouter([]string{backendAddr})
	proxy := NewServer(proxyAddr, router)

	// Start with the manual listener
	go proxy.server.Serve(proxyLn)
	defer proxy.Stop()

	// Test via Proxy
	rdb := redis.NewClient(&redis.Options{Addr: proxyAddr, Protocol: 2})
	val, err := rdb.Get(context.Background(), "somekey").Result()
	if err != nil {
		t.Fatal(err)
	}
	if val != "BACKEND_OK" {
		t.Fatalf("Expected BACKEND_OK, got %s", val)
	}
}
