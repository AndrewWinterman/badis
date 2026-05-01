package proxy

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

type Server struct {
	addr    string
	router  *Router
	server  *redcon.Server
	clients map[string]*redis.Client
	mu      sync.RWMutex
}

func NewServer(addr string, router *Router) *Server {
	s := &Server{
		addr:    addr,
		router:  router,
		clients: make(map[string]*redis.Client),
	}
	s.server = redcon.NewServer(addr, s.handleCmd, func(conn redcon.Conn) bool { return true }, nil)
	return s
}

// Helper getter so test can find the port
func (s *Server) Addr() string {
	if s.server != nil && s.server.Addr() != nil {
		return s.server.Addr().String()
	}
	return s.addr
}

func (s *Server) getClient(addr string) *redis.Client {
	s.mu.RLock()
	if client, ok := s.clients[addr]; ok {
		s.mu.RUnlock()
		return client
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double check
	if client, ok := s.clients[addr]; ok {
		return client
	}

	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	s.clients[addr] = client
	return client
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	cmdName := strings.ToUpper(string(cmd.Args[0]))
	if cmdName == "HELLO" {
		conn.WriteRaw([]byte("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n"))
		return
	}
	if len(cmd.Args) < 2 {
		if cmdName == "PING" {
			conn.WriteString("PONG")
		} else {
			conn.WriteError("ERR wrong number of arguments for '" + strings.ToLower(cmdName) + "' command")
		}
		return
	}

	key := string(cmd.Args[1])
	shardAddr := s.router.LocateKey([]byte(key))
	if shardAddr == "" {
		conn.WriteError("ERR no shards available")
		return
	}

	client := s.getClient(shardAddr)

	// Convert args to interface{} slice for go-redis Do
	var args []interface{}
	for _, arg := range cmd.Args {
		args = append(args, string(arg))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.Do(ctx, args...).Result()
	if err != nil {
		if err == redis.Nil {
			conn.WriteNull()
			return
		}
		// If the backend returns a Redis error (like ERR), go-redis wraps it.
		// We want to pass it back raw. For now, simple string handling.
		if strings.HasPrefix(err.Error(), "ERR") {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR proxy error: " + err.Error())
		}
		return
	}

	// Simplistic response encoding based on type
	switch v := res.(type) {
	case string:
		conn.WriteBulkString(v)
	case []byte:
		conn.WriteBulk(v)
	case int64:
		conn.WriteInt64(v)
	case []interface{}:
		conn.WriteArray(len(v))
		for _, item := range v {
			switch iv := item.(type) {
			case string:
				conn.WriteBulkString(iv)
			case []byte:
				conn.WriteBulk(iv)
			case int64:
				conn.WriteInt64(iv)
			case nil:
				conn.WriteNull()
			default:
				conn.WriteBulkString(fmt.Sprintf("%v", iv))
			}
		}
	default:
		// Fallback for simple responses (like +OK)
		conn.WriteString("OK")
	}
}

func (s *Server) Start() error {
	log.Printf("Starting proxy on %s", s.addr)
	return s.server.ListenAndServe()
}

func (s *Server) Stop() {
	s.mu.Lock()
	for _, c := range s.clients {
		c.Close()
	}
	s.mu.Unlock()
	if s.server != nil {
		s.server.Close()
	}
}
