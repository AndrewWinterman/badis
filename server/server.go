package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"github.com/winterman/badis/router"
	"github.com/winterman/badis/store"
)

type Server struct {
	addr         string
	mux          *redcon.ServeMux
	redconServer *redcon.Server
	fsm          *store.FSM
	raft         *raft.Raft
	router       *router.Router
	clients      map[string]*redis.Client
	mu           sync.RWMutex
}

func NewServer(addr string, fsm *store.FSM, router *router.Router) *Server {
	mux := redcon.NewServeMux()
	s := &Server{
		addr:    addr,
		mux:     mux,
		fsm:     fsm,
		router:  router,
		clients: make(map[string]*redis.Client),
	}

	mux.HandleFunc("ping", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("PONG")
	})
	mux.HandleFunc("set", s.handleSet)
	mux.HandleFunc("get", s.handleGet)
	mux.HandleFunc("del", s.handleDel)
	return s
}

func (s *Server) SetupRaft(localID string, transport raft.Transport, snapshotStore raft.SnapshotStore) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	r, err := raft.NewRaft(config, s.fsm, s.fsm, s.fsm, snapshotStore, transport)
	if err != nil {
		return err
	}
	s.raft = r
	return nil
}

func (s *Server) handleSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	var argsStr []string
	for _, arg := range cmd.Args {
		argsStr = append(argsStr, string(arg))
	}
	slog.Info("handleSet called", "args", argsStr)

	c := store.Command{Op: "SET", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}}

	for i := 3; i < len(cmd.Args); i++ {
		opt := strings.ToUpper(string(cmd.Args[i]))
		switch opt {
		case "NX":
			c.Condition = "NX"
		case "XX":
			c.Condition = "XX"
		case "GET":
			c.ReturnOld = true
		case "EX":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			secs, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			c.TTLMs = secs * 1000
			i++
		case "PX":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			ms, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			c.TTLMs = ms
			i++
		case "EXAT":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			timestamp, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			ttl := (timestamp * 1000) - time.Now().UnixMilli()
			if ttl < 0 {
				ttl = 1
			}
			c.TTLMs = ttl
			i++
		case "PXAT":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR syntax error")
				return
			}
			timestamp, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR value is not an integer or out of range")
				return
			}
			ttl := timestamp - time.Now().UnixMilli()
			if ttl < 0 {
				ttl = 1
			}
			c.TTLMs = ttl
			i++
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	var res interface{}
	if s.raft != nil {
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
		_ = enc.Encode(c)
		future := s.raft.Apply(buf.Bytes(), 5*time.Second)
		if err := future.Error(); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		res = future.Response()
		if err, ok := res.(error); ok && err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
	} else {
		// Fallback for tests if raft not setup
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
		_ = enc.Encode(c)
		res = s.fsm.Apply(&raft.Log{Data: buf.Bytes()})
		if err, ok := res.(error); ok && err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
	}

	if res == nil {
		conn.WriteNull()
		return
	}

	if c.ReturnOld {
		if b, ok := res.([]byte); ok {
			conn.WriteBulk(b)
		} else {
			conn.WriteNull()
		}
		return
	}

	conn.WriteString("OK")
}

func (s *Server) handleGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	val, err := s.fsm.GetString(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if val == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(val)
}

func (s *Server) handleDel(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	var res interface{}
	if s.raft != nil {
		c := store.Command{Op: "DEL", Key: string(cmd.Args[1])}
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
		_ = enc.Encode(c)
		future := s.raft.Apply(buf.Bytes(), 5*time.Second)
		if err := future.Error(); err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}

		res = future.Response()
		if err, ok := res.(error); ok && err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
	} else {
		// Fallback for tests if raft not setup
		c := store.Command{Op: "DEL", Key: string(cmd.Args[1])}
		var buf bytes.Buffer
		enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
		_ = enc.Encode(c)
		res = s.fsm.Apply(&raft.Log{Data: buf.Bytes()})
		if err, ok := res.(error); ok && err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
	}

	if count, ok := res.(int); ok {
		conn.WriteInt(count)
	} else {
		conn.WriteInt(1)
	}
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

	// Commands without a key or cluster-wide commands
	if len(cmd.Args) < 2 {
		if cmdName == "PING" {
			s.mux.ServeRESP(conn, cmd)
		} else {
			conn.WriteError("ERR wrong number of arguments for '" + strings.ToLower(cmdName) + "' command")
		}
		return
	}

	// For data commands, find the target shard
	key := string(cmd.Args[1])
	_, ip, isLocal := s.router.LocateKey([]byte(key))

	// Handle locally if this node owns the slot, or if the routing table is empty/uninitialized
	if isLocal || ip == "" {
		s.mux.ServeRESP(conn, cmd)
		return
	}

	// Proxy to the remote node
	client := s.getClient(ip)
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
		if strings.HasPrefix(err.Error(), "ERR") {
			conn.WriteError(err.Error())
		} else {
			conn.WriteError("ERR proxy error: " + err.Error())
		}
		return
	}

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
		conn.WriteString("OK")
	}
}

func (s *Server) Start() error {
	slog.Info("Starting server", "addr", s.addr)
	s.redconServer = redcon.NewServer(s.addr, s.handleCmd,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)
	return s.redconServer.ListenAndServe()
}

func (s *Server) Stop() {
	s.mu.Lock()
	for _, c := range s.clients {
		c.Close()
	}
	s.mu.Unlock()
	if s.redconServer != nil {
		_ = s.redconServer.Close()
	}
}
