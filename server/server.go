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
	mux.HandleFunc("mset", s.handleMSet)
	mux.HandleFunc("mget", s.handleMGet)
	mux.HandleFunc("incr", s.handleIncr)
	mux.HandleFunc("decr", s.handleDecr)
	mux.HandleFunc("exists", s.handleExists)
	mux.HandleFunc("expire", s.handleExpire)
	mux.HandleFunc("ttl", s.handleTTL)
	mux.HandleFunc("hset", s.handleHSet)
	mux.HandleFunc("hget", s.handleHGet)
	mux.HandleFunc("hgetall", s.handleHGetAll)
	mux.HandleFunc("lpush", s.handleLPush)
	mux.HandleFunc("rpush", s.handleRPush)
	mux.HandleFunc("lpop", s.handleLPop)
	mux.HandleFunc("rpop", s.handleRPop)
	mux.HandleFunc("lrange", s.handleLRange)
	mux.HandleFunc("sadd", s.handleSAdd)
	mux.HandleFunc("smembers", s.handleSMembers)
	mux.HandleFunc("sismember", s.handleSIsMember)
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
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var args [][]byte
	for i := 1; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "DEL", Args: args})
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
func (s *Server) handleMSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 || len(cmd.Args)%2 == 0 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var args [][]byte
	for i := 1; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "MSET", Args: args})
}

func (s *Server) handleMGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	var keys []string
	for i := 1; i < len(cmd.Args); i++ {
		keys = append(keys, string(cmd.Args[i]))
	}
	vals, err := s.fsm.GetStrings(keys)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, val := range vals {
		if val == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(val)
		}
	}
}

func (s *Server) handleIncr(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "INCR", Key: string(cmd.Args[1])})
}

func (s *Server) handleDecr(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "DECR", Key: string(cmd.Args[1])})
}

func (s *Server) handleExists(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var keys []string
	for i := 1; i < len(cmd.Args); i++ {
		keys = append(keys, string(cmd.Args[i]))
	}
	count, err := s.fsm.Exists(keys)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(count)
}

func (s *Server) handleExpire(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	secs, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	s.applyCommand(conn, store.Command{Op: "EXPIRE", Key: string(cmd.Args[1]), TTLMs: secs * 1000})
}

func (s *Server) handleTTL(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	ttl, err := s.fsm.TTL(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(ttl)
}

func (s *Server) handleHSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 4 || len(cmd.Args)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "HSET", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleHGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	val, err := s.fsm.HGet(string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (s *Server) handleHGetAll(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	hash, err := s.fsm.HGetAll(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(hash) * 2)
	for k, v := range hash {
		conn.WriteBulkString(k)
		conn.WriteBulkString(v)
	}
}

func (s *Server) handleLPush(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "LPUSH", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleRPush(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "RPUSH", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleLPop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "LPOP", Key: string(cmd.Args[1])})
}

func (s *Server) handleRPop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "RPOP", Key: string(cmd.Args[1])})
}

func (s *Server) handleLRange(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	start, err1 := strconv.Atoi(string(cmd.Args[2]))
	stop, err2 := strconv.Atoi(string(cmd.Args[3]))
	if err1 != nil || err2 != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	list, err := s.fsm.LRange(string(cmd.Args[1]), start, stop)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(list))
	for _, v := range list {
		conn.WriteBulkString(v)
	}
}

func (s *Server) handleSAdd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "SADD", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleSMembers(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	members, err := s.fsm.SMembers(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(members))
	for _, m := range members {
		conn.WriteBulkString(m)
	}
}

func (s *Server) handleSIsMember(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	isMember, err := s.fsm.SIsMember(string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(isMember)
}

func (s *Server) applyCommand(conn redcon.Conn, c store.Command) {
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
	
	switch v := res.(type) {
	case string:
		conn.WriteString(v)
	case []byte:
		conn.WriteBulk(v)
	case int:
		conn.WriteInt(v)
	case int64:
		conn.WriteInt64(v)
	default:
		conn.WriteString("OK")
	}
}
