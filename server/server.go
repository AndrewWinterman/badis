package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
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
	shardID      string
	clients      map[string]*redis.Client
	mu           sync.RWMutex
	logOutput    io.Writer
	logger       *slog.Logger
}

func NewServer(addr string, fsm *store.FSM, router *router.Router, shardID string) *Server {
	return NewServerWithOptions(addr, fsm, router, shardID, WithLogOutput(os.Stderr))
}

func NewServerWithOptions(addr string, fsm *store.FSM, router *router.Router, shardID string, opts ...Option) *Server {
	mux := redcon.NewServeMux()
	resolved := resolveOptions(opts)
	s := &Server{
		addr:      addr,
		mux:       mux,
		fsm:       fsm,
		router:    router,
		shardID:   shardID,
		clients:   make(map[string]*redis.Client),
		logOutput: resolved.logOutput,
		logger:    slog.New(slog.NewTextHandler(resolved.logOutput, nil)),
	}

	mux.HandleFunc("ping", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("PONG")
	})
	mux.HandleFunc("client", s.handleClient)
	mux.HandleFunc("set", s.handleSet)
	mux.HandleFunc("get", s.handleGet)
	mux.HandleFunc("del", s.handleDel)
	mux.HandleFunc("unlink", s.handleDel)
	mux.HandleFunc("type", s.handleType)
	mux.HandleFunc("keys", s.handleKeys)
	mux.HandleFunc("touch", s.handleTouch)
	mux.HandleFunc("persist", s.handlePersist)
	mux.HandleFunc("pttl", s.handlePTTL)
	mux.HandleFunc("mset", s.handleMSet)
	mux.HandleFunc("mget", s.handleMGet)
	mux.HandleFunc("flushdb", s.handleFlushDB)
	mux.HandleFunc("append", s.handleAppend)
	mux.HandleFunc("strlen", s.handleStrLen)
	mux.HandleFunc("getset", s.handleGetSet)
	mux.HandleFunc("incrby", s.handleIncrBy)
	mux.HandleFunc("decrby", s.handleDecrBy)
	mux.HandleFunc("setex", s.handleSetEx)
	mux.HandleFunc("incr", s.handleIncr)
	mux.HandleFunc("decr", s.handleDecr)
	mux.HandleFunc("exists", s.handleExists)
	mux.HandleFunc("expire", s.handleExpire)
	mux.HandleFunc("ttl", s.handleTTL)
	mux.HandleFunc("hset", s.handleHSet)
	mux.HandleFunc("hsetnx", s.handleHSetNX)
	mux.HandleFunc("hget", s.handleHGet)
	mux.HandleFunc("hgetall", s.handleHGetAll)
	mux.HandleFunc("hdel", s.handleHDel)
	mux.HandleFunc("hexists", s.handleHExists)
	mux.HandleFunc("hlen", s.handleHLen)
	mux.HandleFunc("hkeys", s.handleHKeys)
	mux.HandleFunc("hvals", s.handleHVals)
	mux.HandleFunc("hmget", s.handleHMGet)
	mux.HandleFunc("lpush", s.handleLPush)
	mux.HandleFunc("rpush", s.handleRPush)
	mux.HandleFunc("lpushx", s.handleLPushX)
	mux.HandleFunc("rpushx", s.handleRPushX)
	mux.HandleFunc("lpop", s.handleLPop)
	mux.HandleFunc("rpop", s.handleRPop)
	mux.HandleFunc("lrange", s.handleLRange)
	mux.HandleFunc("llen", s.handleLLen)
	mux.HandleFunc("lindex", s.handleLIndex)
	mux.HandleFunc("ltrim", s.handleLTrim)
	mux.HandleFunc("lrem", s.handleLRem)
	mux.HandleFunc("sadd", s.handleSAdd)
	mux.HandleFunc("smembers", s.handleSMembers)
	mux.HandleFunc("sismember", s.handleSIsMember)
	mux.HandleFunc("scard", s.handleSCard)
	mux.HandleFunc("srem", s.handleSRem)
	mux.HandleFunc("spop", s.handleSPop)
	mux.HandleFunc("srandmember", s.handleSRandMember)
	mux.HandleFunc("smismember", s.handleSMIsMember)
	mux.HandleFunc("eval", s.handleEval)
	mux.HandleFunc("evalsha", s.handleEvalSha)
	mux.HandleFunc("script", s.handleScript)
	return s
}

type connectionContext struct {
	clientName string
	libName    string
	libVer     string
}

func contextForConn(conn redcon.Conn) *connectionContext {
	ctx, ok := conn.Context().(*connectionContext)
	if !ok {
		ctx = &connectionContext{}
		conn.SetContext(ctx)
	}
	return ctx
}

func (s *Server) handleClient(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'client' command")
		return
	}

	subcmd := strings.ToUpper(string(cmd.Args[1]))
	switch subcmd {
	case "SETNAME":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		contextForConn(conn).clientName = string(cmd.Args[2])
		conn.WriteString("OK")
	case "GETNAME":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'client|getname' command")
			return
		}
		name := contextForConn(conn).clientName
		if name == "" {
			conn.WriteNull()
			return
		}
		conn.WriteBulkString(name)
	case "SETINFO":
		if len(cmd.Args) != 4 {
			conn.WriteError("ERR wrong number of arguments for 'client|setinfo' command")
			return
		}
		ctx := contextForConn(conn)
		switch strings.ToUpper(string(cmd.Args[2])) {
		case "LIB-NAME":
			ctx.libName = string(cmd.Args[3])
		case "LIB-VER":
			ctx.libVer = string(cmd.Args[3])
		default:
			conn.WriteError("ERR syntax error")
			return
		}
		conn.WriteString("OK")
	case "INFO":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for 'client|info' command")
			return
		}
		ctx := contextForConn(conn)
		conn.WriteBulkString(fmt.Sprintf(
			"id=1 addr=%s laddr=%s fd=0 name=%s age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2 lib-name=%s lib-ver=%s",
			conn.RemoteAddr(), s.addr, ctx.clientName, ctx.libName, ctx.libVer,
		))
	default:
		conn.WriteError("ERR unknown subcommand '" + strings.ToLower(subcmd) + "'. Try CLIENT HELP.")
	}
}

func (s *Server) SetupRaft(localID string, raftBindAddr string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	config.LogOutput = s.logOutput

	addr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, addr, 3, 10*time.Second, s.logOutput)
	if err != nil {
		return err
	}

	snapshotStore := raft.NewDiscardSnapshotStore()

	r, err := raft.NewRaft(config, s.fsm, s.fsm, s.fsm, snapshotStore, transport)
	if err != nil {
		return err
	}
	s.raft = r
	return nil
}

func (s *Server) GetRaft() *raft.Raft {
	return s.raft
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
	s.logger.Info("handleSet called", "args", argsStr)

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

func (s *Server) handleType(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	typ, err := s.fsm.Type(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(typ)
}

func (s *Server) handleKeys(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	keys, err := s.fsm.Keys(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError("ERR syntax error")
		return
	}
	conn.WriteArray(len(keys))
	for _, key := range keys {
		conn.WriteBulkString(key)
	}
}

func (s *Server) handleTouch(conn redcon.Conn, cmd redcon.Command) {
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

func (s *Server) handlePersist(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "PERSIST", Key: string(cmd.Args[1])})
}

func (s *Server) handlePTTL(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	ttl, err := s.fsm.PTTL(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(ttl)
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
	if cmdName == "CLIENT" || cmdName == "EVAL" || cmdName == "EVALSHA" || cmdName == "SCRIPT" {
		s.mux.ServeRESP(conn, cmd)
		return
	}

	// Commands without a key or cluster-wide commands
	if len(cmd.Args) < 2 {
		if cmdName == "PING" || cmdName == "FLUSHDB" {
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
	s.logger.Info("Starting server", "addr", s.addr)
	s.redconServer = redcon.NewServer(s.addr, s.handleCmd,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)
	return s.redconServer.ListenAndServe()
}

func (s *Server) handleRaftJoin(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("id")
	addr := r.URL.Query().Get("addr")

	if s.raft.State() != raft.Leader {
		http.Error(w, "Not the leader", http.StatusBadRequest)
		return
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// Already joined
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		http.Error(w, f.Error().Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) StartAdmin(adminPort string) {
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/join", s.handleRaftJoin)
	go http.ListenAndServe(adminPort, adminMux)
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

func (s *Server) handleAppend(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "APPEND", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}})
}

func (s *Server) handleStrLen(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	n, err := s.fsm.StrLen(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(n)
}

func (s *Server) handleGetSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "GETSET", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}})
}

func (s *Server) handleIncrBy(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "INCRBY", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}})
}

func (s *Server) handleDecrBy(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "DECRBY", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}})
}

func (s *Server) handleSetEx(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	secs, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil || secs <= 0 {
		conn.WriteError("ERR invalid expire time in 'setex' command")
		return
	}
	s.applyCommand(conn, store.Command{Op: "SET", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[3]}, TTLMs: secs * 1000})
}

func (s *Server) handleFlushDB(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 1 && len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'flushdb' command")
		return
	}
	if len(cmd.Args) == 2 && strings.ToUpper(string(cmd.Args[1])) != "SYNC" && strings.ToUpper(string(cmd.Args[1])) != "ASYNC" {
		conn.WriteError("ERR syntax error")
		return
	}
	s.applyCommand(conn, store.Command{Op: "FLUSHDB"})
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

func (s *Server) handleHSetNX(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	s.applyCommand(conn, store.Command{Op: "HSETNX", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2], cmd.Args[3]}})
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

func (s *Server) handleHDel(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "HDEL", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleHExists(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	n, err := s.fsm.HExists(string(cmd.Args[1]), string(cmd.Args[2]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(n)
}

func (s *Server) handleHLen(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	n, err := s.fsm.HLen(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(n)
}

func (s *Server) handleHKeys(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	keys, err := s.fsm.HKeys(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(keys))
	for _, key := range keys {
		conn.WriteBulkString(key)
	}
}

func (s *Server) handleHVals(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	vals, err := s.fsm.HVals(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(vals))
	for _, val := range vals {
		conn.WriteBulkString(val)
	}
}

func (s *Server) handleHMGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var fields []string
	for i := 2; i < len(cmd.Args); i++ {
		fields = append(fields, string(cmd.Args[i]))
	}
	vals, err := s.fsm.HMGet(string(cmd.Args[1]), fields)
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

func (s *Server) handleLPushX(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "LPUSHX", Key: string(cmd.Args[1]), Args: args})
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

func (s *Server) handleRPushX(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "RPUSHX", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleLPop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	if len(cmd.Args) == 3 {
		args = append(args, cmd.Args[2])
	}
	s.applyCommand(conn, store.Command{Op: "LPOP", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleRPop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	if len(cmd.Args) == 3 {
		args = append(args, cmd.Args[2])
	}
	s.applyCommand(conn, store.Command{Op: "RPOP", Key: string(cmd.Args[1]), Args: args})
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

func (s *Server) handleLLen(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	n, err := s.fsm.LLen(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(n)
}

func (s *Server) handleLIndex(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	index, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	val, err := s.fsm.LIndex(string(cmd.Args[1]), index)
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

func (s *Server) handleLTrim(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	if _, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	if _, err := strconv.Atoi(string(cmd.Args[3])); err != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	s.applyCommand(conn, store.Command{Op: "LTRIM", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2], cmd.Args[3]}})
}

func (s *Server) handleLRem(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	if _, err := strconv.Atoi(string(cmd.Args[2])); err != nil {
		conn.WriteError("ERR value is not an integer")
		return
	}
	s.applyCommand(conn, store.Command{Op: "LREM", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2], cmd.Args[3]}})
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

func (s *Server) handleSCard(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	n, err := s.fsm.SCard(string(cmd.Args[1]))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(n)
}

func (s *Server) handleSRem(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	for i := 2; i < len(cmd.Args); i++ {
		args = append(args, cmd.Args[i])
	}
	s.applyCommand(conn, store.Command{Op: "SREM", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleSPop(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var args [][]byte
	if len(cmd.Args) == 3 {
		count, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil || count < 0 {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		args = append(args, cmd.Args[2])
	}
	s.applyCommand(conn, store.Command{Op: "SPOP", Key: string(cmd.Args[1]), Args: args})
}

func (s *Server) handleSRandMember(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	var count *int
	if len(cmd.Args) == 3 {
		parsed, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError("ERR value is not an integer")
			return
		}
		count = &parsed
	}
	members, err := s.fsm.SRandMember(string(cmd.Args[1]), count)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if count == nil {
		if len(members) == 0 {
			conn.WriteNull()
			return
		}
		conn.WriteBulkString(members[0])
		return
	}
	conn.WriteArray(len(members))
	for _, member := range members {
		conn.WriteBulkString(member)
	}
}

func (s *Server) handleSMIsMember(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	conn.WriteArray(len(cmd.Args) - 2)
	for i := 2; i < len(cmd.Args); i++ {
		isMember, err := s.fsm.SIsMember(string(cmd.Args[1]), string(cmd.Args[i]))
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(isMember)
	}
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
	case []string:
		conn.WriteArray(len(v))
		for _, item := range v {
			conn.WriteBulkString(item)
		}
	default:
		conn.WriteString("OK")
	}
}
