package server

import (
	"encoding/json"
	"log"

	"github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
	"github.com/winterman/badis/store"
)

type Server struct {
	addr         string
	mux          *redcon.ServeMux
	redconServer *redcon.Server
	fsm          *store.FSM
}

func NewServer(addr string, fsm *store.FSM) *Server {
	mux := redcon.NewServeMux()
	s := &Server{addr: addr, mux: mux, fsm: fsm}

	mux.HandleFunc("ping", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("PONG")
	})
	mux.HandleFunc("set", s.handleSet)
	mux.HandleFunc("get", s.handleGet)
	return s
}

func (s *Server) handleSet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	// Bypass raft for now, simulate apply
	c := store.Command{Op: "SET", Key: string(cmd.Args[1]), Args: [][]byte{cmd.Args[2]}}
	data, _ := json.Marshal(c)
	s.fsm.Apply(&raft.Log{Data: data})
	conn.WriteString("OK")
}

func (s *Server) handleGet(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	val, err := s.fsm.Get(string(cmd.Args[1]))
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

func (s *Server) Start() error {
	log.Printf("Starting server on %s", s.addr)
	s.redconServer = redcon.NewServer(s.addr, s.mux.ServeRESP,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)
	return s.redconServer.ListenAndServe()
}

func (s *Server) Stop() {
	if s.redconServer != nil {
		_ = s.redconServer.Close()
	}
}
