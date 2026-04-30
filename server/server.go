package server

import (
	"log"

	"github.com/tidwall/redcon"
)

type Server struct {
	addr         string
	mux          *redcon.ServeMux
	redconServer *redcon.Server
}

func NewServer(addr string) *Server {
	mux := redcon.NewServeMux()
	mux.HandleFunc("ping", func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteString("PONG")
	})
	return &Server{addr: addr, mux: mux}
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
		s.redconServer.Close()
	}
}
