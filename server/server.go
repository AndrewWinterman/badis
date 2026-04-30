package server

import (
	"log"

	"github.com/tidwall/redcon"
)

type Server struct {
	addr string
	mux  *redcon.ServeMux
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
	return redcon.ListenAndServe(s.addr, s.mux.ServeRESP,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)
}

func (s *Server) Stop() {
	// redcon doesn't have a clean stop in this simple mode, we'll refine later
}
