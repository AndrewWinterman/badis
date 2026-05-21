package server

import (
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/winterman/badis/store"
)

func (s *Server) handleEval(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for 'eval' command")
		return
	}
	// Format: EVAL script numkeys key1 key2 ... arg1 arg2 ...
	numKeysStr := string(cmd.Args[2])
	numKeys, err := strconv.Atoi(numKeysStr)
	if err != nil || numKeys < 0 {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}
	if len(cmd.Args) < 3+numKeys {
		conn.WriteError("ERR Number of keys can't be greater than number of args")
		return
	}

	// Pack the EVAL command for store
	// The store Command will be Op="EVAL", Key="", Args=[script, numkeys, key1, key2, ..., arg1, arg2, ...]
	s.applyCommand(conn, store.Command{
		Op:   "EVAL",
		Args: cmd.Args[1:],
	})
}

func (s *Server) handleEvalSha(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for 'evalsha' command")
		return
	}
	numKeysStr := string(cmd.Args[2])
	numKeys, err := strconv.Atoi(numKeysStr)
	if err != nil || numKeys < 0 {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}
	if len(cmd.Args) < 3+numKeys {
		conn.WriteError("ERR Number of keys can't be greater than number of args")
		return
	}

	s.applyCommand(conn, store.Command{
		Op:   "EVALSHA",
		Args: cmd.Args[1:],
	})
}

func (s *Server) handleScript(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'script' command")
		return
	}

	subcmd := strings.ToUpper(string(cmd.Args[1]))
	switch subcmd {
	case "LOAD":
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'script|load' command")
			return
		}
		s.applyCommand(conn, store.Command{
			Op:   "SCRIPT_LOAD",
			Args: [][]byte{cmd.Args[2]},
		})
	case "EXISTS":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for 'script|exists' command")
			return
		}
		s.applyCommand(conn, store.Command{
			Op:   "SCRIPT_EXISTS",
			Args: cmd.Args[2:],
		})
	case "FLUSH":
		s.applyCommand(conn, store.Command{
			Op: "SCRIPT_FLUSH",
		})
	default:
		conn.WriteError("ERR Unknown SCRIPT subcommand or wrong number of arguments")
	}
}
