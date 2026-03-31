package server

import (
	"bufio"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kthallam167/bolt/internal/aof"
	"github.com/kthallam167/bolt/internal/resp"
	"github.com/kthallam167/bolt/internal/store"
)

func isRewriteInProgress(err error) bool {
	return errors.Is(err, aof.ErrRewriteInProgress)
}

// dispatch executes one command and writes its RESP reply to w. When
// persist is true (live client traffic) mutating commands are also
// appended to the AOF; replay (persist=false) only ever mutates the store.
func (s *Server) dispatch(w *bufio.Writer, args []string, persist bool) {
	cmd := normalizeCmd(args[0])
	switch cmd {
	case "PING":
		s.cmdPing(w, args)
	case "ECHO":
		s.cmdEcho(w, args)
	case "SET":
		s.cmdSet(w, args, persist)
	case "GET":
		s.cmdGet(w, args)
	case "DEL":
		s.cmdDel(w, args, persist)
	case "EXISTS":
		s.cmdExists(w, args)
	case "EXPIRE":
		s.cmdExpire(w, args, persist)
	case "PEXPIREAT":
		s.cmdPExpireAt(w, args, persist)
	case "TTL":
		s.cmdTTL(w, args, time.Second)
	case "PTTL":
		s.cmdTTL(w, args, time.Millisecond)
	case "KEYS":
		s.cmdKeys(w, args)
	case "DBSIZE":
		resp.WriteInteger(w, int64(s.cfg.Store.Len()))
	case "FLUSHALL":
		s.cmdFlushAll(w, persist)
	case "BGREWRITEAOF":
		s.cmdBgRewriteAOF(w)
	case "INFO":
		s.cmdInfo(w)
	case "COMMAND":
		// redis-cli probes with `COMMAND DOCS` on connect; an empty array
		// is a valid, harmless reply that keeps it happy.
		resp.WriteArray(w, nil)
	default:
		resp.WriteError(w, fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
}

func (s *Server) cmdPing(w *bufio.Writer, args []string) {
	switch len(args) {
	case 1:
		resp.WriteSimpleString(w, "PONG")
	case 2:
		resp.WriteBulkString(w, args[1])
	default:
		resp.WriteError(w, "ERR wrong number of arguments for 'ping' command")
	}
}

func (s *Server) cmdEcho(w *bufio.Writer, args []string) {
	if len(args) != 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'echo' command")
		return
	}
	resp.WriteBulkString(w, args[1])
}

}
