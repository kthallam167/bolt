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

// cmdSet implements SET key value [EX seconds | PX milliseconds | PXAT unix-ms].
// PXAT isn't advertised in the README (Redis keeps it low-profile too) —
// it's how bolt rewrites relative TTLs into absolute ones before they hit
// the AOF, so replay re-expires at the right instant instead of restarting
// the clock.
func (s *Server) cmdSet(w *bufio.Writer, args []string, persist bool) {
	if len(args) < 3 {
		resp.WriteError(w, "ERR wrong number of arguments for 'set' command")
		return
	}
	key, value := args[1], args[2]

	var ttl time.Duration
	var absExpiry int64
	for i := 3; i < len(args); {
		opt := normalizeCmd(args[i])
		if i+1 >= len(args) {
			resp.WriteError(w, "ERR syntax error")
			return
		}
		n, err := strconv.ParseInt(args[i+1], 10, 64)
		if err != nil {
			resp.WriteError(w, "ERR value is not an integer or out of range")
			return
		}
		switch opt {
		case "EX":
			ttl = time.Duration(n) * time.Second
		case "PX":
			ttl = time.Duration(n) * time.Millisecond
		case "PXAT":
			absExpiry = n * int64(time.Millisecond)
		default:
			resp.WriteError(w, "ERR syntax error")
			return
		}
		i += 2
	}

	var aofArgs []string
	switch {
	case absExpiry != 0:
		s.cfg.Store.SetAbsExpiry(key, []byte(value), absExpiry)
		aofArgs = args
	case ttl > 0:
		abs := time.Now().Add(ttl).UnixNano()
		s.cfg.Store.SetAbsExpiry(key, []byte(value), abs)
		aofArgs = []string{"SET", key, value, "PXAT", strconv.FormatInt(abs/int64(time.Millisecond), 10)}
	default:
		s.cfg.Store.Set(key, []byte(value), 0)
		aofArgs = []string{"SET", key, value}
	}
	if persist {
		s.appendAOF(aofArgs)
	}
	resp.WriteSimpleString(w, "OK")
}

func (s *Server) cmdGet(w *bufio.Writer, args []string) {
	if len(args) != 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'get' command")
		return
	}
	val, ok := s.cfg.Store.Get(args[1])
	if !ok {
		resp.WriteNilBulk(w)
		return
	}
	resp.WriteBulkString(w, string(val))
}

func (s *Server) cmdDel(w *bufio.Writer, args []string, persist bool) {
	if len(args) < 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'del' command")
		return
	}
	n := s.cfg.Store.Del(args[1:]...)
	if persist && n > 0 {
		s.appendAOF(args)
	}
	resp.WriteInteger(w, int64(n))
}

func (s *Server) cmdExists(w *bufio.Writer, args []string) {
	if len(args) < 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'exists' command")
		return
	}
	n := 0
	for _, k := range args[1:] {
		if s.cfg.Store.Exists(k) {
			n++
		}
	}
	resp.WriteInteger(w, int64(n))
}

func (s *Server) cmdExpire(w *bufio.Writer, args []string, persist bool) {
	if len(args) != 3 {
		resp.WriteError(w, "ERR wrong number of arguments for 'expire' command")
		return
	}
	secs, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		resp.WriteError(w, "ERR value is not an integer or out of range")
		return
	}
	abs := time.Now().Add(time.Duration(secs) * time.Second).UnixNano()
	if !s.cfg.Store.ExpireAt(args[1], abs) {
		resp.WriteInteger(w, 0)
		return
	}
	if persist {
		s.appendAOF([]string{"PEXPIREAT", args[1], strconv.FormatInt(abs/int64(time.Millisecond), 10)})
	}
	resp.WriteInteger(w, 1)
}

func (s *Server) cmdPExpireAt(w *bufio.Writer, args []string, persist bool) {
	if len(args) != 3 {
		resp.WriteError(w, "ERR wrong number of arguments for 'pexpireat' command")
		return
	}
	ms, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		resp.WriteError(w, "ERR value is not an integer or out of range")
		return
	}
	abs := ms * int64(time.Millisecond)
	if !s.cfg.Store.ExpireAt(args[1], abs) {
		resp.WriteInteger(w, 0)
		return
	}
	if persist {
		s.appendAOF(args)
	}
	resp.WriteInteger(w, 1)
}

func (s *Server) cmdTTL(w *bufio.Writer, args []string, unit time.Duration) {
	if len(args) != 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'ttl' command")
		return
	}
	ttl, exists, hasExpiry := s.cfg.Store.TTL(args[1])
	switch {
	case !exists:
		resp.WriteInteger(w, -2)
	case !hasExpiry:
		resp.WriteInteger(w, -1)
	default:
		// Round up so a key with e.g. 900ms left reports TTL 1, not 0.
		n := (ttl + unit - 1) / unit
		if n < 0 {
			n = 0
		}
		resp.WriteInteger(w, int64(n))
	}
}

func (s *Server) cmdKeys(w *bufio.Writer, args []string) {
	if len(args) != 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'keys' command")
		return
	}
	resp.WriteArray(w, s.cfg.Store.Keys(args[1]))
}

func (s *Server) cmdFlushAll(w *bufio.Writer, persist bool) {
	s.cfg.Store.FlushAll()
	if persist {
		s.appendAOF([]string{"FLUSHALL"})
	}
	resp.WriteSimpleString(w, "OK")
}

func (s *Server) cmdBgRewriteAOF(w *bufio.Writer) {
	s.mu.Lock()
	a := s.aof
	s.mu.Unlock()
	if a == nil {
		resp.WriteError(w, "ERR persistence is not enabled")
		return
	}
	go s.triggerRewrite()
	resp.WriteSimpleString(w, "Background append only file rewriting started")
}

func (s *Server) cmdInfo(w *bufio.Writer) {
	s.mu.Lock()
	a := s.aof
	s.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(s.uptime().Seconds()))
	fmt.Fprintf(&b, "go_version:%s\r\n", runtime.Version())
	fmt.Fprintf(&b, "goroutines:%d\r\n", runtime.NumGoroutine())
	fmt.Fprintf(&b, "\r\n# Clients\r\n")
	fmt.Fprintf(&b, "connected_clients:%d\r\n", atomic.LoadInt64(&s.connCount))
	fmt.Fprintf(&b, "\r\n# Stats\r\n")
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", atomic.LoadInt64(&s.commandCount))
	fmt.Fprintf(&b, "\r\n# Keyspace\r\n")
	fmt.Fprintf(&b, "db0:keys=%d,shards=%d\r\n", s.cfg.Store.Len(), s.cfg.Store.ShardCount())
	fmt.Fprintf(&b, "\r\n# Persistence\r\n")
	if a == nil {
		fmt.Fprintf(&b, "aof_enabled:0\r\n")
	} else {
		size, _ := a.Size()
		fmt.Fprintf(&b, "aof_enabled:1\r\n")
		fmt.Fprintf(&b, "aof_current_size:%d\r\n", size)
		fmt.Fprintf(&b, "aof_base_size:%d\r\n", atomic.LoadInt64(&s.aofBaseSize))
	}
	resp.WriteBulkString(w, b.String())
}

// dumpAOF writes the minimal set of SET commands that reconstruct st's
// current contents, used as the AOF rewrite/compaction target. Keys with a
// TTL are dumped with an absolute PXAT expiry so the rewritten log still
// expires them at the correct wall-clock instant.
func dumpAOF(w *bufio.Writer, st *store.Store) error {
	var outerErr error
	st.ForEach(func(key string, value []byte, expiresAtUnixNano int64) bool {
		var args []string
		if expiresAtUnixNano != 0 {
			args = []string{"SET", key, string(value), "PXAT", strconv.FormatInt(expiresAtUnixNano/int64(time.Millisecond), 10)}
		} else {
			args = []string{"SET", key, string(value)}
		}
		if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
			outerErr = err
			return false
		}
		return true
	})
	return outerErr
}
