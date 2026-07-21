// Package server implements the TCP frontend: a RESP-speaking listener that
// dispatches client commands against the store and, when enabled, persists
// mutations to the AOF.
package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kthallam167/bolt/internal/aof"
	"github.com/kthallam167/bolt/internal/resp"
	"github.com/kthallam167/bolt/internal/store"
)

// Config bundles everything the server needs to run.
type Config struct {
	Addr   string
	Store  *store.Store
	Logger *log.Logger

	// AOFRewritePercentage triggers a background rewrite once the AOF has
	// grown this many percent beyond its size after the last rewrite (0
	// disables automatic rewrite; BGREWRITEAOF still works).
	AOFRewritePercentage int
	// AOFRewriteMinSize is the minimum AOF size in bytes before automatic
	// rewrite can trigger, so a small/empty log doesn't churn rewrites.
	AOFRewriteMinSize int64
}

// Server is a running (or not-yet-started) bolt TCP server.
type Server struct {
	cfg Config

	mu       sync.Mutex
	listener net.Listener
	aof      *aof.AOF

	aofBaseSize  int64 // atomic: AOF size right after the last rewrite/load
	startTime    time.Time
	connCount    int64 // atomic: currently connected clients
	commandCount int64 // atomic: total commands processed

	// replayW is a single reusable discard writer for AOF replay: replay is
	// single-threaded, so one writer serves every replayed command instead of
	// allocating a throwaway per command.
	replayW *bufio.Writer

	connWG sync.WaitGroup
	quit   chan struct{}
}

// New creates a Server. Call SetAOF before ListenAndServe if persistence is
// enabled, then ListenAndServe to start accepting connections.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &Server{
		cfg:       cfg,
		startTime: time.Now(),
		quit:      make(chan struct{}),
		replayW:   bufio.NewWriter(io.Discard),
	}
}

// SetAOF attaches the AOF used for persisting future writes and records
// baseSize (typically the file's size right after replay) as the baseline
// against which automatic rewrite growth is measured.
func (s *Server) SetAOF(a *aof.AOF, baseSize int64) {
	s.mu.Lock()
	s.aof = a
	s.mu.Unlock()
	atomic.StoreInt64(&s.aofBaseSize, baseSize)
}

// ApplyReplay applies a single command from the AOF during startup replay:
// it mutates the store exactly as a live command would, but never re-writes
// to the AOF (that would duplicate the log) and discards any reply. Replay is
// single-threaded, so the reply goes to a single reused discard writer.
func (s *Server) ApplyReplay(args []string) error {
	s.dispatch(s.replayW, args, false)
	return nil
}

// ListenAndServe binds cfg.Addr and serves connections until Shutdown is
// called.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.cfg.Logger.Printf("bolt listening on %s", ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				return err
			}
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			// Disable Nagle's algorithm: GET/SET payloads are small and
			// latency-sensitive, so we always want them on the wire
			// immediately rather than batched with the next write.
			_ = tc.SetNoDelay(true)
		}
		s.connWG.Add(1)
		go s.handleConn(conn)
	}
}

// Shutdown stops accepting new connections, closes the listener, and waits
// for in-flight connections to finish.
func (s *Server) Shutdown() {
	close(s.quit)
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		ln.Close()
	}
	s.connWG.Wait()
}

func (s *Server) handleConn(conn net.Conn) {
	atomic.AddInt64(&s.connCount, 1)
	defer atomic.AddInt64(&s.connCount, -1)
	defer s.connWG.Done()
	defer conn.Close()

	r := bufio.NewReaderSize(conn, 64*1024)
	w := bufio.NewWriterSize(conn, 64*1024)

	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		atomic.AddInt64(&s.commandCount, 1)
		s.dispatch(w, args, true)

		// Only flush once the client has nothing more queued up to read
		// right now. A pipelined batch gets buffered and flushed in one
		// write() instead of paying a round trip per command — see the
		// pipelining numbers in the README, it's a big multiplier.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) uptime() time.Duration {
	return time.Since(s.startTime)
}

func (s *Server) maybeTriggerRewrite() {
	s.mu.Lock()
	a := s.aof
	s.mu.Unlock()
	if a == nil || s.cfg.AOFRewritePercentage <= 0 {
		return
	}
	size := a.LoggedSize()
	if size < s.cfg.AOFRewriteMinSize {
		return
	}
	base := atomic.LoadInt64(&s.aofBaseSize)
	growth := size - base
	if base <= 0 || growth*100 >= base*int64(s.cfg.AOFRewritePercentage) {
		go s.triggerRewrite()
	}
}

func (s *Server) triggerRewrite() {
	s.mu.Lock()
	a := s.aof
	s.mu.Unlock()
	if a == nil {
		return
	}
	newSize, err := a.Rewrite(func(w *bufio.Writer) error {
		return dumpAOF(w, s.cfg.Store)
	})
	if err != nil {
		if !isRewriteInProgress(err) {
			s.cfg.Logger.Printf("aof rewrite failed: %v", err)
		}
		return
	}
	atomic.StoreInt64(&s.aofBaseSize, newSize)
	s.cfg.Logger.Printf("aof rewrite complete: %d bytes", newSize)
}

// applyWrite performs a store mutation and, when persistence is enabled, its
// AOF append as a single atomic step, so the in-memory state and the log can't
// be reordered relative to another concurrent writer. mutate makes the store
// change and returns the command to persist plus whether it should be logged
// at all. It returns an error only when the append itself fails, letting the
// caller decline to acknowledge a write it could not durably log.
//
// Note: the store is mutated before the append inside the locked step, so a
// failed append still leaves the mutation in memory — the reply becomes an
// error rather than a silent OK, but a full rollback would require capturing
// prior state and is out of scope here.
func (s *Server) applyWrite(persist bool, mutate func() (logArgs []string, logIt bool)) error {
	s.mu.Lock()
	a := s.aof
	s.mu.Unlock()

	if !persist || a == nil {
		mutate()
		return nil
	}
	if err := a.AppendAtomic(mutate); err != nil {
		s.cfg.Logger.Printf("aof append failed: %v", err)
		return err
	}
	s.maybeTriggerRewrite()
	return nil
}

// normalizeCmd upper-cases a command name for dispatch matching.
func normalizeCmd(s string) string { return strings.ToUpper(s) }
