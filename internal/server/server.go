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
// to the AOF (that would duplicate the log) and discards any reply.
func (s *Server) ApplyReplay(args []string) error {
	w := bufio.NewWriter(io.Discard)
	s.dispatch(w, args, false)
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
func (s *Server) dispatch(w *bufio.Writer, args []string, persist bool) {}
