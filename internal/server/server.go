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

