// Package aof implements append-only-file persistence. Every mutating
// command gets logged in RESP wire format, so the log is really just a
// replayable command stream — plus a background rewrite that snapshots
// the current dataset into a minimal log and swaps it in atomically.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/kthallam167/bolt/internal/resp"
)

// FsyncPolicy controls how aggressively the AOF is flushed to durable
// storage, trading durability against write latency.
type FsyncPolicy int

const (
	// FsyncAlways syncs after every append. Safest, and slow — see the
	// benchmarks in the README for just how slow fsync(2) can be.
	FsyncAlways FsyncPolicy = iota
	// FsyncEverySec syncs once a second from a background goroutine.
	// Redis's default, and bolt's. Worst case you lose ~1s of writes.
	FsyncEverySec
	// FsyncNo never syncs explicitly; the OS decides when to flush.
	FsyncNo
)

// ParseFsyncPolicy parses "always", "everysec", or "no" (case-sensitive,
// matching Redis's appendfsync config values).
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "always":
		return FsyncAlways, nil
	case "everysec":
		return FsyncEverySec, nil
	case "no":
		return FsyncNo, nil
	default:
		return 0, fmt.Errorf("aof: unknown fsync policy %q (want always|everysec|no)", s)
	}
}

func (p FsyncPolicy) String() string {
	switch p {
	case FsyncAlways:
		return "always"
	case FsyncEverySec:
		return "everysec"
	case FsyncNo:
		return "no"
	default:
		return "unknown"
	}
}

// ErrRewriteInProgress is returned by Rewrite if a rewrite is already
// running.
var ErrRewriteInProgress = errors.New("aof: rewrite already in progress")

// AOF manages a single append-only log file: appending new commands,
// fsyncing per its policy, and compacting the log via Rewrite.
type AOF struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	writer *bufio.Writer
	policy FsyncPolicy

	rewriting  bool
	rewriteBuf [][]byte

	stop chan struct{}
	wg   sync.WaitGroup
}

// Open opens (creating if necessary) the AOF at path for appending, and
// starts the background fsync goroutine if policy is FsyncEverySec.
func Open(path string, policy FsyncPolicy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		path:   path,
		file:   f,
		writer: bufio.NewWriterSize(f, 64*1024),
		policy: policy,
		stop:   make(chan struct{}),
	}
	if policy == FsyncEverySec {
		a.wg.Add(1)
		go a.fsyncLoop()
	}
	return a, nil
}

func (a *AOF) fsyncLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.mu.Lock()
			_ = a.writer.Flush()
			_ = a.file.Sync()
			a.mu.Unlock()
		case <-a.stop:
			return
		}
	}
}

// Append encodes args as a RESP command and writes it to the log, applying
