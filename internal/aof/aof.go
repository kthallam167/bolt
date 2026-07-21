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
	"sync/atomic"
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

	// size tracks the on-disk log size in bytes, updated as commands are
	// appended so the write path can check growth without a stat(2) syscall.
	size atomic.Int64

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
	if fi, err := f.Stat(); err == nil {
		a.size.Store(fi.Size())
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
// the configured fsync policy. If a rewrite is in progress, the command is
// also buffered in memory so Rewrite can replay it into the new log before
// swapping files — this is what makes rewrite safe under concurrent writes.
func (a *AOF) Append(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.appendLocked(args)
}

// appendLocked is the body of Append; the caller must already hold a.mu. It
// exists so AppendAtomic can hold the lock across a caller-supplied mutation
// and the append together.
func (a *AOF) appendLocked(args []string) error {
	buf := resp.EncodeCommand(args)
	if a.rewriting {
		cp := make([]byte, len(buf))
		copy(cp, buf)
		a.rewriteBuf = append(a.rewriteBuf, cp)
	}
	if _, err := a.writer.Write(buf); err != nil {
		return err
	}
	a.size.Add(int64(len(buf)))
	if a.policy == FsyncAlways {
		if err := a.writer.Flush(); err != nil {
			return err
		}
		return a.file.Sync()
	}
	// For everysec/no we still flush to the OS buffer so readers of the
	// file (e.g. a rewrite dump running concurrently) see up to date bytes;
	// only the fsync-to-disk call is deferred/skipped.
	return a.writer.Flush()
}

// AppendAtomic runs mutate while holding the AOF's lock, then appends the
// command mutate returns (when logIt is true) under that same lock. Performing
// the store mutation and its log record as one locked step keeps their order
// identical for concurrent writers, so the in-memory state and the replayable
// log cannot diverge for racing writes to the same key.
func (a *AOF) AppendAtomic(mutate func() (args []string, logIt bool)) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	args, logIt := mutate()
	if !logIt {
		return nil
	}
	return a.appendLocked(args)
}

// LoggedSize returns the log's current size in bytes, tracked in memory as
// commands are appended so the hot write path never needs a stat(2) syscall to
// decide whether an automatic rewrite is due.
func (a *AOF) LoggedSize() int64 {
	return a.size.Load()
}

// Size returns the current size in bytes of the on-disk AOF file.
func (a *AOF) Size() (int64, error) {
	fi, err := os.Stat(a.path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Rewrite compacts the log. dump gets a fresh buffered writer and should
// write whatever minimal set of commands reconstructs the current dataset.
// Anything appended while dump is running gets captured and replayed onto
// the new file before it replaces the old one — see the comments below,
// this is the one part of the package worth reading carefully. Returns
// the size of the new file in bytes.
func (a *AOF) Rewrite(dump func(w *bufio.Writer) error) (int64, error) {
	a.mu.Lock()
	if a.rewriting {
		a.mu.Unlock()
		return 0, ErrRewriteInProgress
	}
	a.rewriting = true
	a.rewriteBuf = a.rewriteBuf[:0]
	a.mu.Unlock()

	tmpPath := a.path + ".rewrite.tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		a.mu.Lock()
		a.rewriting = false
		a.mu.Unlock()
		return 0, err
	}
	w := bufio.NewWriterSize(f, 64*1024)

	// This walk is the slow part on a big store, so it runs lock-free.
	// Live Appends keep landing on the old file in the meantime and get
	// mirrored into rewriteBuf below, so nothing gets dropped.
	dumpErr := dump(w)

	// Everything past this point happens under the lock, on purpose: a
	// write during the rewrite either lands in rewriteBuf before we read
	// it, or it blocks here and lands straight in the new file once we've
	// swapped a.file and cleared a.rewriting. No gap either way.
	a.mu.Lock()
	var finalErr error
	if dumpErr != nil {
		finalErr = dumpErr
	} else {
		for _, cmd := range a.rewriteBuf {
			if _, err := w.Write(cmd); err != nil {
				finalErr = err
				break
			}
		}
		if finalErr == nil {
			finalErr = w.Flush()
		}
		if finalErr == nil {
			finalErr = f.Sync()
		}
	}
	f.Close()

	if finalErr != nil {
		os.Remove(tmpPath)
		a.rewriting = false
		a.rewriteBuf = nil
		a.mu.Unlock()
		return 0, finalErr
	}

	if err := os.Rename(tmpPath, a.path); err != nil {
		a.rewriting = false
		a.rewriteBuf = nil
		a.mu.Unlock()
		return 0, err
	}

	newFile, err := os.OpenFile(a.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		a.rewriting = false
		a.rewriteBuf = nil
		a.mu.Unlock()
		return 0, err
	}

	oldFile := a.file
	a.file = newFile
	a.writer = bufio.NewWriterSize(newFile, 64*1024)
	a.rewriting = false
	a.rewriteBuf = nil

	// Reset the in-memory size counter to the swapped-in file's size while
	// still holding the lock, so a resuming Append can't race the store and
	// leave the counter stale.
	fi, statErr := newFile.Stat()
	if statErr == nil {
		a.size.Store(fi.Size())
	}
	a.mu.Unlock()

	oldFile.Close()

	if statErr != nil {
		return 0, statErr
	}
	return fi.Size(), nil
}

// Close flushes, fsyncs, and closes the log, stopping the background fsync
// goroutine if one is running.
func (a *AOF) Close() error {
	close(a.stop)
	a.wg.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.writer.Flush(); err != nil {
		return err
	}
	if err := a.file.Sync(); err != nil {
		return err
	}
	return a.file.Close()
}

// Replay feeds every command in the AOF at path through apply, in order.
// A missing file just means an empty log — not an error. If the last
// command is truncated (process died mid-write), replay stops there
// instead of failing the whole load; same idea as Redis's aof-load-truncated.
func Replay(path string, apply func(args []string) error) (int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	n := 0
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			// Any other framing error at end of file is treated as a
			// truncated last write and tolerated; mid-file corruption
			// still surfaces to the caller.
			if errors.Is(err, resp.ErrProtocol) {
				break
			}
			return n, err
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
