package aof

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kthallam167/bolt/internal/resp"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	a, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	commands := [][]string{
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"DEL", "a"},
	}
	for _, c := range commands {
		if err := a.Append(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	var replayed [][]string
	n, err := Replay(path, func(args []string) error {
		cp := append([]string(nil), args...)
		replayed = append(replayed, cp)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(commands) {
		t.Fatalf("expected %d replayed commands, got %d", len(commands), n)
	}
	for i, c := range commands {
		if len(replayed[i]) != len(c) {
			t.Fatalf("command %d mismatch: got %v, want %v", i, replayed[i], c)
		}
		for j := range c {
			if replayed[i][j] != c[j] {
				t.Fatalf("command %d mismatch: got %v, want %v", i, replayed[i], c)
			}
		}
	}
}

func TestReplayMissingFileIsNoop(t *testing.T) {
	n, err := Replay(filepath.Join(t.TempDir(), "does-not-exist.aof"), func(args []string) error {
		t.Fatal("apply should not be called")
		return nil
	})
	if err != nil || n != 0 {
		t.Fatalf("expected no-op for missing file, got n=%d err=%v", n, err)
	}
}

func TestReplayTruncatedTailTolerated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.aof")
	full := resp.EncodeCommand([]string{"SET", "a", "1"})
	partial := resp.EncodeCommand([]string{"SET", "b", "2"})
	partial = partial[:len(partial)-3] // chop off the tail, simulating a crash mid-write

	if err := os.WriteFile(path, append(full, partial...), 0644); err != nil {
		t.Fatal(err)
	}

	var applied int
	n, err := Replay(path, func(args []string) error {
		applied++
		return nil
	})
	if err != nil {
		t.Fatalf("expected truncated tail to be tolerated, got err=%v", err)
	}
	if n != 1 || applied != 1 {
		t.Fatalf("expected exactly the one complete command to be applied, got n=%d applied=%d", n, applied)
	}
}

func TestRewriteCompactsAndPreservesLatestState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rewrite.aof")
	a, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// Write the same key many times so the log has a lot of redundancy for
	// the rewrite to compact away.
	for i := 0; i < 100; i++ {
		if err := a.Append([]string{"SET", "counter", fmt.Sprintf("%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	sizeBefore, _ := a.Size()

	newSize, err := a.Rewrite(func(w *bufio.Writer) error {
		_, err := w.Write(resp.EncodeCommand([]string{"SET", "counter", "99"}))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if newSize >= sizeBefore {
		t.Fatalf("expected rewrite to shrink the file: before=%d after=%d", sizeBefore, newSize)
	}

	final := map[string]string{}
	_, err = Replay(path, func(args []string) error {
		if args[0] == "SET" {
			final[args[1]] = args[2]
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if final["counter"] != "99" {
		t.Fatalf("expected counter=99 after replay, got %v", final)
	}
}

// TestRewriteConcurrentWritesNotLost is the key correctness test for the
// rewrite/compaction path: commands appended while a rewrite's dump phase
// is running must still show up after the rewrite finishes and the file is
// swapped, even though the dump snapshot was taken before they arrived.
func TestRewriteConcurrentWritesNotLost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.aof")
	a, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	if err := a.Append([]string{"SET", "base", "0"}); err != nil {
		t.Fatal(err)
	}

	const concurrentWrites = 200
	var wg sync.WaitGroup
	wg.Add(1)

	newSize, err := a.Rewrite(func(w *bufio.Writer) error {
		// Simulate a slow dump by firing concurrent Appends while this
		// callback is running, before it returns.
		go func() {
			defer wg.Done()
			for i := 0; i < concurrentWrites; i++ {
				_ = a.Append([]string{"SET", fmt.Sprintf("live%d", i), "1"})
			}
		}()
		wg.Wait()
		_, err := w.Write(resp.EncodeCommand([]string{"SET", "base", "0"}))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if newSize == 0 {
		t.Fatal("expected non-zero rewritten file size")
	}

	seen := map[string]bool{}
	_, err = Replay(path, func(args []string) error {
		if args[0] == "SET" {
			seen[args[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !seen["base"] {
		t.Fatal("expected base key to survive rewrite")
	}
	for i := 0; i < concurrentWrites; i++ {
		key := fmt.Sprintf("live%d", i)
		if !seen[key] {
			t.Fatalf("write %q made during rewrite dump was lost", key)
		}
	}
}

func TestParseFsyncPolicy(t *testing.T) {
	cases := map[string]FsyncPolicy{
		"always":   FsyncAlways,
		"everysec": FsyncEverySec,
		"no":       FsyncNo,
	}
	for s, want := range cases {
		got, err := ParseFsyncPolicy(s)
		if err != nil || got != want {
			t.Fatalf("ParseFsyncPolicy(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	if _, err := ParseFsyncPolicy("bogus"); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}
