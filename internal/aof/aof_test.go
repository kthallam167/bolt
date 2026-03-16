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
