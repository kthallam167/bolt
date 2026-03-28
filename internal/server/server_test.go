package server

import (
	"bufio"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kthallam167/bolt/internal/aof"
	"github.com/kthallam167/bolt/internal/resp"
	"github.com/kthallam167/bolt/internal/store"
)

func startTestServer(t *testing.T, withAOF bool) (addr string, srv *Server, aofPath string) {
	t.Helper()
	st := store.New(4)
	srv = New(Config{Addr: "127.0.0.1:0", Store: st, AOFRewritePercentage: 100, AOFRewriteMinSize: 1 << 20})

	if withAOF {
		aofPath = filepath.Join(t.TempDir(), "test.aof")
		a, err := aof.Open(aofPath, aof.FsyncAlways)
		if err != nil {
			t.Fatal(err)
		}
		srv.SetAOF(a, 0)
		t.Cleanup(func() { a.Close() })
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	srv.listener = ln
	srv.mu.Unlock()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			srv.connWG.Add(1)
			go srv.handleConn(conn)
		}
	}()

	t.Cleanup(func() {
		srv.Shutdown()
	})

	return ln.Addr().String(), srv, aofPath
}

func dial(t *testing.T, addr string) (*bufio.Reader, *bufio.Writer, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return bufio.NewReader(conn), bufio.NewWriter(conn), conn
}

func send(t *testing.T, r *bufio.Reader, w *bufio.Writer, args ...string) interface{} {
	t.Helper()
	if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	reply, err := resp.ReadReply(r)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return reply
}

func TestPingSetGetDel(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	if got := send(t, r, w, "PING"); got != "PONG" {
		t.Fatalf("PING: got %v", got)
	}
	if got := send(t, r, w, "SET", "foo", "bar"); got != "OK" {
		t.Fatalf("SET: got %v", got)
	}
	if got := send(t, r, w, "GET", "foo"); got != "bar" {
		t.Fatalf("GET: got %v", got)
	}
	if got := send(t, r, w, "GET", "missing"); got != nil {
		t.Fatalf("GET missing: got %v", got)
	}
	if got := send(t, r, w, "DEL", "foo"); got != int64(1) {
		t.Fatalf("DEL: got %v", got)
	}
	if got := send(t, r, w, "EXISTS", "foo"); got != int64(0) {
		t.Fatalf("EXISTS after DEL: got %v", got)
	}
}

func TestExpireAndTTLOverWire(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	send(t, r, w, "SET", "foo", "bar")
	if got := send(t, r, w, "EXPIRE", "foo", "100"); got != int64(1) {
		t.Fatalf("EXPIRE: got %v", got)
	}
	ttl := send(t, r, w, "TTL", "foo")
	n, ok := ttl.(int64)
	if !ok || n <= 0 || n > 100 {
		t.Fatalf("TTL: got %v", ttl)
	}
	if got := send(t, r, w, "TTL", "missing"); got != int64(-2) {
		t.Fatalf("TTL missing key: got %v", got)
	}
	send(t, r, w, "SET", "bare", "v")
	if got := send(t, r, w, "TTL", "bare"); got != int64(-1) {
		t.Fatalf("TTL no-expiry key: got %v", got)
	}
}

func TestSetWithPXExpiresQuickly(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	send(t, r, w, "SET", "foo", "bar", "PX", "20")
	time.Sleep(60 * time.Millisecond)
	if got := send(t, r, w, "GET", "foo"); got != nil {
		t.Fatalf("expected expired key to be gone, got %v", got)
	}
}

// TestPipelining sends several commands back-to-back without reading
// between writes, then reads all replies — this is exactly the pattern
// handleConn's r.Buffered()==0 flush strategy is meant to support.
func TestPipelining(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	for i := 0; i < 50; i++ {
		if _, err := w.Write(resp.EncodeCommand([]string{"SET", "k", "v"})); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		reply, err := resp.ReadReply(r)
		if err != nil {
			t.Fatal(err)
		}
		if reply != "OK" {
			t.Fatalf("reply %d: got %v", i, reply)
		}
	}
}

func TestFlushAllAndDBSize(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	send(t, r, w, "SET", "a", "1")
	send(t, r, w, "SET", "b", "2")
	if got := send(t, r, w, "DBSIZE"); got != int64(2) {
		t.Fatalf("DBSIZE: got %v", got)
	}
	send(t, r, w, "FLUSHALL")
	if got := send(t, r, w, "DBSIZE"); got != int64(0) {
		t.Fatalf("DBSIZE after FLUSHALL: got %v", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	addr, _, _ := startTestServer(t, false)
	r, w, _ := dial(t, addr)

	if _, err := w.Write(resp.EncodeCommand([]string{"BOGUS"})); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	_, err := resp.ReadReply(r)
	if err == nil {
		t.Fatal("expected an error reply for an unknown command")
	}
}

// TestWritesArePersistedAndReplayable verifies the server actually wires
// live traffic into the AOF: writes made over the wire must be replayable
// into a fresh store afterward.
func TestWritesArePersistedAndReplayable(t *testing.T) {
	addr, _, aofPath := startTestServer(t, true)
	r, w, _ := dial(t, addr)

	send(t, r, w, "SET", "a", "1")
	send(t, r, w, "SET", "b", "2")
	send(t, r, w, "DEL", "a")

	// give the fsync-always path a moment; not strictly needed but keeps
	// this robust if the policy changes later.
	time.Sleep(10 * time.Millisecond)

	st2 := store.New(4)
	srv2 := New(Config{Store: st2})
	if _, err := aof.Replay(aofPath, srv2.ApplyReplay); err != nil {
		t.Fatal(err)
	}
	if st2.Exists("a") {
		t.Fatal("expected 'a' to be deleted after replay")
	}
	v, ok := st2.Get("b")
	if !ok || string(v) != "2" {
		t.Fatalf("expected b=2 after replay, got %q, %v", v, ok)
	}
}
