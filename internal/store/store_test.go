package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New(4)
	s.Set("foo", []byte("bar"), 0)
	v, ok := s.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("got %q, %v", v, ok)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected missing key to be absent")
	}
}

func TestExpiry(t *testing.T) {
	s := New(4)
	s.Set("foo", []byte("bar"), 10*time.Millisecond)

	if _, ok := s.Get("foo"); !ok {
		t.Fatal("expected key to exist before TTL elapses")
	}

	time.Sleep(30 * time.Millisecond)

	if _, ok := s.Get("foo"); ok {
		t.Fatal("expected key to be expired")
	}
}

func TestDel(t *testing.T) {
	s := New(4)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)

	n := s.Del("a", "b", "missing")
	if n != 2 {
		t.Fatalf("expected 2 deletions, got %d", n)
	}
	if s.Exists("a") || s.Exists("b") {
		t.Fatal("expected keys to be gone")
	}
}

func TestExpireAndTTL(t *testing.T) {
	s := New(4)
	s.Set("foo", []byte("bar"), 0)

	if ok := s.Expire("missing", time.Second); ok {
		t.Fatal("expected Expire on missing key to fail")
	}

	if !s.Expire("foo", time.Minute) {
		t.Fatal("expected Expire to succeed")
	}
	ttl, exists, hasExpiry := s.TTL("foo")
	if !exists || !hasExpiry || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("unexpected TTL state: ttl=%v exists=%v hasExpiry=%v", ttl, exists, hasExpiry)
	}

	s.Set("bare", []byte("v"), 0)
	_, exists, hasExpiry = s.TTL("bare")
	if !exists || hasExpiry {
		t.Fatal("expected key with no TTL to report hasExpiry=false")
	}

	_, exists, _ = s.TTL("nope")
	if exists {
		t.Fatal("expected missing key to report exists=false")
	}
}

func TestFlushAllAndLen(t *testing.T) {
	s := New(4)
	for i := 0; i < 10; i++ {
		s.Set(fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	if s.Len() != 10 {
		t.Fatalf("expected 10 keys, got %d", s.Len())
	}
	s.FlushAll()
	if s.Len() != 0 {
		t.Fatalf("expected 0 keys after FlushAll, got %d", s.Len())
	}
}

func TestKeysGlob(t *testing.T) {
	s := New(4)
	s.Set("user:1", []byte("a"), 0)
	s.Set("user:2", []byte("b"), 0)
	s.Set("session:1", []byte("c"), 0)

	keys := s.Keys("user:*")
	if len(keys) != 2 {
		t.Fatalf("expected 2 matches, got %v", keys)
	}
}

func TestActiveExpireCycleEvictsExpiredKeys(t *testing.T) {
	s := New(1) // single shard so the whole set fits in one sample pass
	for i := 0; i < 5; i++ {
		s.Set(fmt.Sprintf("k%d", i), []byte("v"), time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)

	evicted := s.ActiveExpireCycle()
	if evicted != 5 {
		t.Fatalf("expected 5 evictions, got %d", evicted)
	}
}

// TestConcurrentAccess exercises Set/Get/Del/Expire from many goroutines at
// once; run with -race to catch any shard-locking mistakes.
func TestConcurrentAccess(t *testing.T) {
	s := New(16)
	const goroutines = 64
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key:%d:%d", g, i%10)
				switch i % 4 {
				case 0:
					s.Set(key, []byte("v"), 0)
				case 1:
					s.Get(key)
				case 2:
					s.Del(key)
				case 3:
					s.Expire(key, time.Minute)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestForEachSkipsExpired(t *testing.T) {
	s := New(4)
	s.Set("live", []byte("v"), 0)
	s.Set("dead", []byte("v"), time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	seen := map[string]bool{}
	s.ForEach(func(key string, value []byte, expiresAt int64) bool {
		seen[key] = true
		return true
	})
	if !seen["live"] || seen["dead"] {
		t.Fatalf("ForEach should only see live keys, got %v", seen)
	}
}

func TestShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	s := New(10)
	if s.ShardCount() != 16 {
		t.Fatalf("expected 16 shards, got %d", s.ShardCount())
	}
}
