// Package store implements an in-memory key-value store sharded across
// several independently-locked maps so that concurrent readers and writers
// touching different keys never contend on the same mutex.
package store

import (
	"hash/fnv"
	"path"
	"sync"
	"time"
)

// DefaultShardCount is used when a non-positive shard count is requested.
const DefaultShardCount = 32

type entry struct {
	value     []byte
	expiresAt int64 // unix nanoseconds; 0 means no expiry
}

func (e entry) expired(nowNano int64) bool {
	return e.expiresAt != 0 && e.expiresAt <= nowNano
}

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a sharded, concurrency-safe key-value map with per-key TTL
// support. Each shard is guarded by its own RWMutex, so operations on keys
// that hash to different shards proceed in parallel without blocking each
// other.
type Store struct {
	shards []*shard
	mask   uint32
}

// New creates a Store with the given number of shards, rounded up to the
// next power of two so key-to-shard hashing can use a fast bitmask instead
// of a modulo. A non-positive count falls back to DefaultShardCount.
func New(shardCount int) *Store {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}
	n := 1
	for n < shardCount {
		n <<= 1
	}
	shards := make([]*shard, n)
	for i := range shards {
		shards[i] = &shard{data: make(map[string]entry)}
	}
	return &Store{shards: shards, mask: uint32(n - 1)}
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return s.shards[h.Sum32()&s.mask]
}

// Set stores value under key. If ttl > 0 the key expires ttl from now; ttl
// <= 0 means no expiry.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	s.SetAbsExpiry(key, value, exp)
}

// SetAbsExpiry is Set but with an absolute unix-nanosecond expiry instead
// of a relative TTL (0 = no expiry). Both live PXAT requests and AOF
// replay go through this, which is why a key still expires at the right
// wall-clock instant no matter how long ago the command was logged.
func (s *Store) SetAbsExpiry(key string, value []byte, expiresAtUnixNano int64) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.data[key] = entry{value: value, expiresAt: expiresAtUnixNano}
	sh.mu.Unlock()
}

// Get returns the value for key and whether it was found (and not expired).
// Expired keys are lazily removed on access.
func (s *Store) Get(key string) ([]byte, bool) {
	sh := s.shardFor(key)
	now := time.Now().UnixNano()

	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if !e.expired(now) {
		return e.value, true
	}

	sh.mu.Lock()
	if e2, ok := sh.data[key]; ok && e2.expired(time.Now().UnixNano()) {
		delete(sh.data, key)
	}
	sh.mu.Unlock()
	return nil, false
}

// Exists reports whether key is present and not expired.
func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// Del removes the given keys and returns how many were actually present.
func (s *Store) Del(keys ...string) int {
	n := 0
	now := time.Now().UnixNano()
	for _, key := range keys {
		sh := s.shardFor(key)
		sh.mu.Lock()
		if e, ok := sh.data[key]; ok {
			delete(sh.data, key)
			if !e.expired(now) {
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// Expire sets key to expire after ttl from now. It returns false if the key
// does not exist (or is already expired).
func (s *Store) Expire(key string, ttl time.Duration) bool {
	return s.ExpireAt(key, time.Now().Add(ttl).UnixNano())
}

// ExpireAt sets key's absolute expiry to expiresAtUnixNano. It returns
// false if the key does not exist (or is already expired).
func (s *Store) ExpireAt(key string, expiresAtUnixNano int64) bool {
	sh := s.shardFor(key)
	now := time.Now().UnixNano()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		return false
	}
	e.expiresAt = expiresAtUnixNano
	sh.data[key] = e
	return true
}

// TTL reports the remaining time-to-live for key. exists is false if the
// key is absent or expired. hasExpiry is false if the key exists but never
// expires, in which case ttl is meaningless.
func (s *Store) TTL(key string) (ttl time.Duration, exists bool, hasExpiry bool) {
	sh := s.shardFor(key)
	now := time.Now().UnixNano()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		return 0, false, false
	}
	if e.expiresAt == 0 {
		return 0, true, false
	}
	return time.Duration(e.expiresAt - now), true, true
}

// FlushAll removes every key from the store.
func (s *Store) FlushAll() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]entry)
		sh.mu.Unlock()
	}
}

// Len returns the total number of keys in the store, including any that
// have expired but have not yet been swept. This matches Redis's DBSIZE
// semantics.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// Keys returns all non-expired keys matching the given shell glob pattern
// (see path.Match). It is O(N) and, like Redis's KEYS, intended for
// debugging rather than hot-path use.
func (s *Store) Keys(pattern string) []string {
	now := time.Now().UnixNano()
	var out []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			if ok, _ := path.Match(pattern, k); ok {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// ForEach calls fn for every non-expired key, one shard at a time — it only
// holds a shard's read lock while iterating that shard, so a full scan
// (AOF rewrite uses this) doesn't stall the rest of the store. Stops early
// if fn returns false.
func (s *Store) ForEach(fn func(key string, value []byte, expiresAtUnixNano int64) bool) {
	now := time.Now().UnixNano()
	for _, sh := range s.shards {
		sh.mu.RLock()
		cont := true
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			if !fn(k, e.value, e.expiresAt) {
				cont = false
				break
			}
		}
		sh.mu.RUnlock()
		if !cont {
			return
		}
	}
}

// activeExpireSampleSize is how many keys are sampled per shard on each
// active-expiry pass.
const activeExpireSampleSize = 20

// ActiveExpireCycle samples a bounded number of keys per shard and evicts
// whichever have expired, returning the count. This is the other half of
// TTL handling — Get/Exists/TTL expire keys lazily on access, but a key
// nobody ever reads again would otherwise just sit there forever. Same
// sampling idea Redis uses: cheap, bounded, good enough.
func (s *Store) ActiveExpireCycle() int {
	now := time.Now().UnixNano()
	evicted := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		i := 0
		for k, e := range sh.data {
			if i >= activeExpireSampleSize {
				break
			}
			i++
			if e.expired(now) {
				delete(sh.data, k)
				evicted++
			}
		}
		sh.mu.Unlock()
	}
	return evicted
}

// RunActiveExpiry runs ActiveExpireCycle on interval until stop is closed.
// It is intended to be launched as a background goroutine by the server.
func (s *Store) RunActiveExpiry(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.ActiveExpireCycle()
		case <-stop:
			return
		}
	}
}

// ShardCount returns the number of shards the store was created with.
func (s *Store) ShardCount() int {
	return len(s.shards)
}
