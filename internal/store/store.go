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
