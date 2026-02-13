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

