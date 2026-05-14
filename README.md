# bolt

[![CI](https://github.com/kthallam167/bolt/actions/workflows/ci.yml/badge.svg)](https://github.com/kthallam167/bolt/actions/workflows/ci.yml)
[![Go Reference](https://img.shields.io/badge/go-1.22%2B-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A concurrent, in-memory key-value store written in Go, speaking a
[RESP2](https://redis.io/docs/latest/develop/reference/protocol-spec/)-compatible
wire protocol (works with `redis-cli`/`redis-benchmark` and any Redis
client library) with append-only-file persistence, background compaction,
and command pipelining.

Everything here — the sharded store, the TCP server, the AOF/rewrite
engine, the wire protocol — is implemented from scratch in the standard
library; no third-party dependencies.

## Features

- **Concurrent, sharded store** — keys are hashed across N independently
  locked shards, so operations on different keys never contend on the same
  mutex. `GET`/`SET`/`DEL`/`EXPIRE`/`TTL`/`EXISTS`/`KEYS`/`DBSIZE`, all with
  per-key TTL and lazy + active (sampled, background) expiration.
- **AOF persistence** — every mutation is logged in RESP wire format
  (the log doubles as a replayable command stream). Configurable fsync
  policy (`always` / `everysec` / `no`) trades durability for latency,
  exactly like Redis's `appendfsync`.
- **Background rewrite (compaction)** — `BGREWRITEAOF`, or automatic
  triggering once the log grows past a configurable threshold, snapshots
  the live dataset into a minimal command log and atomically swaps it in.
  Writes that land *while* the snapshot is being taken are buffered and
  replayed into the new file before the swap, so nothing is lost.
- **Sub-second crash recovery** — replay reconstructs a 1M-key dataset in
  under a second (measured, see [Benchmarks](#benchmarks)).
- **RESP2 wire protocol** — real Redis clients, `redis-cli`, and
  `redis-benchmark` all work against bolt unmodified.
- **Pipelining** — the server only flushes its response buffer once a
  client's request buffer is drained, so a pipelined batch of commands
  costs one network round trip instead of one per command.
- **Zero dependencies** — pure Go standard library.

## Quickstart

```sh
git clone https://github.com/kthallam167/bolt.git
cd bolt
make build
./bin/bolt-server                      # listens on :6380, persists to ./bolt.aof
```

In another terminal:

```sh
./bin/bolt-cli
localhost:6380> SET hello world
"OK"
localhost:6380> GET hello
"world"
localhost:6380> EXPIRE hello 100
(integer) 1
localhost:6380> TTL hello
(integer) 100
```

Or use `redis-cli` / `redis-benchmark` directly against it — bolt speaks
the same protocol:

```sh
redis-cli -p 6380 SET foo bar
redis-benchmark -p 6380 -c 50 -n 200000 -t get,set -q
```

## Architecture

```
                     ┌───────────────────────────────────────────┐
                     │                bolt-server                │
                     │                                            │
   TCP clients  ───► │  net.Listener                              │
 (redis-cli, bolt-   │     │  one goroutine per connection         │
  cli, app code)     │     ▼                                       │
                     │  RESP decode ──► dispatch ──► RESP encode   │
                     │                     │                       │
                     │                     ▼                       │
                     │   ┌──────────────────────────────┐          │
                     │   │   sharded store (32 shards)   │          │
                     │   │  shard 0  shard 1  ...  N-1   │          │
                     │   │  RWMutex  RWMutex      RWMutex│          │
                     │   └──────────────┬─────────────────┘        │
                     │                  │ mutating commands        │
                     │                  ▼                          │
                     │            AOF (RESP log)                   │
                     │        append ─┬─ fsync policy               │
                     │                └─ background rewrite/compact │
                     └───────────────────────────────────────────┘
```

- **`internal/resp`** — RESP2 framing: decoding client requests, encoding
  replies, and the same `EncodeCommand` primitive is reused to serialize
  the AOF log, so the log and the wire protocol are literally the same
  format.
- **`internal/store`** — the sharded map + TTL engine. `ForEach` takes
  each shard's read lock only for the duration of that shard's own
  iteration, so a full-dataset scan (used by AOF rewrite) doesn't block
  the whole store, only one shard at a time.
- **`internal/aof`** — append, fsync policy, replay, and rewrite/compaction.
- **`internal/server`** — the TCP frontend: connection handling, command
  dispatch, pipelining, and wiring commands to the store + AOF.
- **`cmd/bolt-server`** — the server binary and flags.
- **`cmd/bolt-cli`** — a minimal interactive RESP client (works against
  bolt or real Redis) for exploring the store without needing `redis-cli`
  installed.
- **`cmd/bolt-bench`** — a `redis-benchmark`-style concurrent load
  generator, used to produce the numbers below.

### Concurrency model

The store is partitioned into a configurable number of shards (rounded up
to a power of two, default 32), each an independent `map[string]entry`
guarded by its own `sync.RWMutex`. A key's shard is chosen by FNV-1a
hashing the key and masking. Two requests for keys in different shards
proceed fully in parallel; only requests for the *same* shard's keys ever
block each other. Each client connection runs in its own goroutine, so the
server scales with Go's runtime scheduler rather than an explicit thread
pool.

### TTL / expiration

Expiration is **lazy** (a `GET`/`EXISTS`/`TTL` on an expired key deletes it
on the spot and reports it as missing) **and active** (a background
goroutine samples a bounded number of keys per shard on a timer, evicting
any that have expired — the same strategy Redis uses so that TTL'd keys
nobody ever reads again don't linger in memory forever).

### AOF durability & the rewrite/compaction race

Every mutating command (`SET`, `DEL`, `EXPIRE`, `FLUSHALL`, ...) is appended
to the log as a RESP-encoded command, with relative TTLs (`SET ... EX 10`)
rewritten to an **absolute** expiry (`SET ... PXAT <unix-ms>`) before
they're logged — so replaying the log at any point in the future still
expires the key at the *correct* wall-clock instant instead of restarting
a 10-second countdown from whenever the process happened to restart.

`BGREWRITEAOF` (manual, or auto-triggered once the log has grown a
configurable percentage past its size after the last rewrite — default
100%, i.e. doubled, matching Redis's default) compacts the log: it walks
the live dataset and writes the minimal set of `SET` commands that
reconstruct it, to a temp file. The tricky part is correctness under
concurrent writes — a write that lands *while* that walk is in progress
must not be lost. bolt handles this by mirroring every `Append` into an
in-memory buffer whenever a rewrite is active; once the (potentially slow,
full-dataset) dump finishes, the buffered commands are replayed into the
new file **before** it atomically replaces the old one via `rename(2)`,
and the AOF's lock is held for that entire finalization window so no write
can slip through the gap between "read the buffer" and "swap the file".
See [`internal/aof/aof.go`](internal/aof/aof.go) and the
`TestRewriteConcurrentWritesNotLost` test in
[`internal/aof/aof_test.go`](internal/aof/aof_test.go).

### Pipelining

`handleConn` only flushes the response buffer once `bufio.Reader.Buffered()
== 0` — i.e. once there's no more data the client has already sent sitting
in the read buffer. A client that pipelines N commands back-to-back (writes
them all, then reads N replies) gets all N replies flushed in a single
`write()` syscall instead of paying a network round trip per command. See
[Benchmarks](#benchmarks) for the measured effect.

## Commands
