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

| Command | Description |
|---|---|
| `PING [msg]` | Liveness check |
| `ECHO msg` | Echo |
| `SET key value [EX seconds \| PX ms]` | Set a key, optionally with a TTL |
| `GET key` | Get a key |
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key [key ...]` | Count how many of the given keys exist |
| `EXPIRE key seconds` | Set a TTL on an existing key |
| `PEXPIREAT key unix-ms` | Set an absolute expiry (also used internally by AOF replay) |
| `TTL key` / `PTTL key` | Remaining TTL in seconds / milliseconds (`-1` no TTL, `-2` no key) |
| `KEYS pattern` | List keys matching a glob pattern (`path.Match` syntax) — O(N), debugging only |
| `DBSIZE` | Number of keys |
| `FLUSHALL` | Remove all keys |
| `BGREWRITEAOF` | Manually trigger AOF compaction |
| `INFO` | Server/stats/persistence info |

## Configuration

`bolt-server` flags:

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:6380` | TCP listen address |
| `-aof` | `bolt.aof` | AOF file path (empty disables persistence) |
| `-aof-fsync` | `everysec` | `always` \| `everysec` \| `no` |
| `-shards` | `32` | Store shard count (rounded up to a power of two) |
| `-aof-rewrite-percentage` | `100` | Auto-rewrite once the log grows this % past its post-rewrite size (`0` disables) |
| `-aof-rewrite-min-size` | `1048576` | Minimum AOF size (bytes) before auto-rewrite can trigger |
| `-active-expiry-interval` | `100ms` | Interval between background TTL sweeps |

## Benchmarks

Measured with `bolt-bench` (included, `cmd/bolt-bench`) and cross-checked
against real `redis-benchmark`/`redis-server` on the **same machine**:
Apple M4 Pro, macOS, Go 1.22, loopback TCP. Numbers are what this specific
laptop produces — the point isn't the absolute figures (any machine/network
will differ) but the shape of the results and that they're honestly
reproducible with the tools in this repo, not copy-pasted claims.

**Throughput, 50 concurrent connections, no pipelining (one round trip per
op):**

| Server | Workload | Throughput | p50 latency |
|---|---|---|---|
| bolt | mixed GET/SET, 64B values | ~53,000 ops/sec | ~0.93 ms |
| redis-benchmark → bolt | GET/SET, 64B values | ~59,000 ops/sec | ~0.5 ms |
| redis-benchmark → real Redis 7 | GET/SET, 64B values | ~61,000 ops/sec | ~0.6 ms |

bolt matches stock Redis within noise on identical hardware; at 50
un-pipelined connections both are latency-bound by loopback round-trip
queueing on this machine, not CPU.

**Pipelining, 50 connections, mixed workload:**

| Pipeline depth | Throughput | Speedup vs. depth 1 |
|---|---|---|
| 1 (sequential) | ~53,600 ops/sec | 1.0x |
| 16 | ~340,000 ops/sec | 6.3x |
| 32 | ~372,000 ops/sec | 6.9x |
| 64 | ~397,000 ops/sec | **7.4x** |

Batching requests amortizes the network round trip, not per-command CPU
work — throughput climbs steeply until the connection is CPU/syscall bound
rather than latency bound.

**Bulk load throughput** (`SET`, pipeline 32, 50 connections,
`-aof-fsync everysec`): **231,000 ops/sec** sustained while persisting
1,000,000 keys to the AOF.

**Crash recovery** (replay a 99MB / 1,000,000-command AOF on startup):

```
loading AOF from load.aof
replayed 1000000 commands (1000000 keys) in 729.955875ms
```

Sub-second, as claimed — reproduce with:

```sh
./bin/bolt-bench -c 50 -n 1000000 -mode set -pipeline 32   # populate
kill <server pid>                                           # clean shutdown flushes+fsyncs
./bin/bolt-server                                            # watch the replay log line
```

**AOF compaction**: writing the same key 300 times plus 500 distinct keys
produced a 29,676-byte log; `BGREWRITEAOF` compacted it to 19,319 bytes
(35% smaller) with `DBSIZE` and every key's value unchanged afterward.

**Durability tradeoff** (`-aof-fsync always`, i.e. `fsync(2)` on *every*
write): ~244 ops/sec on this machine — macOS `fsync` costs roughly 40ms
per call here. This is the whole point of the fsync policy knob: `always`
buys the strongest durability (lose at most the in-flight write on crash)
at a real, measured cost; `everysec` (the default) loses at most ~1s of
writes and pays none of that per-request tax.

Reproduce any of these:

```sh
make build
./bin/bolt-server &
./bin/bolt-bench -c 50 -n 500000 -mode mixed -pipeline 1
./bin/bolt-bench -c 50 -n 3000000 -mode mixed -pipeline 64
```

## Testing

```sh
make race     # go test -race -count=1 ./...
```

Covers: RESP encode/decode round trips and malformed-input handling; the
sharded store under concurrent access (run with `-race`); AOF append/replay
round trips, truncated-tail tolerance, and — the trickiest part — a test
that fires concurrent writes *during* a rewrite's dump phase and asserts
none of them are lost; and full TCP integration tests (`PING`/`SET`/`GET`/
`DEL`/`EXPIRE`/`TTL`, pipelining, and that live writes over the wire are
actually replayable from the AOF afterward).

## Project layout

```
cmd/
  bolt-server/   server binary
  bolt-cli/      interactive RESP client
  bolt-bench/    concurrent load generator
internal/
  resp/          RESP2 protocol encode/decode
  store/         sharded, concurrent, TTL-aware key-value map
  aof/           append-only-file persistence + rewrite/compaction
  server/        TCP server, command dispatch, pipelining
```

## License

[MIT](LICENSE)
