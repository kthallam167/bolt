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
