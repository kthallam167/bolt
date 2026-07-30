# Project Status (`status.md`)

**Repository**: `bolt` (Concurrent In-Memory Key-Value Store in Go)  
**Last Updated**: 2026-07-30  
**Overall System Health**: PASSING / STABLE  

---

## 1. System Overview

`bolt` is a high-performance, concurrent, in-memory key-value store written in pure Go (standard library only). It implements the Redis RESP2 wire protocol, append-only file (AOF) persistence, background compaction/rewrite, sharded concurrency, and command pipelining.

### Components

| Package / Module | Path | Description | Status |
|---|---|---|---|
| **RESP2 Protocol** | `internal/resp` | Wire protocol parser, reply encoders, command decoders, CRLF validation | PASSING |
| **Sharded Store** | `internal/store` | Sharded `map[string]entry` with per-key TTL, lazy & active background sweep | PASSING |
| **AOF Persistence** | `internal/aof` | Append-only log, fsync policies (`always`, `everysec`, `no`), replay, background compaction | PASSING |
| **TCP Server** | `internal/server` | Listener, connection loop, command dispatch, pipelining flushes | PASSING |
| **Server Binary** | `cmd/bolt-server` | Entrypoint CLI flags, graceful signal handling (`SIGINT`, `SIGTERM`) | PASSING |
| **CLI Shell** | `cmd/bolt-cli` | Interactive shell and single-command execution mode | PASSING |
| **Load Generator** | `cmd/bolt-bench` | Concurrent benchmark tool with pipelining & latency percentiles | PASSING |

---

## 2. Recent Audit & Bug Fixes Log

### Bug Fix 1: RESP2 Trailing CRLF Framing Validation (`internal/resp/resp.go`)
- **Issue**: `ReadCommand` and `ReadReply` read $N+2$ bytes for bulk strings but did not verify that the last two bytes were `\r\n`. Corrupted stream bytes could be accepted silently.
- **Fix**: Added explicit validation checks (`buf[length] == '\r'` and `buf[length+1] == '\n'`) returning `ErrProtocol` on framing errors.
- **Test**: Added `TestReadCommandMissingTrailingCRLF` in `internal/resp/resp_test.go`.

### Bug Fix 2: `SET` Non-Positive TTL Validation (`internal/server/commands.go`)
- **Issue**: Executing `SET key val EX 0` or `SET key val EX -5` (non-positive integer) caused `ttl > 0` to evaluate to false and fell through to the default case, setting a permanent key with no TTL instead of rejecting the invalid expire time.
- **Fix**: Added validation for non-positive values ($n \le 0$) on `EX`, `PX`, and `PXAT` options in `cmdSet`, returning `ERR invalid expire time in 'set' command`.
- **Test**: Added `TestSetNonPositiveTTL` in `internal/server/server_test.go`.

### Bug Fix 3: Lazy Expiration Cleanup in `ExpireAt` (`internal/store/store.go`)
- **Issue**: When `ExpireAt` encountered an already-expired key in the shard map, it returned `false` without removing the dead entry from `sh.data`.
- **Fix**: Added lazy `delete(sh.data, key)` cleanup inside `ExpireAt` when an expired entry is detected.

### Bug Fix 4: AOF Double-Close Safety (`internal/aof/aof.go`)
- **Issue**: Invoking `Close()` multiple times on an `AOF` handle caused a `panic: close of closed channel` on `close(a.stop)`.
- **Fix**: Added `closed bool` protection inside `Close()` to safely guard channel closure and file operations.
- **Test**: Added `TestAOFDoubleCloseSafe` in `internal/aof/aof_test.go`.

---

## 3. Verification & Test Metrics

- **`go vet ./...`**: 0 warnings / errors.
- **Build Status (`make build`)**:
  - `bin/bolt-server`: Compiled cleanly
  - `bin/bolt-cli`: Compiled cleanly
  - `bin/bolt-bench`: Compiled cleanly
- **Unit & Integration Tests**:
  - `internal/resp`: 8 PASS / 0 FAIL
  - `internal/store`: 10 PASS / 0 FAIL
  - `internal/aof`: 7 PASS / 0 FAIL
  - `internal/server`: 8 PASS / 0 FAIL

---

## 4. Maintenance Guidelines

- When adding new RESP commands to `internal/server/commands.go`, add corresponding unit/integration tests in `internal/server/server_test.go`.
- Run `make test` or `go test -v ./...` to verify all test suites before submitting changes.
