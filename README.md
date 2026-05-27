# DistLog

> A distributed log search engine being built from scratch in Go. Currently a single-node storage engine (LSM-tree with WAL, MemTable, SSTable, freeze/flush pipeline, crash recovery, backpressure) fronted by a minimal HTTP server. Distributed layer, query engine, and inverted index are not yet implemented.

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## What this is

DistLog is a personal learning project exploring how a search engine optimized **specifically for logs** (append-heavy, time-ordered, time-range queries) can be simpler and lighter than general-purpose stores like Elasticsearch. The goal is to implement core distributed-systems and database internals from first principles in production-quality Go.

The README reflects what currently exists in the repo. Anything not listed under "What's working" should be assumed unimplemented.

## What's working

The single-node service is operational and tested end-to-end via HTTP. Specifically:

- **Write-ahead log** ([`internal/wal`](internal/wal/)) — segmented WAL with CRC-protected records, sealed-segment replay, atomic segment rotation, and a `Manager` that owns the active writer and the sealed-segment list.
- **MemTable** ([`internal/memtable`](internal/memtable/)) — in-memory sorted store with `Freeze` semantics; frozen MemTables reject further writes.
- **SSTable v1** ([`internal/sstable`](internal/sstable/)) — immutable on-disk format with data blocks (CRC-trailed), fixed-size index entries (binary-searchable), 88-byte meta block, and a 48-byte footer with magic at the tail; atomic write via tmp-file + rename + parent-dir fsync. Reader uses `ReadAt`. Format details in [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-002.
- **Engine** ([`internal/engine`](internal/engine/)) — ties WAL + MemTable + SSTable together: write path (WAL → MemTable, with at-least-once semantics), read path (active → frozen → SSTables, newest-first), background freeze coordinator, background flush worker with bounded retry + fatal state, and atomic SSTable rotation.
- **Crash recovery** — on `Open`, replays WAL segments (skipping records already covered by SSTables), quarantines corrupted SSTables to `dataDir/corrupted/` instead of failing or silently dropping them (ADR-007), and rebuilds the docID allocator monotonically.
- **Backpressure** (ADR-009) — under sustained flush stall, `Engine.Write` blocks once the active MemTable reaches `MemTableHardLimit` (default 2× `MemTableSizeLimit`) and the frozen queue is full. Block is bounded by `ctx`; on expiry the call returns `errors.Join(ErrBackpressure, ctx.Err())`. Engine stays healthy and resumes serving once flush drains.
- **YAML config** ([`internal/config`](internal/config/)) — single typed loader; unknown fields fail loudly; defaults live in code, not YAML. See [`configs/dev.yaml`](configs/dev.yaml).
- **HTTP server** ([`internal/server`](internal/server/)) — three endpoints (`POST /ingest`, `GET /logs/{docID}`, `GET /healthz`) with `ErrBackpressure → 429 + Retry-After`, `ErrClosed → 503`, `ErrEngineFatal → 500`, unknown-field rejection on JSON input, RFC3339-with-nanos timestamp output.
- **Server binary** ([`cmd/distlog`](cmd/distlog/)) — loads config, opens engine, serves HTTP, handles SIGINT/SIGTERM for graceful shutdown.

What that adds up to: a durable, recoverable, single-process log store you can `curl` into. Storage internals are real; querying is still point-lookup-by-DocID only.

## What's not yet built

The following are stubs (empty directory) or absent:

| Component                       | Status            |
|---------------------------------|-------------------|
| Inverted index                  | Not started       |
| SQL parser / query planner      | Not started       |
| gRPC API                        | Not started       |
| Client SDK                      | Not started       |
| Raft replication                | Not started       |
| Sharding & routing              | Not started       |
| Ingester / querier binaries     | Not started       |
| Compaction (size-tiered)        | Not started       |
| Docker Compose / Helm chart     | Not started       |
| Prometheus / OTel integration   | Not started       |

There is no full-text search and no SQL. `WHERE message MATCH '...'` does not work; only `GET /logs/{docID}` by primary key.

## Quick start

```bash
go build -o ./bin/distlog ./cmd/distlog
mkdir -p ./data
./bin/distlog --config configs/dev.yaml
# distlog: listening on :8080, data dir ./data
```

In another terminal:

```bash
# Ingest a log line.
curl -s -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "ts": "2026-05-26T12:00:00Z",
    "tenant_id": "t1",
    "source": "api-1",
    "message": "connection timeout to upstream",
    "fields": {"level": "error"}
  }'
# {"doc_id":1}

# Fetch it back.
curl -s http://localhost:8080/logs/1
# {"doc_id":1,"ts":"2026-05-26T12:00:00Z","tenant_id":"t1",...}

# Engine stats.
curl -s http://localhost:8080/healthz
# {"status":"ok","active_memtable_size":109,"frozen_memtable_count":0,...}
```

Stop with Ctrl-C; the server drains in-flight requests and closes the engine cleanly.

### HTTP contract

| Endpoint                | Notes                                                                                          |
|-------------------------|------------------------------------------------------------------------------------------------|
| `POST /ingest`          | Body: single `LogRecord` JSON. `ts` accepts RFC3339 string or unix-nanos integer; omitted → now. Returns `{"doc_id": N}`. Unknown fields rejected. |
| `GET /logs/{docID}`     | Returns the record JSON, or 404. `ts` always emitted as RFC3339 with nanos in UTC.            |
| `GET /healthz`          | Returns engine stats; `200` if healthy, `503` if engine is in fatal or closed state.          |

Error mapping (chosen so generic HTTP retry libraries do the right thing):

| Engine error           | HTTP status                | Notes                              |
|------------------------|----------------------------|------------------------------------|
| `ErrBackpressure`      | `429 Too Many Requests`    | + `Retry-After: 1`                 |
| `ErrClosed`            | `503 Service Unavailable`  |                                    |
| `ErrEngineFatal`       | `500 Internal Server Error`|                                    |
| (any other)            | `500 Internal Server Error`| Error message in body              |

## Layout (as of today)

```
distlog/
├── cmd/distlog/        # server binary (loads config, opens engine, serves HTTP)
├── internal/
│   ├── config/         # YAML config loader (DONE)
│   ├── server/         # HTTP handlers (DONE)
│   ├── engine/         # WAL + MemTable + SSTable orchestration (DONE through Stage E)
│   ├── wal/            # write-ahead log + Manager (DONE)
│   ├── memtable/       # in-memory sorted store with freeze (DONE)
│   ├── sstable/        # immutable on-disk format + reader/writer (DONE)
│   ├── types/          # LogRecord, DocID allocator (DONE)
│   ├── iter/           # iterator helpers (DONE)
│   ├── index/          # empty — planned (inverted index)
│   ├── query/          # empty — planned (SQL parser, planner, executor)
│   └── raft/           # empty — planned (replication)
├── pkg/client/         # empty — planned (Go client SDK)
├── api/proto/          # empty — planned (gRPC service definitions)
├── configs/
│   └── dev.yaml        # example local config
├── test/
│   ├── integration/    # empty — planned (`-tags=integration`)
│   └── chaos/          # empty — planned (`-tags=chaos`)
└── docs/
    └── DECISIONS.md    # ADR-001..009
```

## Build & test

Requirements: Go 1.22+.

```bash
go build ./...           # compile everything
go test ./...            # run all unit tests
go test -race ./...      # data-race detector
```

The `Makefile` has additional targets (`lint`, `test-int`, `test-chaos`, `cluster-up`) — `lint` requires `golangci-lint`, and the cluster targets reference files that do not exist yet.

## Design documents

- [`docs/DECISIONS.md`](docs/DECISIONS.md) — ADRs covering byte order, SSTable format v1, fsync semantics on macOS vs Linux, SSTable reader strategy, corrupted-SSTable quarantine, fork-self crash test deferral, and backpressure (ADR-009).

Other design docs (`ARCHITECTURE.md`, `STORAGE.md`, `INDEX.md`, `DISTRIBUTED.md`) referenced in earlier drafts do not exist yet.

## Stage roadmap

Each stage is a coherent commit-set, not a calendar week.

- [x] **Stage A** — segmented WAL with `Manager` interface
- [x] **Stage B** — SSTable v1 format (reader + writer)
- [x] **Stage C** — engine write path + freeze/flush pipeline
- [x] **Stage D** — startup recovery (WAL replay, SSTable rediscovery, quarantine, fatal-state path)
- [x] **Stage E** — write-side backpressure under sustained flush stall (ADR-009)
- [x] **Stage F** — config loader + HTTP server + working `cmd/distlog` binary
- [ ] **Next** — TBD; likely the start of the inverted index, or size-tiered compaction

## Contributing

Personal learning project. Issues and PRs welcome, but expect slow review and an opinionated bar for changes outside the current stage's focus.

## References

- [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [In Search of an Understandable Consensus Algorithm (Raft, Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [Designing Data-Intensive Applications (Kleppmann)](https://dataintensive.net/) — chapters 3, 5, 9
- [Lucene's index format](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/codecs/lucene90/package-summary.html)
- [Quickwit](https://github.com/quickwit-oss/quickwit) — log-optimized search engine in Rust
- LevelDB / RocksDB source for SSTable format precedent

## License

MIT — see [LICENSE](LICENSE).

## Author

[melodyincopenhagen](https://github.com/melodyincopenhagen)
