# DistLog

> A distributed log search engine being built from scratch in Go. Currently a single-node engine (LSM-tree storage with WAL, MemTable, SSTable, freeze/flush, crash recovery, write backpressure) plus a small SQL dialect (SELECT / WHERE / LIMIT, full-scan executor) reachable over HTTP. Distributed layer, inverted index, and ORDER BY / aggregations are not yet implemented.

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
- **HTTP server** ([`internal/server`](internal/server/)) — `POST /ingest`, `GET /logs/{docID}`, `POST /query`, `GET /healthz`; `ErrBackpressure → 429 + Retry-After`, `ErrClosed → 503`, `ErrEngineFatal → 500`; unknown-field rejection on JSON input; RFC3339-with-nanos timestamp output.
- **Engine scan API** ([`internal/engine`](internal/engine/) `Scan`) — k-way merge over active MemTable + frozen MemTables + SSTables via `container/heap`, ascending DocID with source-priority tie-break for duplicates that briefly exist during flush. Powers the query executor.
- **SQL parser** ([`internal/query`](internal/query/)) — `participle`-based grammar for `SELECT [cols|*] FROM logs [WHERE expr] [LIMIT N]`. WHERE expression supports `=` / `!=`, `AND` / `OR`, parentheses, and these fields: `ts`, `doc_id`, `tenant_id`, `source`, `message`, `fields.<key>`. Timestamp literals are RFC3339; comparison normalizes to unix-nanos.
- **Query executor** ([`internal/query`](internal/query/)) — scan + filter + project + LIMIT. Aborts the underlying engine scan once LIMIT is reached (internal sentinel error, translated back to a clean return).
- **Timestamp predicate pushdown** ([`internal/query`](internal/query/) Plan) — extracts `ts =`, `ts <`, `ts <=`, `ts >`, `ts >=` bounds from the top-level AND chain of WHERE and passes them to `engine.ScanWithOptions`. SSTables whose meta-block min/max timestamp falls outside the requested window are pruned without opening their data blocks. Conservative-by-default: any OR at the top level disables pushdown so we never drop a valid SSTable. In the bundled benchmark (20k records, 23 SSTables, query targeting ~5% of the time range), pushdown prunes 19/23 SSTables and reduces query latency from 44 ms to 7.7 ms — see [Benchmarks](#benchmarks).
- **Server binary** ([`cmd/distlog`](cmd/distlog/)) — loads config, opens engine, serves HTTP, handles SIGINT/SIGTERM for graceful shutdown.

What that adds up to: a durable, recoverable, single-process log store you can `curl` ingest into and run small SQL queries against.

## What's not yet built

The following are stubs (empty directory) or absent:

| Component                              | Status            |
|----------------------------------------|-------------------|
| Inverted index / full-text `MATCH`     | Not started       |
| `ORDER BY`, `GROUP BY`, aggregations   | Not started       |
| gRPC API                               | Not started       |
| Client SDK                             | Not started       |
| Raft replication                       | Not started       |
| Sharding & routing                     | Not started       |
| Ingester / querier binaries            | Not started       |
| Compaction (size-tiered)               | Not started       |
| Docker Compose / Helm chart            | Not started       |
| Prometheus / OTel integration          | Not started       |

The current SQL executor is a full scan — every query reads every SSTable + frozen + active MemTable. Fine for the data sizes this repo currently exercises; not a production query engine.

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

# Run a SQL query.
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT message, source FROM logs WHERE fields.level = '\''error'\'' LIMIT 10"}'
# {"columns":["message","source"],"rows":[{"doc_id":1,"values":{"message":"connection timeout to upstream","source":"api-1"}}]}
```

Stop with Ctrl-C; the server drains in-flight requests and closes the engine cleanly.

### HTTP contract

| Endpoint                | Notes                                                                                          |
|-------------------------|------------------------------------------------------------------------------------------------|
| `POST /ingest`          | Body: single `LogRecord` JSON. `ts` accepts RFC3339 string or unix-nanos integer; omitted → now. Returns `{"doc_id": N}`. Unknown fields rejected. |
| `GET /logs/{docID}`     | Returns the record JSON, or 404. `ts` always emitted as RFC3339 with nanos in UTC.            |
| `POST /query`           | Body: `{"sql": "..."}`. Returns `{"columns": [...], "rows": [{"doc_id": N, "values": {...}}]}`. SQL parse errors and bad field/literal references → 400. |
| `GET /healthz`          | Returns engine stats; `200` if healthy, `503` if engine is in fatal or closed state.          |

**SQL dialect** (full-scan executor; no optimizer):

```sql
SELECT * | <col> [, <col>]* FROM logs
  [WHERE <expr>]
  [LIMIT <n>]

<expr> := <field> ('=' | '!=') <literal>
        | <field> ('<' | '<=' | '>' | '>=') <literal>   -- numeric fields only
        | <expr> 'AND' <expr>
        | <expr> 'OR' <expr>
        | '(' <expr> ')'

<field> := ts | doc_id | tenant_id | source | message | fields.<key>
```

Timestamp literals are RFC3339 strings (`'2026-05-26T12:00:00Z'`); comparisons against `ts` normalize both sides to unix-nanos. `fields.<key>` reads from the per-record `Fields` map.

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
│   ├── query/          # SQL parser + executor (DONE for scan dialect)
│   ├── index/          # empty — planned (inverted index for MATCH pushdown)
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

## Benchmarks

Run with `go test -bench=. -benchmem -run=^$ ./internal/{engine,query}/`. Numbers below are from an Apple M4 (10-core, macOS, gp3-equivalent local SSD); reproduce yours with the same command. Treat these as relative ballpark figures, not vendor-comparable claims.

**Engine** ([`internal/engine/bench_test.go`](internal/engine/bench_test.go), `MemTableSizeLimit=256 KiB` to force flushes mid-run):

| Bench                          | Result                                       |
|--------------------------------|----------------------------------------------|
| `Write`                        | ~7.2 µs/op (~140k writes/sec, single-thread) |
| `Get` — active MemTable hit    | ~14 ns/op                                    |
| `Get` — SSTable hit            | ~93 µs/op (read block + binary search + JSON decode) |
| `Scan` — full table, 20k recs  | ~446k records/sec                            |

**Timestamp predicate pushdown** ([`internal/query/bench_test.go`](internal/query/bench_test.go), 20k records seeded into 20 disjoint ts buckets producing 23 SSTables; query targets a ~5% window):

| Bench                           | Latency   | SSTables scanned | SSTables pruned |
|---------------------------------|-----------|------------------|-----------------|
| `TsPushdown/with_pushdown`      | ~7.7 ms   | 4 / 23           | 19              |
| `TsPushdown/without_pushdown`   | ~44 ms    | 23 / 23          | 0               |

A ~5.8× speedup is consistent with «pruning eliminates 83% of block reads», not a hot-loop micro-optimization. The without-pushdown variant uses an `OR` in WHERE to suppress the planner (per the conservative pushdown rule); both variants return the same rows.

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
- [x] **Stage G** — `engine.Scan` k-way merge + minimal SQL parser/executor + `POST /query`
- [x] **Stage H** — `ts` predicate pushdown (planner extracts AND-chain bounds; engine prunes SSTables by meta-block min/max timestamp) + benchmark suite
- [ ] **Next** — likely inverted index for `MATCH`, or `ORDER BY ts DESC LIMIT N` with TopK min-heap

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
