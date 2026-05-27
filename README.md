# DistLog

> A distributed log search engine being built from scratch in Go. Currently a single-node storage engine (LSM-tree with WAL, MemTable, SSTable, freeze/flush pipeline, crash recovery, and write-side backpressure). Distributed layer, query engine, and client SDK are not yet implemented.

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## What this is

DistLog is a personal learning project exploring how a search engine optimized **specifically for logs** (append-heavy, time-ordered, time-range queries) can be simpler and lighter than general-purpose stores like Elasticsearch. The goal is to implement core distributed-systems and database internals from first principles in production-quality Go.

The README reflects what currently exists in the repo. Anything not listed under "What's working" should be assumed unimplemented.

## What's working

The single-node storage engine is operational and tested. Specifically:

- **Write-ahead log** ([`internal/wal`](internal/wal/)) — segmented WAL with CRC-protected records, sealed-segment replay, atomic segment rotation, and a `Manager` that owns the active writer and the sealed-segment list.
- **MemTable** ([`internal/memtable`](internal/memtable/)) — in-memory sorted store with `Freeze` semantics; frozen MemTables reject further writes.
- **SSTable v1** ([`internal/sstable`](internal/sstable/)) — immutable on-disk format with data blocks (CRC-trailed), fixed-size index entries (binary-searchable), 88-byte meta block, and a 48-byte footer with magic at the tail; atomic write via tmp-file + rename + parent-dir fsync. Reader uses `ReadAt`. Format details in [`docs/DECISIONS.md`](docs/DECISIONS.md) ADR-002.
- **Engine** ([`internal/engine`](internal/engine/)) — ties WAL + MemTable + SSTable together: write path (WAL → MemTable, with at-least-once semantics), read path (active → frozen → SSTables, newest-first), background freeze coordinator, background flush worker with bounded retry + fatal state, and atomic SSTable rotation.
- **Crash recovery** — on `Open`, replays WAL segments (skipping records already covered by SSTables), quarantines corrupted SSTables to `dataDir/corrupted/` instead of failing or silently dropping them (ADR-007), and rebuilds the docID allocator monotonically.
- **Backpressure** (Stage E, ADR-009) — under sustained flush stall, `Engine.Write` blocks once the active MemTable reaches `MemTableHardLimit` (default 2× `MemTableSizeLimit`) and the frozen queue is full. Block is bounded by `ctx`; on expiry the call returns `errors.Join(ErrBackpressure, ctx.Err())`. Engine stays healthy and resumes serving once flush drains.

What that adds up to: a durable, recoverable, single-process key-value-by-DocID store with the LSM machinery for the rest of the system to plug into.

## What's not yet built

The following are stubs (empty directory) or absent. Anything in a previous version of this README claiming otherwise was aspirational, not factual:

| Component                       | Status            |
|---------------------------------|-------------------|
| Inverted index                  | Not started       |
| SQL parser / query planner      | Not started       |
| HTTP / gRPC server              | Not started       |
| Client SDK                      | Not started       |
| Raft replication                | Not started       |
| Sharding & routing              | Not started       |
| Ingester / querier binaries     | Not started       |
| Compaction (size-tiered)        | Not started       |
| Docker Compose / Helm chart     | Not started       |
| Prometheus / OTel integration   | Not started       |

No HTTP endpoint exists. No `curl` example will work today.

## Layout (as of today)

```
distlog/
├── cmd/distlog/        # main: placeholder binary, prints "not implemented yet"
├── internal/
│   ├── wal/            # write-ahead log + Manager (DONE)
│   ├── memtable/       # in-memory sorted store with freeze (DONE)
│   ├── sstable/        # immutable on-disk format + reader/writer (DONE)
│   ├── engine/         # WAL + MemTable + SSTable orchestration (DONE through Stage E)
│   ├── types/          # LogRecord, DocID allocator (DONE)
│   ├── iter/           # iterator helpers (DONE)
│   ├── config/         # empty — planned
│   ├── index/          # empty — planned
│   ├── query/          # empty — planned
│   ├── raft/           # empty — planned
│   └── server/         # empty — planned
├── pkg/client/         # empty — planned
├── api/proto/          # empty — planned
├── test/
│   ├── integration/    # empty — planned (`-tags=integration`)
│   └── chaos/          # empty — planned (`-tags=chaos`)
└── docs/
    └── DECISIONS.md    # ADR-001..009 (current decisions)
```

## Build & test

Requirements: Go 1.22+.

```bash
go build ./...           # compile everything
go test ./...            # run all unit tests
go test -race ./...      # data-race detector
```

The `Makefile` defines `lint`, `test-int`, `test-chaos`, `cluster-up`, etc., but `lint` requires `golangci-lint` and the cluster targets reference files that do not exist yet. The two commands above are the reliable ones today.

## Design documents

- [`docs/DECISIONS.md`](docs/DECISIONS.md) — ADRs covering byte order, SSTable format v1, fsync semantics on macOS vs Linux, SSTable reader strategy, corrupted-SSTable quarantine, fork-self crash test deferral, and Stage E backpressure (ADR-009).

Other design docs (`ARCHITECTURE.md`, `STORAGE.md`, `INDEX.md`, `DISTRIBUTED.md`) referenced in earlier drafts do not exist yet.

## Stage roadmap

Each stage is a coherent commit-set, not a calendar week.

- [x] **Stage A** — segmented WAL with `Manager` interface
- [x] **Stage B** — SSTable v1 format (reader + writer)
- [x] **Stage C** — engine write path + freeze/flush pipeline
- [x] **Stage D** — startup recovery (WAL replay, SSTable rediscovery, quarantine, fatal-state path)
- [x] **Stage E** — write-side backpressure under sustained flush stall (ADR-009)
- [ ] **Next** — TBD; likely either compaction or the start of the inverted index

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
