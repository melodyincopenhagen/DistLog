# DistLog

> A distributed log search engine built from scratch in Go. Optimized for high-throughput log ingestion and time-range queries with full-text search.

[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![CI](https://github.com/yuexi/distlog/actions/workflows/ci.yml/badge.svg)](https://github.com/yuexi/distlog/actions)

## Why DistLog?

Elasticsearch is the industry standard for log search, but it's heavy on resources, expensive to operate, and over-engineered for the append-heavy, time-ordered nature of log data. DistLog is a from-scratch exploration of how a search engine optimized **specifically for logs** can be simpler, faster, and lighter.

This is a learning project that implements core distributed systems and database internals concepts in production-quality Go.

## Key Features

- **LSM-tree storage engine** — Append-only writes with WAL, MemTable, SSTable, and leveled compaction
- **Compressed inverted index** — Delta-encoded posting lists with skip-list acceleration
- **SQL-like query language** — `SELECT ... WHERE ... MATCH ... ORDER BY ... LIMIT` with predicate pushdown
- **Distributed by design** — Range-sharded with Raft-replicated shards for fault tolerance
- **Cloud-native** — Docker Compose for local dev, Helm chart for Kubernetes deployment
- **Observable** — Prometheus metrics, structured logging (zap), and OpenTelemetry tracing

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client (HTTP / gRPC)                     │
└──────────────┬──────────────────────────────┬───────────────┘
               │ Ingest                       │ Query
               ▼                              ▼
        ┌─────────────┐              ┌─────────────┐
        │  Ingester   │              │   Querier   │
        │  (stateless)│              │  (stateless)│
        └──────┬──────┘              └──────┬──────┘
               │ route by shard key         │ scatter-gather
               ▼                            ▼
     ┌──────────────────────────────────────────────┐
     │          Data Nodes (Shard + Replica)         │
     │  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
     │  │ Node A  │  │ Node B  │  │ Node C  │       │
     │  │ Shard 1 │  │ Shard 1 │  │ Shard 2 │  ...  │
     │  │ (leader)│  │(follower│  │ (leader)│       │
     │  └─────────┘  └─────────┘  └─────────┘       │
     │           ↑ Raft consensus per shard          │
     └──────────────────────────────────────────────┘
                          │
                          ▼
                ┌──────────────────┐
                │  Metadata Store  │
                │ (shard routing,  │
                │  node health)    │
                └──────────────────┘
```

For detailed design rationale, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose
- Make

### Run a single node locally

```bash
git clone https://github.com/yuexi/distlog.git
cd distlog
make build
./bin/distlog server --config configs/dev.yaml
```

### Run a 3-node cluster with Docker Compose

```bash
docker compose up
```

This brings up:
- 3 data nodes (with Raft replication)
- 1 ingester
- 1 querier
- Prometheus + Grafana (http://localhost:3000)
- Jaeger UI (http://localhost:16686)

### Ingest some logs

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "ts": "2026-04-27T10:00:00Z",
    "service": "api",
    "level": "ERROR",
    "message": "connection timeout to upstream service"
  }'
```

### Query

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT ts, service, message FROM logs WHERE message MATCH '\''timeout'\'' AND service = '\''api'\'' ORDER BY ts DESC LIMIT 10"
  }'
```

## Performance

> Note: Benchmarks will be added at the end of Week 4. Current numbers are targets.

| Metric                 | DistLog (target) | Elasticsearch (baseline) |
|------------------------|------------------|--------------------------|
| Ingest throughput      | 30k docs/sec     | ~15k docs/sec            |
| Query P99 latency      | < 50ms           | ~80ms                    |
| Index size (compressed)| 0.4× raw         | 1.2× raw                 |
| Memory footprint       | ~500MB           | ~2GB (default JVM heap)  |

Hardware: single AWS `t3.xlarge` (4 vCPU, 16GB RAM, gp3 SSD).

See [`docs/benchmark.md`](docs/benchmark.md) for methodology and full results.

## Project Status

🚧 **Active development** — this is a learning project under active construction.

| Component               | Status |
|-------------------------|--------|
| WAL                     | 🚧 In progress |
| MemTable + SSTable      | ⏳ Planned |
| Compaction              | ⏳ Planned |
| Inverted index          | ⏳ Planned |
| SQL parser              | ⏳ Planned |
| Raft replication        | ⏳ Planned |
| Scatter-gather queries  | ⏳ Planned |
| Prometheus metrics      | ⏳ Planned |
| Helm chart              | ⏳ Planned |

See the [project board](https://github.com/yuexi/distlog/projects/1) for granular progress.

## Project Layout

```
distlog/
├── cmd/                  # Binary entry points
│   ├── distlog/          # Main server (data node)
│   ├── ingester/         # Stateless ingest router
│   └── querier/          # Stateless query coordinator
├── internal/             # Private application code
│   ├── wal/              # Write-ahead log
│   ├── memtable/         # In-memory sorted structure
│   ├── sstable/          # On-disk sorted string table
│   ├── compaction/       # Background compaction
│   ├── index/            # Inverted index
│   ├── query/            # SQL parser, planner, executor
│   ├── raft/             # Raft FSM and replication
│   ├── shard/            # Shard routing and management
│   └── metrics/          # Prometheus instrumentation
├── pkg/                  # Public, importable packages
│   └── client/           # Go client SDK
├── api/
│   └── proto/            # gRPC service definitions
├── configs/              # YAML config files
├── deploy/
│   ├── docker/           # Dockerfiles
│   ├── compose/          # docker-compose.yml
│   └── helm/             # Helm chart
├── docs/                 # Design docs, architecture, benchmarks
├── scripts/              # Dev and load-testing scripts
└── test/
    ├── integration/      # End-to-end cluster tests
    └── chaos/            # Fault-injection tests
```

## Design Documents

- [`ARCHITECTURE.md`](docs/ARCHITECTURE.md) — High-level system design
- [`STORAGE.md`](docs/STORAGE.md) — LSM-tree, WAL, compaction internals
- [`INDEX.md`](docs/INDEX.md) — Inverted index encoding and query execution
- [`DISTRIBUTED.md`](docs/DISTRIBUTED.md) — Sharding, replication, Raft integration
- [`DECISIONS.md`](docs/DECISIONS.md) — Architectural decision records (ADRs)

## Development

### Build

```bash
make build      # Compile all binaries to ./bin/
make test       # Run unit tests
make lint       # Run golangci-lint
make bench      # Run benchmarks
```

### Run a local cluster

```bash
make cluster-up    # Bring up 3-node Docker Compose cluster
make cluster-down  # Tear down
make load-test     # Run load generator against local cluster
```

### Chaos testing

```bash
make chaos-test    # Random node kills + network partitions
```

## Roadmap

- [x] Project scaffolding & CI
- [ ] Single-node MVP (Week 1)
- [ ] Production-grade storage engine (Week 2)
- [ ] Distributed cluster with Raft (Week 3)
- [ ] Observability + benchmarks vs Elasticsearch (Week 4)

## Contributing

This is primarily a personal learning project, but feedback, discussion, and PRs are welcome. Open an issue to discuss any non-trivial change before submitting a PR.

## References & Inspiration

- [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [In Search of an Understandable Consensus Algorithm (Raft, Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [Designing Data-Intensive Applications (Kleppmann)](https://dataintensive.net/) — chapters 3, 5, 9
- [Lucene's index format](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/codecs/lucene90/package-summary.html)
- [Quickwit](https://github.com/quickwit-oss/quickwit) — log-optimized search engine in Rust
- [Grafana Loki](https://github.com/grafana/loki) — index-light log aggregation

## License

MIT — see [LICENSE](LICENSE).

## Author

Yuexi — [GitHub](https://github.com/yuexi) · [LinkedIn](https://linkedin.com/in/yuexi)