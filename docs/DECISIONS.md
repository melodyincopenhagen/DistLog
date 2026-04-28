# Architectural Decision Records

This file is the chronological log of design decisions that are not obvious
from the code. New entries are appended; existing entries are amended with a
"Superseded by" link rather than rewritten.

Each entry has a status (`Accepted` / `Superseded` / `Deprecated`), a date,
context, decision, and consequences. Keep them short — link to longer design
docs when needed.

---

## ADR-001: Little-endian byte order for all binary formats

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: WAL, SSTable, future on-disk formats and non-protobuf
  network framing.

### Context

DistLog needs a single byte order convention across all binary on-disk and
on-wire formats. The two reasonable options are:

1. **Little-endian**: native on x86_64 and ARM64; zero-cost on the only
   platforms we target.
2. **Big-endian / network byte order**: the historical default for
   interoperability, but irrelevant for a single-codebase storage system.

### Decision

All multi-byte integers in DistLog binary formats use little-endian encoding,
implemented via `encoding/binary.LittleEndian`. This applies to:

- WAL record header (CRC32, length, type)
- SSTable footer, meta block, index entries, data block trailers and record
  headers
- Any future custom binary protocol that is not Protocol Buffers

### Consequences

- Encoding and decoding are zero-cost on target platforms.
- Aligns with LevelDB and RocksDB precedent.
- Files are not portable to big-endian platforms (none in our deployment
  targets). If we ever need that, encoding helpers are centralized enough
  that a per-format byte-order switch is straightforward.

---

## ADR-002: SSTable on-disk format v1

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: `internal/sstable`.

### Context

The first cut of the SSTable format must support: point lookup by DocID,
ordered scan, shard pruning by min/max DocID and timestamp, atomic write,
and forward-compatible extension (bloom filters, compression, per-block
timestamp pruning) without breaking existing files.

### Decision

File layout:

```
[ data block 1 ]
[ data block 2 ]
...
[ data block N ]
[ index block  ]
[ meta block   ]
[ footer (48B) ]
```

- **Data block** target size: 16 KiB. Each block ends with a trailer
  `(record_count: u32, crc32: u32)` covering the records bytes.
- **Record header**: fixed 20 bytes — `docID: u64, timestamp: i64,
  payload_len: u32` — followed by `payload_len` bytes of payload. DocID and
  timestamp are *outside* the payload so readers can scan without
  deserializing.
- **Index block**: array of fixed 24-byte entries
  `(firstDocID, blockOffset, blockSize, reserved)` plus the same trailer
  shape as data blocks. Fixed entry size enables binary search.
- **Meta block**: 88 bytes — `min/max DocID`, `min/max Timestamp`,
  `recordCount`, `dataBlockCount`, `createdAtUnix`, `reserved[32]`, `crc32`.
- **Footer**: fixed 48 bytes. Layout in [internal/sstable/format.go](../internal/sstable/format.go).
  Magic `"DLSSTBL\0"` is at the **last 8 bytes** (LevelDB convention) so a
  successful magic read implies the footer is fully present.

CRC algorithm: `crc32.IEEE`, matching the WAL. Castagnoli is faster on
recent hardware, but cross-format consistency outweighs the micro-
optimization until profiling shows CRC as a hot spot.

Atomic write protocol: writer creates `<path>.tmp`, writes all blocks +
footer, fsyncs the file, closes it, renames to `<path>`, fsyncs the parent
directory. There is no path on which a partially-written file appears at
the final name.

### Consequences

- Forward compatibility is supported via:
  - `IndexEntry.Reserved` (4 bytes per entry) for per-block min/max ts.
  - `MetaBlock.Reserved` (32 bytes) for bloom filter offset, compression,
    tenant id, etc.
  - `Footer.Flags` (4 bytes) and 4 reserved bytes for global flags.

  New fields steal from `Reserved` without bumping `FormatVersion`. A
  `FormatVersion` bump is reserved for incompatible layout changes.

- The format is *not* compressed. Compression is a Week-2 optimization;
  when added it will be per-block, signaled by `Footer.Flags` and per-block
  metadata in `IndexEntry.Reserved`.

---

## ADR-003: macOS development, Linux production for durability semantics

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: All `fsync` and directory-fsync code paths.

### Context

`os.File.Sync()` translates to `fsync(2)` on Linux but to `fsync(2)` on
macOS as well — and macOS `fsync` only flushes to the disk cache, not the
platter. True durability on macOS requires `fcntl(F_FULLFSYNC)`.

### Decision

DistLog targets Linux production and uses `os.File.Sync()` everywhere.
Development on macOS is supported but is not durability-tested: a power
failure on a developer machine may lose recently-acked writes. This is
acceptable for a development workflow.

### Consequences

- If we ever run durability/crash tests on macOS, the test harness must use
  `golang.org/x/sys/unix.Fcntl(F_FULLFSYNC)` rather than `os.File.Sync()`.
- Production deployment must verify the underlying disk does not lie about
  flush completion (some consumer SSDs do; `fsync` returns before data is
  on stable media).

---

## ADR-004: SSTable reader uses ReadAt; mmap and block-decode cache deferred

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: `internal/sstable/reader.go`. Revisited Week 4.

### Context

The first SSTable reader needs a backing-storage strategy. Two real options:

1. **mmap** the whole file. Reads are pointer arithmetic, the OS page cache
   does the rest. Has zero-copy potential.
2. **`os.File.ReadAt`** with the OS page cache absorbing repeat reads.

The narrative pull toward mmap is strong (the resume sentence "I built an
LSM with mmap-backed SSTables" sounds great), but the supposed zero-copy
advantage does not materialize while record payloads are JSON: every
`Get` decodes payload bytes into a fresh `LogRecord` regardless of how
those bytes got into memory.

### Decision

Reader v1 uses `os.File.ReadAt`. The `readerSource` interface is the seam
where an mmap implementation can be swapped in later without changing
Reader logic.

We also explicitly do **not** add a user-space block cache in v1. The OS
page cache already de-duplicates syscalls; what would benefit from
user-space caching is the **decoded** form (`[]recordEntry`), not the raw
bytes. That is a separate optimization with its own design (LRU keying,
eviction on compaction, memory bounding) and belongs in Week 4 once we
have profiles to point at.

`loadBlock` carries a `TODO(week4)` comment naming the optimization so a
future grep finds it.

### Consequences

- Hot-block `Get` latency is dominated by JSON decoding, not by the
  syscall. Profiling in Week 4 must measure decode time vs. syscall time
  before deciding whether to cache decoded blocks, switch to mmap, or
  both.
- mmap's downsides (SIGBUS on truncation, ulimit on open mappings,
  complex crash semantics) are also avoided in v1 — we can add mmap when
  the benefit is measured, not assumed.
- Iterator and Get share `loadBlock`, so any future caching is a single-
  point change.

---
