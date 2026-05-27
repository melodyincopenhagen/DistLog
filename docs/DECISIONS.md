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

## ADR-007: Corrupted SSTable Quarantine Policy

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: `internal/engine` Open path.

### Context

When the engine opens, it scans the data directory for SSTable files and
loads them. Some of those files may be corrupt (bad CRC, bad magic,
truncated). Three policies were considered:

1. Fail the Open call: forces operator intervention before service can
   resume.
2. Delete the corrupt file: simple, but irreversible.
3. Quarantine the corrupt file to a `corrupted/` subdirectory: keeps
   forensic evidence while letting the service continue.

### Decision

Quarantine to `dataDir/corrupted/`:

- The corrupt SSTable is **moved** (not copied) via `os.Rename` so the
  primary directory no longer contains it.
- The engine records the count in `RecoveryReport.SSTablesQuarantined`
  and exposes it via `Stats().CorruptedSSTablesQuarantined`. Day 22 will
  expose this as a Prometheus metric.
- A successful quarantine (move) lets Open succeed. Recovery proceeds
  with the remaining valid SSTables.
- A failed quarantine (cannot create `corrupted/`, cannot rename) makes
  Open **fail**. This is one of the few "hard error at startup" cases:
  if the disk cannot tolerate a directory create or rename, the engine
  cannot reliably continue.

### Out of scope: partial recovery

We do not attempt to extract still-valid records from a partially-
corrupt SSTable. The reasoning:

- A footer-OK / data-block-corrupt SSTable is harder to handle than
  fully corrupt: which docID ranges are trustworthy? The metadata
  said `MinDocID=1, MaxDocID=1000` but if a middle block is bad we
  cannot tell which docIDs actually got persisted.
- Partial recovery doubles the surface area of the recovery code and
  introduces a class of "silently lost data" bugs.
- WAL is the ground truth. If the WAL covering those docIDs has not
  been deleted yet, replay will recover them. If it has, the data is
  truly lost; partial SSTable recovery would not change that for the
  ranges that lived only in the corrupted SSTable.

If a future operator wants forensic recovery, they can manually inspect
the quarantined file with a debug tool.

### Consequences

- Engine never silently discards data on its own; corruption is
  preserved on disk and counted.
- Operations: a non-zero `CorruptedSSTablesQuarantined` is a strong
  signal of disk-level trouble (bad sectors, kernel bug, hardware
  failure) and should page on-call.
- The `corrupted/` directory grows over time; cleanup is an
  operational decision, not the engine's responsibility.

---

## ADR-008: Fork-Self Crash Test Deferred to Week 4

- **Status**: Accepted
- **Date**: 2026-04-28
- **Applies to**: Crash-recovery test coverage in `internal/engine`.

### Context

Stage D needed a "post-SIGKILL recovery" test. Two implementation
options:

- **(A)** Fork-self subprocess test: spawn a child running an Engine,
  let it write data, then `SIGKILL` the child and reopen the engine
  in the parent. This faithfully models a SIGKILL: all goroutines
  terminate, all file descriptors are reclaimed by the OS, only
  `fsync`'d state survives.
- **(B)** "Drop the WAL handle" approximation: in a single test
  process, call `Manager.SimulateAbruptShutdownForTest()` to close
  the active segment's `*os.File` without going through any cleanup
  path, then re-Open on the same dataDir.

### Decision

Stage D ships option (B). Option (A) is deferred to the Week 4 chaos
test bundle (Day 21).

### Rationale

- (B) catches the recovery-path correctness questions Stage D needs
  to answer: WAL replay over a non-cleanly-closed segment, docID
  allocator resumption, SSTable rediscovery.
- (B) costs ~30 minutes; (A) costs ~4 hours plus ongoing maintenance
  of a test-helper binary entry point.
- Day 21 chaos testing has a coherent story: SIGKILL, filesystem
  errors injected, partial-write simulation. Adding the SIGKILL
  test there bundles all crash-related testing in one place where
  it can grow into a real chaos suite, rather than living as a
  one-off in Stage D.

### Limits of option (B)

(B) does **not** model:

- Goroutine termination: the freeze and flush goroutines in the
  test process keep running after `SimulateAbruptShutdownForTest`.
  They will hit IO errors on their next operation but stay live;
  the engine's fatal-state path may or may not trigger depending on
  timing.
- Page cache loss: a real SIGKILL preserves anything that was
  fsync'd; the OS page cache for the dead process is reclaimed, so
  re-Open reads from disk fresh. In option (B) the page cache is
  shared with the test process and may give an unrealistically
  optimistic view of "what survived".

These gaps are exactly what Day 21's option (A) test is meant to
close.

### Consequences

- The Stage D test `TestRecovery_FromAbruptShutdown` is real but
  not exhaustive. Its name acknowledges the limitation in its doc
  string.
- Day 21 chaos plan must include a fork-self SIGKILL test as a
  first-class deliverable, not as a stretch goal.

---

## ADR-009: Backpressure Semantics Under Sustained Flush Stall

- **Status**: Accepted
- **Date**: 2026-04-28 (proposed); 2026-04-28 (accepted, Stage E)
- **Applies to**: `internal/engine` Write path and freeze coordinator.

### Context

Identified during the Stage D wrap-up audit. The current Stage C
backpressure implementation has a real gap: when `len(e.frozen) >=
MaxFrozenMemTables`, `doFreeze` returns without rotating, but **the
write path is not gated**. Concrete sequence:

1. Flush stalls (e.g. disk slow, hook gates flush in tests).
2. `e.active` reaches `MemTableSizeLimit`. `maybeTriggerFreeze` sends
   on `freezeCh`.
3. `doFreeze` wakes, sees `len(frozen) >= MaxFrozenMemTables`, returns.
4. Next Write succeeds — `e.active` is the same MemTable, `Put` works
   on a frozen=false MemTable, size grows past the limit.
5. `maybeTriggerFreeze` keeps signaling; `doFreeze` keeps bouncing.
6. `e.active` grows without bound; OOM is the only stop.

The Stage C `TestBackpressure_FreezeWaitsForFlush` test asserts the
right thing about the frozen list (cap not exceeded) but never looks
at the active MemTable's size, so this gap was missed.

### Options

**Option A — Hard limit with stall.** Add a `MemTableHardLimit`
config field. When `e.active.SizeBytes() >= MemTableHardLimit` AND
`len(e.frozen) >= MaxFrozenMemTables`, the write path stalls.
Sub-questions:

- *Block or fail-fast?* Probably "block on Write(ctx) until gate
  opens or ctx expires; on ctx expiry return ErrBackpressure". Single
  signature serves both fail-fast (short ctx) and patient (long ctx)
  callers. RocksDB's write stall is roughly this shape.
- *Default for MemTableHardLimit?* The naive
  `MemTableSizeLimit × MaxFrozenMemTables` lets active grow up to
  ~MaxFrozenMemTables × the per-table limit before stalling, which
  pushes peak memory to roughly `(MaxFrozenMemTables + 1) ×
  MemTableSizeLimit`. That conflicts with the user's likely
  intuition that `MaxFrozenMemTables` caps memory at
  `MaxFrozenMemTables × MemTableSizeLimit`. Pick a default that
  makes the memory ceiling predictable from existing config fields,
  not surprising.
- *Wait primitive?* `sync.Cond` on a guard mutex, signaled by
  `flushOneIfAny` Phase 6 when frozen drains. `time.NewTimer`-driven
  ctx watchdog wakes the wait if ctx expires.

**Option B — Document non-blocking semantics.** Engine never blocks
Write; the operator is responsible for monitoring
`Stats().FrozenMemTableCount + ActiveMemTableSize` and applying flow
control upstream. Simple and Kafka-broker-shaped, but pushes a hard
correctness problem onto every caller.

### Decision

Adopt Option A in the "ctx-driven block-or-error" form. Concrete
parameters:

- New config field `MemTableHardLimit`, defaulting to
  `2 × MemTableSizeLimit`. Decoupled from `MaxFrozenMemTables` so
  the two knobs stay orthogonal: tuning the frozen queue (a
  burst-absorption knob) does not also amplify active-MemTable
  headroom.
- `MemTableHardLimit` is documented as "hard upper bound under
  stall," not "steady-state target." Under healthy operation the
  active MemTable stays well below it; the hard limit only matters
  when freeze cannot rotate.
- Stall primitive is `sync.Cond` on a dedicated `stallMu`. The
  flush worker `Broadcast`s after Phase 6 (frozen queue shrinks).
  The freeze coordinator `Broadcast`s after rotating active to
  fresh.
- Per-Write watchdog goroutine, started only on the slow path,
  bridges `ctx` to `sync.Cond`. Healthy Writes do not pay this
  cost — the fast path is a single read-lock predicate check.
- Watchdog lifecycle is explicit: a `stop` channel and a
  `watchdogDone` channel pair guarantee the goroutine exits before
  Write returns, regardless of which side wins the race (natural
  wake vs. ctx expiry).
- Error type: `errors.Join(ErrBackpressure, ctx.Err())`. Both
  `errors.Is(err, ErrBackpressure)` and
  `errors.Is(err, context.DeadlineExceeded)` (or `context.Canceled`)
  match, letting callers distinguish "deadline elapsed, retry later"
  from "caller cancelled, do not retry."

### Why not Option B

Option B (engine never blocks; caller monitors stats and applies
flow control upstream) was rejected because the LSM engine's Write
callers — replication FSM, ingestion pipelines, log shippers —
typically do not know and should not need to know the state of the
frozen MemTable queue. Pushing that responsibility onto every caller
makes correctness an N-place problem instead of a 1-place one, and
historically these "monitor and back off" contracts decay: callers
forget the contract, miss the metric, or sample too slowly. Option A
keeps the correctness boundary inside the engine; the only thing
callers must understand is `ctx` and a sentinel error, both of which
they already handle for every other engine call.

The Kafka-broker analogy in the original Option B framing does not
hold cleanly: Kafka brokers expose the queue state via a wire
protocol with explicit producer-side throttling primitives, not via
"check stats and self-throttle." DistLog has no such protocol, and
adding one purely to support non-blocking Writes is more work than
the cond-based block.

### Implementation notes from Stage E

- `flushHook` was kept as a low-level fault-injection seam; a
  higher-level `BlockFlushUntil() (release func())` test helper
  was added in `internal/engine/export_test.go` for stall tests.
  Following stdlib precedent, no `//go:build test` tag is used:
  `_test.go` files are only compiled under `go test` and can
  expose package-private symbols to same-package tests.
- The pre-Stage-E test `TestBackpressure_FreezeWaitsForFlush`
  was preserved (with `MemTableHardLimit: 1<<30` so it asserts
  only the frozen-queue invariant), and a new
  `TestBackpressure_ActiveMemTableBoundedUnderStall` asserts
  the active-MemTable growth bound that ADR-009 was filed for.
- Companion test
  `TestEngine_WALAppendFailure_EngineRemainsHealthy` covers the
  ADR-009-tail concern: WAL append failure surfaces as a per-call
  error, the engine stays out of fatal state, and reads of
  pre-failure data continue to work. The MemTable-subset-of-WAL
  invariant is preserved because the Write path returns before
  touching MemTable on WAL failure.

---
