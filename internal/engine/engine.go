// Package engine ties the WAL, MemTable, and SSTable layers into a single
// storage engine. It owns the active write path (WAL append + in-memory
// insert), the read path (active → frozen → SSTables), and the asynchronous
// flush pipeline that turns frozen MemTables into SSTables.
//
// Stage C (current): write/get + freeze + flush + fatal-state. No recovery
// (Stage D): a reopened Engine starts empty.
//
// Concurrency invariants (must hold across all stages):
//
//  1. MemTable contents are always a subset of WAL contents. Writes go
//     WAL-first then MemTable; WAL-append failure aborts the write before
//     MemTable is touched.
//  2. e.active is non-nil while the Engine is open and not in fatal state.
//  3. len(e.frozen) == len(e.frozenWALs); each frozen MemTable corresponds
//     1:1 with a sealed WAL segment in the same slice position.
//  4. Every MemTable in e.frozen has IsFrozen() == true.
//  5. e.sstables is sorted newest-first (most-recently-flushed at index 0).
//  6. e.alloc.Peek() is greater than every persisted DocID in WAL or
//     SSTables. Allocator monotonicity guarantees this at runtime.
//  7. len(e.frozen) <= cfg.MaxFrozenMemTables. Backpressure in the freeze
//     coordinator enforces this.
//  8. Frozen MemTables never receive Put. The write path only ever
//     references e.active under the read lock; rotation under the write
//     lock atomically replaces e.active and freezes the old one before any
//     new writer can see it.
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuexishen/distlog/internal/memtable"
	"github.com/yuexishen/distlog/internal/sstable"
	"github.com/yuexishen/distlog/internal/types"
	"github.com/yuexishen/distlog/internal/wal"
)

// Errors returned by Engine.
var (
	ErrClosed              = errors.New("engine: closed")
	ErrEngineFatal         = errors.New("engine: fatal state")
	ErrTooManyWriteRetries = errors.New("engine: too many write retries")

	// ErrBackpressure is returned by Write when sustained flush stall
	// has filled both the frozen MemTable queue and the active MemTable
	// up to MemTableHardLimit, and the caller's context expired before a
	// slot opened.
	//
	// ErrBackpressure is transient: the engine remains healthy after
	// returning it, both reads and writes continue to be served, and
	// future Write calls may succeed once flush drains a frozen slot.
	// This is the contrast point with ErrEngineFatal, which is terminal.
	//
	// The returned error wraps both ErrBackpressure and the underlying
	// ctx.Err() via errors.Join, so both
	//
	//	errors.Is(err, ErrBackpressure)
	//	errors.Is(err, context.DeadlineExceeded)  // or context.Canceled
	//
	// match. Callers that distinguish "deadline elapsed (retry later)"
	// from "caller cancelled (do not retry)" can use the second form.
	ErrBackpressure = errors.New("engine: write stalled due to backpressure")
)

// OnFatalFunc is invoked when the engine enters an unrecoverable state.
//
// Constraints on the callback:
//   - MUST NOT block. The background goroutine that invoked the callback
//     will be blocked for its duration; long callbacks leak system
//     resources. For non-trivial work, dispatch to a separate goroutine
//     inside the callback.
//   - MUST NOT call Engine.Write or Engine.Close from within the callback.
//     Doing so can deadlock (Close waits for the goroutine the callback
//     runs on) or hit fatal-state errors. Engine.Get is safe — read paths
//     remain available in fatal state.
//   - MAY be invoked from any background goroutine (currently flushLoop).
//   - Invoked exactly once. Subsequent internal errors are logged but do
//     not re-trigger the callback.
//
// The default callback logs the error and panics. Override for testing or
// for custom degraded-mode handling (e.g. read-only + alert).
type OnFatalFunc func(error)

// Config controls Engine behavior.
//
// Defaults are filled in by Open if zero values are passed; only DataDir is
// strictly required.
type Config struct {
	// DataDir is the directory holding WAL segments and SSTable files.
	// Created if it does not exist.
	DataDir string

	// MemTableSizeLimit is the soft byte threshold that triggers freezing
	// the active MemTable. Default: 4 MiB.
	MemTableSizeLimit int64

	// MemTableHardLimit is the hard upper bound on the active MemTable's
	// size during a flush stall. It is *not* a steady-state target — under
	// normal operation freeze rotation keeps the active MemTable below
	// MemTableSizeLimit. Only when freeze cannot rotate (because the
	// frozen queue is full waiting on flush) does the active MemTable
	// grow past MemTableSizeLimit, and MemTableHardLimit is the size at
	// which Write enters its stall path and blocks until ctx expiry or
	// until flush drains a slot.
	//
	// Two knobs, two roles, intentionally orthogonal:
	//
	//   MemTableSizeLimit   freeze trigger
	//   MemTableHardLimit   Write-stall trigger
	//
	// The gap between them is the tolerance window for "freeze gate has
	// been told to open but hasn't actually rotated yet."
	//
	// Default: 2 × MemTableSizeLimit. Decoupling this from
	// MaxFrozenMemTables means tuning the frozen queue (a burst-absorption
	// knob) does not also amplify active-MemTable headroom; the two
	// knobs stay independent.
	MemTableHardLimit int64

	// MaxFrozenMemTables caps how many frozen MemTables may sit waiting
	// for flush before write-side backpressure kicks in (the freeze
	// coordinator waits for the next flush to drain a slot).
	// Default: 4.
	MaxFrozenMemTables int

	// MaxFlushRetries is the number of consecutive flush failures
	// tolerated before the engine enters fatal state. Default: 5.
	MaxFlushRetries int

	// FlushRetryBaseDelay is the initial backoff delay between flush
	// retries; doubled each retry and capped at FlushRetryMaxDelay.
	// Default: 100ms.
	FlushRetryBaseDelay time.Duration

	// FlushRetryMaxDelay caps the per-retry delay. Default: 5s.
	FlushRetryMaxDelay time.Duration

	// OnFatal is called when the engine enters fatal state. See
	// OnFatalFunc for constraints. If nil, the default panics.
	OnFatal OnFatalFunc
}

const (
	defaultMemTableSizeLimit  = 4 * 1024 * 1024
	defaultMaxFrozenMemTables = 4
	defaultMaxFlushRetries    = 5
	defaultRetryBaseDelay     = 100 * time.Millisecond
	defaultRetryMaxDelay      = 5 * time.Second

	// maxWriteAttempts caps Write retries when an active MemTable is
	// found frozen. Belt-and-suspenders: with the current locking the
	// retry path is unreachable, but kept as a safety net for future
	// refactors.
	maxWriteAttempts = 3
)

func (c *Config) applyDefaults() {
	if c.MemTableSizeLimit == 0 {
		c.MemTableSizeLimit = defaultMemTableSizeLimit
	}
	if c.MemTableHardLimit == 0 {
		c.MemTableHardLimit = 2 * c.MemTableSizeLimit
	}
	if c.MaxFrozenMemTables == 0 {
		c.MaxFrozenMemTables = defaultMaxFrozenMemTables
	}
	if c.MaxFlushRetries == 0 {
		c.MaxFlushRetries = defaultMaxFlushRetries
	}
	if c.FlushRetryBaseDelay == 0 {
		c.FlushRetryBaseDelay = defaultRetryBaseDelay
	}
	if c.FlushRetryMaxDelay == 0 {
		c.FlushRetryMaxDelay = defaultRetryMaxDelay
	}
	if c.OnFatal == nil {
		c.OnFatal = func(err error) { panic(fmt.Sprintf("engine: fatal: %v", err)) }
	}
}

// Engine is the durable, ordered key-value store keyed by DocID.
//
// Write semantics: Write returns once the record is in the WAL (durable)
// and the active MemTable (visible to Get). Records are flushed to
// SSTables asynchronously by a background flush worker.
//
// At-least-once: if Write returns an error, the record may or may not have
// been persisted (e.g. WAL append succeeded but a subsequent step failed
// after which the engine returned the error to the caller). Callers must
// handle write retries idempotently.
//
// Concurrency: Engine is safe for concurrent use. Multiple goroutines may
// Write and Get simultaneously.
type Engine struct {
	cfg Config

	// mu protects active/frozen/frozenWALs/sstables and the WAL manager's
	// notion of "current segment". The read lock is held briefly during
	// Write (to bind active+WAL together for an append) and during Get
	// (to snapshot slice headers). The write lock is held by freeze
	// rotation and by flush completion.
	mu         sync.RWMutex
	active     memtable.MemTable
	frozen     []memtable.MemTable
	frozenWALs []*wal.SealedSegment
	sstables   []*sstable.Reader

	walMgr *wal.Manager
	alloc  *types.DocIDAllocator

	// Counter for assigning unique SSTable filenames. Bumped under the
	// write lock during flush completion.
	nextSSTSeq atomic.Uint64

	// Background goroutine plumbing.
	freezeCh     chan struct{}
	flushCh      chan struct{}
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	workersWG    sync.WaitGroup

	// Stall condvar. Writers waiting for backpressure to clear sleep
	// here; the flush worker Broadcasts after Phase 6 (frozen queue
	// shrinks). stallMu guards the cond only — predicate evaluation
	// still queries e.mu-protected state.
	stallMu   sync.Mutex
	stallCond *sync.Cond

	// State flags.
	closed atomic.Bool
	fatal  atomic.Bool

	// Hooks for testing. Not part of the public API.
	flushHook func() // called at the start of flushOneIfAny if non-nil

	// Recovery report from the most recent Open. Set during Open and
	// thereafter immutable; safe to read without locking.
	lastRecoveryReport RecoveryReport
}

// RecoveryReport summarizes what happened during the most recent Open.
//
// Stage D returns this from LastRecoveryReport so tests, ops, and (Day 22)
// metrics can observe recovery behavior without the engine taking a
// dependency on a logger. Day 22 will additionally feed it to the structured
// logger at recovery completion.
type RecoveryReport struct {
	SSTablesLoaded         int
	SSTablesQuarantined    int
	MaxSSTableDocID        types.DocID
	WALSegmentsReplayed    int
	WALSegmentsDeleted     int
	WALRecordsReplayed     int
	WALRecordsSkipped      int
	NextDocID              types.DocID
}

// Stats is a snapshot of runtime engine state.
type Stats struct {
	ActiveMemTableSize           int64
	FrozenMemTableCount          int
	SSTableCount                 int
	CorruptedSSTablesQuarantined int
	NextDocID                    types.DocID
	Fatal                        bool
	Closed                       bool
}

// Open creates or reopens an Engine rooted at cfg.DataDir.
//
// Recovery proceeds as follows:
//
//  1. Scan dataDir for the largest historical SSTable id (across .sst and
//     .sst.tmp). Initialize the SSTable sequence counter to it so future
//     ids are strictly greater than anything ever seen.
//  2. Remove orphan .sst.tmp files (leftovers from a flush that crashed
//     before atomic rename).
//  3. Open every .sst file. Files that fail to Open are quarantined to
//     dataDir/corrupted/. The remaining valid SSTables form the engine's
//     persisted state.
//  4. Compute maxSSTableDocID = max over all loaded SSTables.
//  5. Open the WAL manager, then replay all of its segments. Records
//     whose docID is <= maxSSTableDocID are skipped (already flushed —
//     the flush succeeded but the WAL deletion may not have, or this is
//     just normal operation where WAL still holds pre-flush records).
//     Records with docID > maxSSTableDocID are inserted into a fresh
//     MemTable, which becomes the active MemTable for the new run.
//  6. Delete WAL segments whose maximum docID is <= maxSSTableDocID
//     (fully covered by SSTables). Best effort — any failure is
//     non-fatal because the WAL replay's docID-skip is idempotent.
//  7. Observe maxSSTableDocID and the replayed-MemTable's max docID into
//     the DocIDAllocator so the next Next() returns a strictly greater
//     value.
//  8. Seal whichever WAL segment is currently active (it now contains
//     records that are already in the active MemTable; we don't want
//     more records appended to it). Open a fresh segment for ongoing
//     writes. This makes WAL-segment / MemTable correspondence 1:1
//     starting from the new run.
//
// On any non-recoverable failure (quarantine fails, WAL replay hits
// mid-stream corruption), Open returns an error and any partially-opened
// resources are released.
func Open(cfg Config) (*Engine, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("engine: DataDir is required")
	}
	cfg.applyDefaults()

	// Step 1: SSTable id continuity across restarts.
	maxSSTableID, err := loadSSTableMaxID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if err := removeOrphanSSTableTmp(cfg.DataDir); err != nil {
		return nil, err
	}

	// Step 3: load and quarantine SSTables.
	sstables, quarantined, err := loadAllSSTables(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	// Step 4: maxSSTableDocID for replay cutoff.
	var maxSSTableDocID types.DocID
	for _, s := range sstables {
		if s.MaxDocID() > maxSSTableDocID {
			maxSSTableDocID = s.MaxDocID()
		}
	}

	// Sort sstables newest-first by MaxDocID.
	sortSSTablesNewestFirst(sstables)

	// Step 5: open WAL manager and replay.
	mgr, err := wal.OpenManager(cfg.DataDir)
	if err != nil {
		closeAll(sstables)
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}

	recoveredMT := memtable.New()
	report := RecoveryReport{
		SSTablesLoaded:      len(sstables),
		SSTablesQuarantined: quarantined,
		MaxSSTableDocID:     maxSSTableDocID,
	}

	// Replay sealed segments and the currently-active segment file.
	// Sealed segments may be deleted if fully covered by SSTables. The
	// active segment file MUST NOT be deleted — the WAL Manager is
	// still appending to it.
	sealedSegs := mgr.Sealed()
	type sealedInfo struct {
		seg      *wal.SealedSegment
		maxDocID types.DocID
		hasAny   bool
	}
	var sealedInfos []sealedInfo

	replayCallback := func(rec wal.Record, localMax *types.DocID, localHas *bool) error {
		docID, recordBytes, derr := decodeWALPayload(rec.Payload)
		if derr != nil {
			return fmt.Errorf("decode wal payload: %w", derr)
		}
		*localHas = true
		if docID > *localMax {
			*localMax = docID
		}
		if docID <= maxSSTableDocID {
			report.WALRecordsSkipped++
			return nil
		}
		lr, derr := types.Unmarshal(recordBytes)
		if derr != nil {
			return fmt.Errorf("unmarshal record: %w", derr)
		}
		if perr := recoveredMT.Put(docID, lr); perr != nil {
			return fmt.Errorf("recovered memtable put: %w", perr)
		}
		report.WALRecordsReplayed++
		return nil
	}

	for _, seg := range sealedSegs {
		var localMax types.DocID
		var localHas bool
		err := seg.Replay(func(rec wal.Record) error {
			return replayCallback(rec, &localMax, &localHas)
		})
		if err != nil {
			_ = mgr.Close()
			closeAll(sstables)
			return nil, fmt.Errorf("engine: wal replay sealed segment %d: %w", seg.ID(), err)
		}
		report.WALSegmentsReplayed++
		sealedInfos = append(sealedInfos, sealedInfo{seg: seg, maxDocID: localMax, hasAny: localHas})
	}

	// Replay the active segment via its file path. Manager.OpenManager
	// adopted it as active; we reuse its file path through a temporary
	// SealedSegment view (Replay opens the file on demand and does not
	// hold the active writer's fd).
	if activeID := mgr.Active().SegmentID(); activeID != 0 {
		activeView := wal.NewSealedSegmentView(mgr.Dir(), activeID)
		var localMax types.DocID
		var localHas bool
		err := activeView.Replay(func(rec wal.Record) error {
			return replayCallback(rec, &localMax, &localHas)
		})
		if err != nil {
			_ = mgr.Close()
			closeAll(sstables)
			return nil, fmt.Errorf("engine: wal replay active segment %d: %w", activeID, err)
		}
		report.WALSegmentsReplayed++
	}

	// Step 6: delete fully-covered SEALED WAL segments. Best effort.
	// The active segment is never deleted here.
	for _, info := range sealedInfos {
		if !info.hasAny || info.maxDocID <= maxSSTableDocID {
			if err := info.seg.Delete(); err == nil {
				report.WALSegmentsDeleted++
			}
		}
	}

	// Step 7: rebuild the DocIDAllocator.
	alloc := types.NewDocIDAllocator()
	alloc.Observe(maxSSTableDocID)
	if recoveredMT.Len() > 0 {
		// The MemTable's largest docID is the largest we just replayed.
		it := recoveredMT.Iterator()
		var memMax types.DocID
		for it.Next() {
			if it.DocID() > memMax {
				memMax = it.DocID()
			}
		}
		it.Close()
		alloc.Observe(memMax)
	}
	report.NextDocID = alloc.Peek() + 1

	// Step 8: do nothing structural to the WAL.
	//
	// recoveredMT contains every record from the WAL whose docID is
	// > maxSSTableDocID. The active WAL segment (adopted by OpenManager
	// from the most-recent-existing segment file) holds those same
	// records on disk. Subsequent writes will append to that same
	// segment, extending the 1:1 (active MemTable ↔ active WAL segment)
	// correspondence forward.
	//
	// On a future crash before freeze, the next Open will replay the
	// segment again and reproduce the same recoveredMT — recovery is
	// idempotent. Once the active MemTable freezes and flushes, the
	// segment becomes fully covered and is deleted by the normal flush
	// pipeline.
	//
	// The MemTable-subset-of-WAL invariant holds: every docID in
	// recoveredMT was just observed in the WAL we are still appending
	// to.

	e := &Engine{
		cfg:                cfg,
		active:             recoveredMT,
		sstables:           sstables,
		walMgr:             mgr,
		alloc:              alloc,
		freezeCh:           make(chan struct{}, 1),
		flushCh:            make(chan struct{}, 1),
		shutdownCh:         make(chan struct{}),
		lastRecoveryReport: report,
	}
	e.stallCond = sync.NewCond(&e.stallMu)
	e.nextSSTSeq.Store(maxSSTableID)

	e.workersWG.Add(2)
	go e.freezeLoop()
	go e.flushLoop()
	return e, nil
}

// Write durably appends record to the log and inserts it into the active
// MemTable. Returns the DocID assigned to the record.
//
// Returns ErrClosed if the Engine is closed, ErrEngineFatal if the engine
// is in fatal state, ErrBackpressure (joined with ctx.Err()) if a
// sustained flush stall has filled the active MemTable past
// MemTableHardLimit and ctx expires before flush drains a slot, or a
// wrapped I/O error if WAL append fails.
//
// Backpressure semantics: under healthy operation, Write does not block
// beyond WAL append latency. If freeze cannot keep up with write rate
// (frozen queue full and flush stalled), Write blocks once the active
// MemTable reaches MemTableHardLimit. Blocking is bounded by ctx —
// callers that prefer fail-fast semantics pass a short context; callers
// that prefer to wait pass a long one. ErrBackpressure is transient:
// the engine remains healthy, reads continue to be served, and the next
// Write call may succeed once flush makes progress.
func (e *Engine) Write(ctx context.Context, record *types.LogRecord) (types.DocID, error) {
	if e.closed.Load() {
		return 0, ErrClosed
	}
	if e.fatal.Load() {
		return 0, ErrEngineFatal
	}

	// Stall gate — block before allocating a docID so that timeout does
	// not consume a docID and leave a hole in the allocator sequence.
	if err := e.waitForStallClear(ctx); err != nil {
		return 0, err
	}

	docID := e.alloc.Next()
	recordBytes, err := types.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("engine: marshal record: %w", err)
	}
	walPayload := encodeWALPayload(docID, recordBytes)

	for attempt := 0; attempt < maxWriteAttempts; attempt++ {
		// Bind active MemTable + active WAL writer + perform append +
		// Put, all under the read lock. Holding the read lock across
		// these operations is what guarantees the WAL never receives
		// duplicate docIDs from retry loops: rotation requires the
		// write lock, which cannot be acquired while we hold the read
		// lock.
		e.mu.RLock()
		mt := e.active
		walWriter := e.walMgr.Active()

		if _, err := walWriter.Append(ctx, wal.RecordPut, walPayload); err != nil {
			e.mu.RUnlock()
			return 0, fmt.Errorf("engine: wal append: %w", err)
		}
		putErr := mt.Put(docID, record)
		e.mu.RUnlock()

		if putErr == nil {
			e.maybeTriggerFreeze(mt)
			return docID, nil
		}
		if !errors.Is(putErr, memtable.ErrFrozen) {
			return 0, fmt.Errorf("engine: memtable put: %w", putErr)
		}
		// Unreachable on the current code path: holding the read lock
		// during Put means the MemTable cannot be frozen between bind
		// and use. Fall through to retry as a safety net.
	}
	return 0, ErrTooManyWriteRetries
}

// Get returns the record for docID, searching active MemTable, frozen
// MemTables (newest first), and SSTables (newest first) in that order.
func (e *Engine) Get(docID types.DocID) (*types.LogRecord, bool, error) {
	e.mu.RLock()
	active := e.active
	frozen := e.frozen
	sstables := e.sstables
	e.mu.RUnlock()

	if active != nil {
		if rec, ok := active.Get(docID); ok {
			return rec, true, nil
		}
	}
	for i := len(frozen) - 1; i >= 0; i-- {
		if rec, ok := frozen[i].Get(docID); ok {
			return rec, true, nil
		}
	}
	for _, sst := range sstables {
		if docID < sst.MinDocID() || docID > sst.MaxDocID() {
			continue
		}
		rec, found, err := sst.Get(docID)
		if err != nil {
			return nil, false, err
		}
		if found {
			return rec, true, nil
		}
	}
	return nil, false, nil
}

// Close shuts down the engine and releases resources. Idempotent. After
// Close, Write returns ErrClosed; Get's behavior is undefined.
//
// Close is best-effort: it always tries to release every resource and
// returns the first error encountered. Close in fatal state is identical
// to Close in healthy state — fatal does not preclude clean shutdown.
//
// After workersWG.Wait() returns, no background goroutine is active, so
// the subsequent file-handle cleanup runs without locks.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.signalShutdown()
	e.workersWG.Wait()

	var firstErr error
	if e.walMgr != nil {
		if err := e.walMgr.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.walMgr = nil
	}
	for _, sst := range e.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.sstables = nil
	return firstErr
}

// signalShutdown closes shutdownCh exactly once, regardless of whether
// it was triggered by Close or by fatal state. Both call this; ordering
// between them is not defined, both are safe. After closing, any writers
// stalled on the backpressure cond are broadcast-woken so they can
// observe shutdown / fatal / closed and unwind cleanly.
func (e *Engine) signalShutdown() {
	e.shutdownOnce.Do(func() {
		close(e.shutdownCh)
		e.notifyStallCleared()
	})
}

// onFatalError transitions the engine into fatal state. Idempotent: only
// the first call performs work; subsequent calls are no-ops.
//
// Order: state transition → shutdown signal → user callback. The user
// callback observes a consistent fatal state (e.fatal == true, background
// goroutines told to stop).
func (e *Engine) onFatalError(err error) {
	if !e.fatal.CompareAndSwap(false, true) {
		return
	}
	e.signalShutdown()
	if e.cfg.OnFatal != nil {
		e.cfg.OnFatal(err)
	}
}

// LastRecoveryReport returns the recovery summary from the most recent
// Open. Safe to call at any time; the report is set during Open and never
// mutated afterward.
func (e *Engine) LastRecoveryReport() RecoveryReport {
	return e.lastRecoveryReport
}

// Stats returns a snapshot of runtime engine state.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var activeSize int64
	if e.active != nil {
		activeSize = e.active.SizeBytes()
	}
	return Stats{
		ActiveMemTableSize:           activeSize,
		FrozenMemTableCount:          len(e.frozen),
		SSTableCount:                 len(e.sstables),
		CorruptedSSTablesQuarantined: e.lastRecoveryReport.SSTablesQuarantined,
		NextDocID:                    e.alloc.Peek() + 1,
		Fatal:                        e.fatal.Load(),
		Closed:                       e.closed.Load(),
	}
}

// === Backpressure stall ===

// stallPredicate reports whether Write must block. The active MemTable
// is over the hard limit AND the frozen queue is full, so freeze cannot
// rotate to make room. Both conditions must hold; either alone is fine.
//
// Caller must hold e.mu (read or write).
func (e *Engine) stallPredicateLocked() bool {
	if e.active == nil {
		return false
	}
	return e.active.SizeBytes() >= e.cfg.MemTableHardLimit &&
		len(e.frozen) >= e.cfg.MaxFrozenMemTables
}

// waitForStallClear blocks until backpressure has cleared, ctx expires,
// or the engine starts shutting down. The fast path (no stall) takes
// only a read-lock check and adds no goroutines.
//
// On stall, a per-Write watchdog goroutine is started to broadcast on
// ctx expiry — sync.Cond does not natively integrate with ctx. The
// watchdog is started only on the slow path; healthy Writes do not pay
// for it.
//
// The watchdog Broadcasts to the whole cond, waking every stalled
// writer. Each woken writer re-checks (a) the predicate, (b) its own
// ctx, and (c) shutdown — standard Cond re-check loop. Spurious
// broadcasts cause harmless re-evaluation, never silent skip-of-stall.
//
// On the writer's exit path the watchdog's timer is stopped and the
// watchdog goroutine is joined via watchdogDone — no goroutine leak
// regardless of which side wins the race.
func (e *Engine) waitForStallClear(ctx context.Context) error {
	// Fast path: predicate already false.
	e.mu.RLock()
	if !e.stallPredicateLocked() {
		e.mu.RUnlock()
		return nil
	}
	e.mu.RUnlock()

	// Slow path: enter the cond loop. The watchdog goroutine ensures
	// ctx expiry wakes us even if no flush ever drains a slot.
	watchdogDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		select {
		case <-ctx.Done():
			e.stallCond.L.Lock()
			e.stallCond.Broadcast()
			e.stallCond.L.Unlock()
		case <-e.shutdownCh:
			e.stallCond.L.Lock()
			e.stallCond.Broadcast()
			e.stallCond.L.Unlock()
		case <-stop:
		}
	}()
	defer func() {
		close(stop)
		<-watchdogDone
	}()

	e.stallCond.L.Lock()
	defer e.stallCond.L.Unlock()
	for {
		// Re-check ctx and shutdown first; either may have caused the
		// wake, and we should not re-enter Wait if so.
		if err := ctx.Err(); err != nil {
			return errors.Join(ErrBackpressure, err)
		}
		if e.closed.Load() {
			return ErrClosed
		}
		if e.fatal.Load() {
			return ErrEngineFatal
		}

		// Re-check the predicate. Drop the cond mutex during the
		// e.mu read lock to avoid lock-order coupling between the
		// two — flush takes e.mu before broadcasting, and we must
		// not invert that order.
		e.stallCond.L.Unlock()
		e.mu.RLock()
		clear := !e.stallPredicateLocked()
		e.mu.RUnlock()
		e.stallCond.L.Lock()

		if clear {
			return nil
		}

		// Re-check ctx after the predicate read. The watchdog may
		// have broadcast while we were unlocked.
		if err := ctx.Err(); err != nil {
			return errors.Join(ErrBackpressure, err)
		}

		e.stallCond.Wait()
	}
}

// notifyStallCleared wakes any writers stalled in waitForStallClear.
// Safe to call unconditionally; spurious wakes are filtered by the
// predicate re-check inside the cond loop.
func (e *Engine) notifyStallCleared() {
	if e.stallCond == nil {
		return
	}
	e.stallCond.L.Lock()
	e.stallCond.Broadcast()
	e.stallCond.L.Unlock()
}

// === Freeze coordinator ===

func (e *Engine) maybeTriggerFreeze(mt memtable.MemTable) {
	if mt.SizeBytes() < e.cfg.MemTableSizeLimit {
		return
	}
	select {
	case e.freezeCh <- struct{}{}:
	default:
	}
}

func (e *Engine) freezeLoop() {
	defer e.workersWG.Done()
	for {
		select {
		case <-e.shutdownCh:
			return
		case <-e.freezeCh:
			e.doFreeze()
		}
	}
}

// doFreeze rotates the active MemTable to frozen, seals the active WAL
// segment, and opens a fresh active MemTable + WAL segment. Backpressure:
// if the frozen list is at the configured cap, doFreeze returns without
// rotating. The flush worker re-signals the freeze channel after each
// successful flush, so progress resumes when capacity is available.
func (e *Engine) doFreeze() {
	e.mu.Lock()

	if e.fatal.Load() {
		e.mu.Unlock()
		return
	}
	if e.active == nil || e.active.SizeBytes() < e.cfg.MemTableSizeLimit {
		// Already rotated by a prior call, or the threshold became
		// unmet (only possible with a future shrink mechanism; not
		// today, but cheap to check).
		e.mu.Unlock()
		return
	}
	if len(e.frozen) >= e.cfg.MaxFrozenMemTables {
		// Backpressure: defer rotation until flush drains a slot.
		e.mu.Unlock()
		return
	}

	oldActive := e.active
	oldActive.Freeze()

	sealed, err := e.walMgr.Seal()
	if err != nil {
		e.mu.Unlock()
		e.onFatalError(fmt.Errorf("engine: seal wal: %w", err))
		return
	}
	e.frozen = append(e.frozen, oldActive)
	e.frozenWALs = append(e.frozenWALs, sealed)
	e.active = memtable.New()
	e.mu.Unlock()

	// Tell the flush worker there is work.
	select {
	case e.flushCh <- struct{}{}:
	default:
	}
	// Active was just reset to an empty MemTable — even if the frozen
	// queue is still full, the predicate's "active >= hard limit" half
	// is now false and any stalled writer can proceed.
	e.notifyStallCleared()
}

// === Flush worker ===

func (e *Engine) flushLoop() {
	defer e.workersWG.Done()

	consecutiveFailures := 0
	for {
		select {
		case <-e.shutdownCh:
			return
		case <-e.flushCh:
		}

		// Drain as many frozen MemTables as exist (and as we can flush
		// successfully). On failure, back off; on persistent failure,
		// fatal.
		for {
			done, err := e.flushOneIfAny()
			if err != nil {
				consecutiveFailures++
				if consecutiveFailures >= e.cfg.MaxFlushRetries {
					e.onFatalError(fmt.Errorf("engine: flush failed %d times: %w", consecutiveFailures, err))
					return
				}
				delay := e.cfg.FlushRetryBaseDelay << (consecutiveFailures - 1)
				if delay > e.cfg.FlushRetryMaxDelay || delay <= 0 {
					delay = e.cfg.FlushRetryMaxDelay
				}
				select {
				case <-time.After(delay):
				case <-e.shutdownCh:
					return
				}
				// Retry the same frozen[0] on next iteration.
				continue
			}
			consecutiveFailures = 0
			if !done {
				break
			}
		}
	}
}

// flushOneIfAny flushes the oldest frozen MemTable if any exist.
//
// Phases:
//
//  1. Snapshot frozen[0] and frozenWALs[0] under the read lock.
//  2. Write the SSTable file (heavy I/O, no lock).
//  3. Open a Reader for the new SSTable.
//  4. Under the write lock, advance frozen/frozenWALs and prepend the
//     new SSTable to e.sstables.
//  5. Best-effort delete of the corresponding WAL segment.
//  6. Re-signal freezeCh in case freeze was waiting on backpressure.
//
// Returns done=true if a flush was performed, done=false if there was
// nothing to flush, err on any failure.
func (e *Engine) flushOneIfAny() (done bool, err error) {
	if e.flushHook != nil {
		e.flushHook()
	}
	if e.fatal.Load() {
		return false, nil
	}

	// Phase 1.
	e.mu.RLock()
	if len(e.frozen) == 0 {
		e.mu.RUnlock()
		return false, nil
	}
	mt := e.frozen[0]
	walSeg := e.frozenWALs[0]
	e.mu.RUnlock()

	// Phase 2: write SSTable.
	sstPath := e.nextSSTablePath()
	w, err := sstable.NewWriter(sstPath)
	if err != nil {
		return false, fmt.Errorf("engine: new sstable writer: %w", err)
	}

	it := mt.Iterator()
	for it.Next() {
		if err := w.Add(it.DocID(), it.Record()); err != nil {
			it.Close()
			_ = w.Abort()
			return false, fmt.Errorf("engine: sstable add: %w", err)
		}
	}
	if iterErr := it.Err(); iterErr != nil {
		it.Close()
		_ = w.Abort()
		return false, fmt.Errorf("engine: memtable iter: %w", iterErr)
	}
	it.Close()

	if err := w.Close(); err != nil {
		return false, fmt.Errorf("engine: sstable close: %w", err)
	}

	// Phase 3.
	reader, err := sstable.Open(sstPath)
	if err != nil {
		// File is on disk but unreadable. Leave it as an orphan; the
		// flush will be retried with a fresh path.
		return false, fmt.Errorf("engine: sstable open: %w", err)
	}

	// Phase 4: atomic metadata swap.
	e.mu.Lock()
	if len(e.frozen) == 0 || e.frozen[0] != mt {
		e.mu.Unlock()
		_ = reader.Close()
		return false, errors.New("engine: frozen list mutated during flush; bug")
	}
	e.frozen = e.frozen[1:]
	e.frozenWALs = e.frozenWALs[1:]
	e.sstables = append([]*sstable.Reader{reader}, e.sstables...)
	e.mu.Unlock()

	// Phase 5: best-effort WAL deletion. Failure is recovered on next
	// startup via docID-skip in WAL replay.
	if err := walSeg.Delete(); err != nil {
		// Logging is intentional but not wired up here (no logger yet).
		// Stage D / Day 22 add structured logging; for now, swallow.
		_ = err
	}

	// Phase 6: notify freeze in case it was waiting on backpressure,
	// and wake stalled writers — frozen queue just shrank by one.
	select {
	case e.freezeCh <- struct{}{}:
	default:
	}
	e.notifyStallCleared()

	return true, nil
}

func (e *Engine) nextSSTablePath() string {
	seq := e.nextSSTSeq.Add(1)
	return filepath.Join(e.cfg.DataDir, sstableFilename(seq))
}

// === SSTable filename helpers ===

const (
	sstablePrefix    = "sst-"
	sstableSuffix    = ".sst"
	sstableTmpSuffix = ".sst.tmp"
)

func sstableFilename(seq uint64) string {
	return fmt.Sprintf("%s%016d%s", sstablePrefix, seq, sstableSuffix)
}

// parseSSTableID extracts the numeric id from filenames matching either
// "sst-<id>.sst" or "sst-<id>.sst.tmp". Returns (0, false) for any other
// name shape.
func parseSSTableID(name string) (uint64, bool) {
	if !strings.HasPrefix(name, sstablePrefix) {
		return 0, false
	}
	rest := name[len(sstablePrefix):]
	var idStr string
	switch {
	case strings.HasSuffix(rest, sstableTmpSuffix):
		idStr = strings.TrimSuffix(rest, sstableTmpSuffix)
	case strings.HasSuffix(rest, sstableSuffix):
		idStr = strings.TrimSuffix(rest, sstableSuffix)
	default:
		return 0, false
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// loadSSTableMaxID returns the largest SSTable id seen in dataDir, looking
// at both .sst and .sst.tmp so the next sequence is strictly greater than
// any historical id — even those of orphan tmp files about to be removed.
// Returns 0 if no SSTable files are present.
func loadSSTableMaxID(dataDir string) (uint64, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("engine: scan dataDir for sstable ids: %w", err)
	}
	var maxID uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseSSTableID(e.Name()); ok {
			if id > maxID {
				maxID = id
			}
		}
	}
	return maxID, nil
}

// loadAllSSTables opens every .sst file in dataDir. Files that fail to
// Open (corruption, bad CRC, bad magic) are quarantined to
// dataDir/corrupted/ for forensic preservation. Returns the loaded
// readers (in directory order, not yet sorted by DocID), the count of
// quarantined files, and an error if quarantine itself fails.
func loadAllSSTables(dataDir string) (readers []*sstable.Reader, quarantined int, err error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("engine: scan dataDir for sstables: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, sstablePrefix) || !strings.HasSuffix(name, sstableSuffix) ||
			strings.HasSuffix(name, sstableTmpSuffix) {
			continue
		}
		path := filepath.Join(dataDir, name)
		r, openErr := sstable.Open(path)
		if openErr == nil {
			readers = append(readers, r)
			continue
		}
		// Corrupted: quarantine.
		if qerr := quarantineSSTable(dataDir, name); qerr != nil {
			closeAll(readers)
			return nil, 0, qerr
		}
		quarantined++
	}
	return readers, quarantined, nil
}

func quarantineSSTable(dataDir, name string) error {
	qDir := filepath.Join(dataDir, "corrupted")
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		return fmt.Errorf("engine: create quarantine dir: %w", err)
	}
	src := filepath.Join(dataDir, name)
	dst := filepath.Join(qDir, name)
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("engine: quarantine %s: %w", name, err)
	}
	return nil
}

func sortSSTablesNewestFirst(readers []*sstable.Reader) {
	// Sort by MaxDocID descending. Ties broken by MinDocID descending.
	for i := 0; i < len(readers); i++ {
		for j := i + 1; j < len(readers); j++ {
			if readers[j].MaxDocID() > readers[i].MaxDocID() ||
				(readers[j].MaxDocID() == readers[i].MaxDocID() && readers[j].MinDocID() > readers[i].MinDocID()) {
				readers[i], readers[j] = readers[j], readers[i]
			}
		}
	}
}

func closeAll(readers []*sstable.Reader) {
	for _, r := range readers {
		_ = r.Close()
	}
}

// removeOrphanSSTableTmp deletes any leftover *.sst.tmp files in dataDir.
// These are remnants of a previous flush that crashed before the atomic
// rename. They never contain useful state — the writer's contract is that
// a complete SSTable lives at the final ".sst" path or nowhere at all.
func removeOrphanSSTableTmp(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("engine: scan dataDir for tmp sstables: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), sstablePrefix) && strings.HasSuffix(e.Name(), sstableTmpSuffix) {
			if err := os.Remove(filepath.Join(dataDir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("engine: remove orphan tmp %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}
