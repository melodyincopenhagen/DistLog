// Package memtable provides the in-memory write buffer of the LSM-tree.
//
// Records are inserted with a monotonically increasing DocID supplied by the
// caller. When SizeBytes exceeds the configured threshold, the active
// MemTable is frozen and flushed to an SSTable; further writes go to a new
// MemTable. The active/frozen swap is handled by the storage layer above
// this package, not here.
package memtable

import (
	"errors"

	"github.com/yuexishen/distlog/internal/iter"
	"github.com/yuexishen/distlog/internal/types"
)

// ErrFrozen is returned by Put after the MemTable has been frozen for flush.
// Callers (the ingest path) treat this as a signal to rotate to a new active
// MemTable and retry.
var ErrFrozen = errors.New("memtable: frozen")

// Iterator is the shared ordered-record iterator interface. Re-exported here
// so callers can use memtable.Iterator without an extra import.
//
// MemTable's Iterator implementation holds a read lock for its entire
// lifetime; Close MUST be called to release it. Deferring Close at the call
// site is the recommended pattern. An iterator is single-use: after Close,
// no other methods may be called.
type Iterator = iter.Iterator

// MemTable is the in-memory ordered store keyed by DocID.
type MemTable interface {
	// Put inserts (docID, record). DocIDs must be supplied in any order;
	// the MemTable orders them internally. Returns ErrFrozen if the
	// MemTable has been frozen.
	Put(docID types.DocID, record *types.LogRecord) error

	// Get returns the record for docID, or (nil, false) if absent.
	Get(docID types.DocID) (*types.LogRecord, bool)

	// Iterator returns a snapshot iterator in ascending DocID order. Holds
	// a read lock until Close() is called. Valid on both active and
	// frozen MemTables.
	Iterator() Iterator

	// SizeBytes returns an estimate of the memory footprint of stored
	// records. Estimate, not exact: used by the flush trigger, which is a
	// soft threshold.
	SizeBytes() int64

	// Len returns the number of entries.
	Len() int

	// Freeze marks the MemTable as immutable. After Freeze, Put returns
	// ErrFrozen; Get and Iterator continue to work. Idempotent.
	Freeze()

	// IsFrozen reports whether Freeze has been called.
	IsFrozen() bool
}

// recordSizeBytes estimates the in-memory footprint of a LogRecord. Used for
// flush threshold accounting; not exact and not stable across Go versions.
//
// Coverage: string content + map entries + a fixed per-record overhead that
// approximates struct headers, slice headers, and skiplist node bookkeeping.
func recordSizeBytes(r *types.LogRecord) int64 {
	const perRecordOverhead = 64 // rough: struct header + skiplist node + pointers
	const perFieldOverhead = 16  // rough: map bucket entry overhead
	n := int64(perRecordOverhead)
	n += int64(len(r.TenantID) + len(r.Source) + len(r.Message))
	for k, v := range r.Fields {
		n += int64(len(k) + len(v) + perFieldOverhead)
	}
	return n
}
