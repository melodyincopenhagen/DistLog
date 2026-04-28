// Package types defines the core domain types shared across DistLog subsystems.
//
// Types in this package are intentionally minimal and free of behavior beyond
// trivial conversions. Anything that requires I/O, locking, or non-trivial
// logic belongs in a higher-level package.
package types

import "time"

// DocID is the system-assigned monotonically increasing identifier for a log
// record. Allocated by DocIDAllocator at ingest time. Used as the primary key
// in MemTable, SSTable, and inverted index posting lists.
//
// DocID(0) is reserved as "unassigned" and must never be persisted.
type DocID uint64

// Timestamp is unix nanoseconds since the epoch. A typed wrapper around int64
// to prevent accidental mixing with other int64 quantities (offsets, sizes,
// LSNs).
type Timestamp int64

func Now() Timestamp { return Timestamp(time.Now().UnixNano()) }

func (t Timestamp) Time() time.Time { return time.Unix(0, int64(t)) }

func (t Timestamp) UnixNano() int64 { return int64(t) }

// LogRecord is the business-level representation of one log line. It does not
// carry a DocID — DocIDs are system-assigned and travel alongside the record
// (see memtable.Entry). This keeps LogRecord meaningful both before and after
// ingest without an "assigned vs unassigned" state machine.
type LogRecord struct {
	Timestamp Timestamp
	TenantID  string
	Source    string
	Message   string
	// Fields holds structured key-value pairs. First version is string-only:
	// numeric range queries on fields are explicitly out of scope (see
	// docs/DECISIONS.md when written).
	Fields map[string]string
}
