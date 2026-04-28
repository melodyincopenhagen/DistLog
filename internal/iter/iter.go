// Package iter defines the shared Iterator interface used by every ordered
// data source in the LSM-tree (MemTable, SSTable, future merge iterators).
//
// Sharing one interface lets compaction and query execution merge sources of
// different physical types without per-type adapters.
package iter

import "github.com/yuexishen/distlog/internal/types"

// Iterator walks records in ascending DocID order.
//
// Lifecycle:
//
//	it := source.Iterator()
//	defer it.Close()
//	for it.Next() {
//	    docID := it.DocID()
//	    rec   := it.Record()
//	    ...
//	}
//	if err := it.Err(); err != nil { ... }
//
// First Next() advances to the first element; DocID/Record are only valid
// after Next() returns true. After Next() returns false, callers must use
// Err() to distinguish clean end-of-stream from error termination.
//
// Concurrency: Iterator is NOT safe for concurrent use. The data source
// (Reader / MemTable) may itself be concurrent-safe; the iterator is not.
type Iterator interface {
	Next() bool
	DocID() types.DocID
	Record() *types.LogRecord
	Err() error
	Close() error
}
