package memtable

import (
	"sync"
	"sync/atomic"

	"github.com/huandu/skiplist"
	"github.com/yuexishen/distlog/internal/types"
)

// New returns an empty MemTable backed by a skiplist.
//
// Concurrency: a single sync.RWMutex serializes structural mutations (Put,
// Freeze) against readers (Get, Iterator). The underlying skiplist is not
// itself thread-safe; correctness depends on this lock.
func New() MemTable {
	return &skipMemTable{
		list: skiplist.New(skiplist.GreaterThanFunc(func(a, b any) int {
			ax, bx := a.(types.DocID), b.(types.DocID)
			switch {
			case ax < bx:
				return -1
			case ax > bx:
				return 1
			default:
				return 0
			}
		})),
	}
}

type skipMemTable struct {
	mu        sync.RWMutex
	list      *skiplist.SkipList
	sizeBytes atomic.Int64
	frozen    atomic.Bool
}

func (m *skipMemTable) Put(docID types.DocID, record *types.LogRecord) error {
	if m.frozen.Load() {
		return ErrFrozen
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the lock: a concurrent Freeze may have raced.
	if m.frozen.Load() {
		return ErrFrozen
	}

	// On overwrite, subtract the old size before adding the new. Overwrite
	// is not expected on the hot path (DocIDs are allocated fresh) but is
	// supported for correctness — recovery may replay a record more than
	// once if WAL truncation lags behind flush.
	if old := m.list.Get(docID); old != nil {
		m.sizeBytes.Add(-recordSizeBytes(old.Value.(*types.LogRecord)))
	}
	m.list.Set(docID, record)
	m.sizeBytes.Add(recordSizeBytes(record))
	return nil
}

func (m *skipMemTable) Get(docID types.DocID) (*types.LogRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	el := m.list.Get(docID)
	if el == nil {
		return nil, false
	}
	return el.Value.(*types.LogRecord), true
}

func (m *skipMemTable) SizeBytes() int64 {
	return m.sizeBytes.Load()
}

func (m *skipMemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.Len()
}

func (m *skipMemTable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen.Store(true)
}

func (m *skipMemTable) IsFrozen() bool {
	return m.frozen.Load()
}

func (m *skipMemTable) Iterator() Iterator {
	m.mu.RLock()
	return &skipIterator{
		parent: m,
		// Front returns the smallest element. We position before it; first
		// Next() advances to it.
		next:    m.list.Front(),
		started: false,
	}
}

// skipIterator is an in-memory iterator over the skiplist. It implements
// iter.Iterator. Because every operation is in-memory, Err() always returns
// nil — the field exists only to satisfy the shared interface used by
// SSTable iterators (where I/O can fail mid-iteration).
type skipIterator struct {
	parent  *skipMemTable
	cur     *skiplist.Element
	next    *skiplist.Element
	started bool
	closed  bool
	err     error // always nil; see type doc
}

func (it *skipIterator) Next() bool {
	if it.closed {
		return false
	}
	if !it.started {
		it.started = true
		it.cur = it.next
		if it.cur != nil {
			it.next = it.cur.Next()
		}
		return it.cur != nil
	}
	it.cur = it.next
	if it.cur == nil {
		return false
	}
	it.next = it.cur.Next()
	return true
}

func (it *skipIterator) DocID() types.DocID {
	return it.cur.Key().(types.DocID)
}

func (it *skipIterator) Record() *types.LogRecord {
	return it.cur.Value.(*types.LogRecord)
}

func (it *skipIterator) Err() error { return it.err }

func (it *skipIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.cur = nil
	it.next = nil
	it.parent.mu.RUnlock()
	return nil
}
