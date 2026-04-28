package types

import "sync/atomic"

// DocIDAllocator hands out monotonically increasing DocIDs.
//
// Recovery model: at startup, every persistent source that may hold DocIDs
// (WAL, SSTables, snapshot metadata) calls Observe with the maximum DocID it
// has seen. Order does not matter; the allocator's next value will be
// max(observed) + 1. This keeps recovery composable as new persistence layers
// are added (Day 3+: SSTables; Week 3+: snapshots) without changing the
// allocator's interface.
//
// DocIDAllocator is safe for concurrent use.
type DocIDAllocator struct {
	next atomic.Uint64
}

// NewDocIDAllocator returns an allocator whose first Next() will return
// DocID(1). DocID(0) is reserved as "unassigned".
func NewDocIDAllocator() *DocIDAllocator {
	return &DocIDAllocator{}
}

// Next returns a fresh DocID, never previously returned by this allocator
// and strictly greater than every previously returned or observed value.
func (a *DocIDAllocator) Next() DocID {
	return DocID(a.next.Add(1))
}

// Observe records that DocID `seen` has been encountered in persistent
// storage. After all observations, the next Next() call will return a value
// strictly greater than every observed DocID. Safe to call concurrently with
// Next() and other Observe() calls, though typically used during single-
// threaded recovery before Next() is invoked.
func (a *DocIDAllocator) Observe(seen DocID) {
	for {
		cur := a.next.Load()
		if uint64(seen) <= cur {
			return
		}
		if a.next.CompareAndSwap(cur, uint64(seen)) {
			return
		}
	}
}

// Peek returns the most recently allocated DocID without advancing. Returns
// DocID(0) if Next() has never been called and Observe has not seen anything.
// Intended for metrics and tests, not for allocation logic.
func (a *DocIDAllocator) Peek() DocID {
	return DocID(a.next.Load())
}
