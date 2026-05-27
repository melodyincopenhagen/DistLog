package engine

import (
	"sync"
	"sync/atomic"
)

// BlockFlushUntil gates the flush worker until the returned release
// function is called. While gated, every entry into flushOneIfAny
// blocks. Subsequent flush attempts after release proceed normally.
//
// This is a high-level test helper: callers describe intent ("stop
// flush") rather than the mechanism (callbacks at specific phases).
// Multiple BlockFlushUntil calls compose — flush remains gated until
// every release has fired.
//
// Restrictions:
//   - Must not be called after Close.
//   - The returned release is idempotent.
//   - Only one BlockFlushUntil-style hook may be installed at a time.
//     Calling SetFlushHook concurrently is a misuse; tests should pick
//     one mechanism per test.
func (e *Engine) BlockFlushUntil() (release func()) {
	gate := make(chan struct{})
	var released atomic.Bool

	prior := e.flushHook
	e.flushHook = func() {
		if prior != nil {
			prior()
		}
		if released.Load() {
			return
		}
		<-gate
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			released.Store(true)
			close(gate)
		})
	}
}

// SetFlushHook installs a function called at the entry of every flush
// attempt. Used by fault-injection tests that need to observe or
// perturb state at the start of flushOneIfAny (e.g. delete the data
// directory to force a writer error). Pass nil to clear.
//
// Lower-level than BlockFlushUntil; prefer BlockFlushUntil for
// backpressure / stall tests.
func (e *Engine) SetFlushHook(fn func()) {
	e.flushHook = fn
}

// StallPredicateForTest returns true if the engine is currently in the
// state where Write would stall on the backpressure cond. Used by
// tests to assert on stall entry/exit without racing on internal state.
func (e *Engine) StallPredicateForTest() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stallPredicateLocked()
}

// ActiveSizeForTest returns the size of the active MemTable. Used by
// tests asserting bounded growth under stall.
func (e *Engine) ActiveSizeForTest() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.active == nil {
		return 0
	}
	return e.active.SizeBytes()
}

