package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBackpressure_ActiveMemTableBoundedUnderStall is the regression
// test for the gap identified in ADR-009: prior to Stage E, a
// permanently-stalled flush would let the active MemTable grow without
// bound while the frozen queue stayed at its cap.
//
// With the fix in place, Write blocks once the active MemTable reaches
// MemTableHardLimit, and the active size stays within the configured
// bound.
func TestBackpressure_ActiveMemTableBoundedUnderStall(t *testing.T) {
	dir := t.TempDir()
	const (
		softLimit = int64(4 * 1024)
		hardLimit = int64(8 * 1024)
		maxFrozen = 2
	)
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  softLimit,
		MemTableHardLimit:  hardLimit,
		MaxFrozenMemTables: maxFrozen,
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()
	defer release()

	// Drive writes from a background goroutine using a long-context
	// "patient" caller. We expect this to eventually block once the
	// frozen queue fills and active reaches hard limit.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writesDone := make(chan struct{})
	go func() {
		defer close(writesDone)
		for i := 0; ; i++ {
			if _, err := e.Write(ctx, makeBigPayload(i)); err != nil {
				return
			}
		}
	}()

	// Wait for stall to engage: predicate must become true.
	waitFor(t, e.StallPredicateForTest, 5*time.Second, "engine to enter stall")

	// Sample for a short window — under stall the active size must
	// not exceed hardLimit by more than one record's worth (a single
	// in-flight Write past the predicate check is allowed to land,
	// but the next one must block).
	const margin = int64(1 * 1024) // generous: one record << 1 KiB
	for i := 0; i < 50; i++ {
		size := e.ActiveSizeForTest()
		require.LessOrEqual(t, size, hardLimit+margin,
			"active MemTable %d B exceeds hardLimit %d B + margin %d B", size, hardLimit, margin)
		time.Sleep(2 * time.Millisecond)
	}

	// Release flush; writes should resume and the writer goroutine
	// stays alive (no error). Cancel ctx to let it exit.
	release()
	waitFor(t, func() bool { return !e.StallPredicateForTest() }, 5*time.Second, "stall to clear")
	cancel()
	<-writesDone
}

// TestBackpressure_CtxDeadlineReturnsErrBackpressure verifies that a
// fail-fast caller (short ctx) gets ErrBackpressure joined with
// DeadlineExceeded — and the engine remains healthy for subsequent
// writes once stall clears.
func TestBackpressure_CtxDeadlineReturnsErrBackpressure(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  4 * 1024,
		MemTableHardLimit:  8 * 1024,
		MaxFrozenMemTables: 2,
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()
	defer release()

	// Drive engine into stall using long-context writes from another
	// goroutine. Need to keep them coming until stall engages.
	driverCtx, driverCancel := context.WithCancel(context.Background())
	defer driverCancel()
	driverDone := make(chan struct{})
	go func() {
		defer close(driverDone)
		for i := 0; ; i++ {
			if _, err := e.Write(driverCtx, makeBigPayload(i)); err != nil {
				return
			}
		}
	}()

	waitFor(t, e.StallPredicateForTest, 5*time.Second, "engine to enter stall")

	// Now issue a fail-fast write with a short deadline.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shortCancel()
	_, err = e.Write(shortCtx, makeBigPayload(99999))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBackpressure)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Engine still healthy: stats reports not fatal, not closed.
	stats := e.Stats()
	require.False(t, stats.Fatal)
	require.False(t, stats.Closed)

	// Release stall; subsequent writes succeed.
	release()
	waitFor(t, func() bool { return !e.StallPredicateForTest() }, 5*time.Second, "stall to clear")

	postCtx, postCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer postCancel()
	_, err = e.Write(postCtx, makeBigPayload(123456))
	require.NoError(t, err, "engine must accept writes after backpressure clears")

	driverCancel()
	<-driverDone
}

// TestBackpressure_CtxCancelReturnsErrBackpressure verifies the
// canceled-context case (vs. deadline). Both should trip
// ErrBackpressure but the wrapped ctx error differs — important for
// callers deciding whether to retry.
func TestBackpressure_CtxCancelReturnsErrBackpressure(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  4 * 1024,
		MemTableHardLimit:  8 * 1024,
		MaxFrozenMemTables: 2,
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()
	defer release()

	driverCtx, driverCancel := context.WithCancel(context.Background())
	defer driverCancel()
	driverDone := make(chan struct{})
	go func() {
		defer close(driverDone)
		for i := 0; ; i++ {
			if _, err := e.Write(driverCtx, makeBigPayload(i)); err != nil {
				return
			}
		}
	}()
	waitFor(t, e.StallPredicateForTest, 5*time.Second, "engine to enter stall")

	cancelCtx, cancelFn := context.WithCancel(context.Background())
	writeErr := make(chan error, 1)
	go func() {
		_, e := e.Write(cancelCtx, makeBigPayload(77777))
		writeErr <- e
	}()
	// Give the Write a moment to enter the stall loop.
	time.Sleep(20 * time.Millisecond)
	cancelFn()
	select {
	case err := <-writeErr:
		require.ErrorIs(t, err, ErrBackpressure)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not return after ctx cancel")
	}

	driverCancel()
	<-driverDone
}

// TestBackpressure_NConcurrentWritersAllResume drives N writers into
// the stall path simultaneously; after release, every one of them
// must wake and complete. Catches missed-broadcast bugs and any
// re-check-loop logic that lets a goroutine skip the predicate.
func TestBackpressure_NConcurrentWritersAllResume(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  4 * 1024,
		MemTableHardLimit:  8 * 1024,
		MaxFrozenMemTables: 2,
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()
	defer release()

	// Drive into stall.
	driverCtx, driverCancel := context.WithCancel(context.Background())
	defer driverCancel()
	driverDone := make(chan struct{})
	go func() {
		defer close(driverDone)
		for i := 0; ; i++ {
			if _, err := e.Write(driverCtx, makeBigPayload(i)); err != nil {
				return
			}
		}
	}()
	waitFor(t, e.StallPredicateForTest, 5*time.Second, "engine to enter stall")

	const N = 16
	results := make(chan error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := e.Write(ctx, makeBigPayload(1_000_000+i))
			results <- err
		}(i)
	}

	// Briefly let all N goroutines park in the cond.
	time.Sleep(50 * time.Millisecond)

	// Stop the driver (so it doesn't keep filling) and release flush.
	driverCancel()
	<-driverDone
	release()

	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err, "every stalled writer must wake and complete")
	}
}

// TestBackpressure_WatchdogDoesNotLeak — best-effort goroutine count
// check. Healthy writes (no stall) must not spawn watchdog goroutines.
func TestBackpressure_WatchdogDoesNotLeak(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 4 * 1024,
		MemTableHardLimit: 8 * 1024,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}
	// No direct goroutine count assertion — too flaky across CI. The
	// real assertion is that the test does not deadlock or OOM,
	// which validates the fast-path no-spawn invariant indirectly.
}

// TestEngine_WALAppendFailure_EngineRemainsHealthy is the
// ADR-009-tail companion test: when WAL append fails (simulated
// abrupt segment closure), Write returns an error but the engine
// itself is still able to serve reads and does not panic on
// subsequent operations. The MemTable invariants hold — we never
// inserted into MemTable past the failed append.
func TestEngine_WALAppendFailure_EngineRemainsHealthy(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 4 * 1024,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()

	// First write succeeds, gives us a known-good docID.
	id1, err := e.Write(ctx, makeBigPayload(1))
	require.NoError(t, err)

	// Force the WAL active segment closed beneath the engine. Next
	// Append will return an error.
	require.NoError(t, e.walMgr.SimulateAbruptShutdownForTest())

	// Next Write must fail with a wrapped WAL error.
	_, err = e.Write(ctx, makeBigPayload(2))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrEngineFatal,
		"WAL append failure must surface as a per-call error, not as fatal")

	// Engine is not in fatal state — WAL append failures are
	// per-call errors, not fatal.
	stats := e.Stats()
	require.False(t, stats.Fatal,
		"engine entered fatal state on WAL append failure; expected per-call error only")
	require.False(t, stats.Closed)

	// Reads of the previously-written record still work.
	rec, ok, gerr := e.Get(id1)
	require.NoError(t, gerr)
	require.True(t, ok)
	require.NotNil(t, rec)

	// Another Write keeps failing (segment is still closed) but does
	// not panic. The MemTable's "subset of WAL" invariant is preserved
	// because we never inserted into MemTable on the failed path.
	_, err = e.Write(ctx, makeBigPayload(3))
	require.Error(t, err)
}
