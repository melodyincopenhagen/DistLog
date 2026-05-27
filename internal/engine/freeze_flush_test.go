package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/memtable"
	"github.com/yuexishen/distlog/internal/types"
	"github.com/yuexishen/distlog/internal/wal"
)

// makeBigPayload returns a record whose marshaled size is large enough that
// `count` of them comfortably exceed `threshold` bytes.
func makeBigPayload(i int) *types.LogRecord {
	body := strings.Repeat("x", 200)
	return &types.LogRecord{
		Timestamp: types.Now(),
		TenantID:  "t1",
		Source:    fmt.Sprintf("host-%d", i),
		Message:   fmt.Sprintf("msg-%d-%s", i, body),
		Fields:    map[string]string{"level": "info", "k": body},
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

// === Freeze + Flush round-trip ===

func TestFreezeAndFlushProducesSSTable(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 16 * 1024, // small to trigger freeze fast
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	const n = 200
	ids := make([]types.DocID, n)
	for i := 0; i < n; i++ {
		id, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
		ids[i] = id
	}

	// Wait until at least one SSTable has been produced.
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.sstables) >= 1
	}, 5*time.Second, "first SSTable to appear")

	// All previously-written records still readable across active +
	// frozen + sstables.
	for i, id := range ids {
		rec, ok, err := e.Get(id)
		require.NoError(t, err)
		require.True(t, ok, "missing docID %d (i=%d)", id, i)
		require.Contains(t, rec.Message, fmt.Sprintf("msg-%d-", i))
	}
}

func TestMultipleFlushesDrainFrozenList(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 8 * 1024,
		MaxFrozenMemTables: 8,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}

	// Eventually frozen drains to ~0 and many SSTables exist.
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.sstables) >= 3 && len(e.frozen) <= 1
	}, 10*time.Second, "frozen list to drain into multiple SSTables")
}

// === Red-line: WAL never has duplicate docIDs ===

func TestWALNeverHasDuplicateDocID(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 4 * 1024, // very small to force frequent freezes
	})
	require.NoError(t, err)

	ctx := context.Background()
	const writers = 4
	const perWriter = 200

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				_, err := e.Write(ctx, makeBigPayload(i))
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, e.Close())

	// Independently scan all WAL segments for duplicates.
	mgr, err := wal.OpenManager(dir)
	require.NoError(t, err)
	defer mgr.Close()
	// Seal the active so we can replay it too.
	_, _ = mgr.Seal()

	seen := make(map[types.DocID]int)
	for _, seg := range mgr.Sealed() {
		require.NoError(t, seg.Replay(func(rec wal.Record) error {
			docID, _, err := decodeWALPayload(rec.Payload)
			if err != nil {
				return err
			}
			seen[docID]++
			return nil
		}))
	}

	for id, count := range seen {
		require.Equal(t, 1, count, "docID %d appeared %d times in WAL", id, count)
	}
}

// === Red-line: frozen MemTables never receive Put ===

func TestFrozenMemTableRejectsPut(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:             dir,
		MemTableSizeLimit:   8 * 1024,
		MemTableHardLimit:   1 << 30, // never stall — this test gates only via the frozen queue
		MaxFrozenMemTables:  64,      // never backpressure
		FlushRetryBaseDelay: 10 * time.Millisecond, // unused, but harmless
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.frozen) >= 2
	}, 5*time.Second, "frozen list to accumulate")

	// Snapshot a frozen MemTable and assert it rejects Put.
	e.mu.RLock()
	require.GreaterOrEqual(t, len(e.frozen), 1)
	frozenMT := e.frozen[0]
	e.mu.RUnlock()

	require.True(t, frozenMT.IsFrozen())
	err = frozenMT.Put(99999999, makeBigPayload(0))
	require.ErrorIs(t, err, memtable.ErrFrozen)

	// Release the flush gate so Close can proceed.
	release()
}

// === Red-line: fatal state ===

func TestEngine_FatalState_WriteRejected(t *testing.T) {
	dir := t.TempDir()
	var fatalCalls atomic.Int64
	var fatalErr atomic.Value // error
	e, err := Open(Config{
		DataDir:             dir,
		MemTableSizeLimit:   4 * 1024,
		MaxFlushRetries:     1,
		FlushRetryBaseDelay: 1 * time.Millisecond,
		FlushRetryMaxDelay:  1 * time.Millisecond,
		OnFatal: func(err error) {
			fatalCalls.Add(1)
			fatalErr.Store(err)
		},
	})
	require.NoError(t, err)
	defer e.Close()

	// Inject a hook that makes the SSTable writer path fail by removing
	// the data directory just before flush. We restore it afterwards so
	// Close can still operate; engine has already entered fatal state.
	saved := dir
	e.flushHook = func() {
		// Replace dir with a path that does not exist to make sstable
		// NewWriter fail.
		if _, err := os.Stat(saved); err == nil {
			_ = os.RemoveAll(saved)
		}
	}

	ctx := context.Background()
	id, err := e.Write(ctx, makeBigPayload(0))
	require.NoError(t, err)

	// Push enough data to trigger freeze and flush attempts.
	for i := 1; i < 200; i++ {
		_, _ = e.Write(ctx, makeBigPayload(i))
	}

	// Wait for fatal state.
	waitFor(t, func() bool { return e.fatal.Load() }, 5*time.Second, "fatal state")

	// Restore dir for clean Close.
	_ = os.MkdirAll(saved, 0o755)

	// Write rejected.
	_, werr := e.Write(ctx, makeBigPayload(0))
	require.ErrorIs(t, werr, ErrEngineFatal)

	// Get still works for active-MemTable hits (the first write).
	rec, ok, gerr := e.Get(id)
	require.NoError(t, gerr)
	if ok { // active may have been rotated to frozen MemTable
		require.NotNil(t, rec)
	}

	// Callback called at least once with a non-nil error.
	require.GreaterOrEqual(t, fatalCalls.Load(), int64(1))
	require.NotNil(t, fatalErr.Load())

	// Close cleanly.
	require.NoError(t, e.Close())
}

func TestEngine_FatalState_OnFatalCalledOnce(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int64
	e, err := Open(Config{
		DataDir: dir,
		OnFatal: func(error) { calls.Add(1) },
	})
	require.NoError(t, err)
	defer e.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.onFatalError(errors.New("trigger"))
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), calls.Load())
}

// === Red-line: concurrent close, no deadlock ===

func TestConcurrentCloseDoesntDeadlock(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  8 * 1024,
		MaxFrozenMemTables: 16,
	})
	require.NoError(t, err)

	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = e.Write(ctx, makeBigPayload(0))
		}
	}()

	// Let writes go for a moment so flush worker has work in flight.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Now hit Close concurrently from many goroutines.
	const closers = 8
	closeWG := sync.WaitGroup{}
	for i := 0; i < closers; i++ {
		closeWG.Add(1)
		go func() {
			defer closeWG.Done()
			require.NoError(t, e.Close())
		}()
	}
	doneCh := make(chan struct{})
	go func() {
		closeWG.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked")
	}
}

// === Behavior: freeze does not block writes ===

func TestFreezeDoesNotBlockWrites(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 8 * 1024,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	// Pre-fill so the first write triggers freeze near-immediately.
	for i := 0; i < 50; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}

	// Now time a batch of writes; even with freeze + flush in flight,
	// each write should complete quickly.
	const samples = 100
	const maxPerWrite = 250 * time.Millisecond // generous bound for CI
	for i := 0; i < samples; i++ {
		start := time.Now()
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
		elapsed := time.Since(start)
		require.Less(t, elapsed, maxPerWrite, "write %d took %s, freeze likely blocked path", i, elapsed)
	}
}

// === Backpressure ===

func TestBackpressure_FreezeWaitsForFlush(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:            dir,
		MemTableSizeLimit:  4 * 1024,
		MemTableHardLimit:  1 << 30, // effectively no Write stall — this test asserts only the frozen-cap invariant
		MaxFrozenMemTables: 2,
	})
	require.NoError(t, err)
	defer e.Close()

	release := e.BlockFlushUntil()

	ctx := context.Background()
	for i := 0; i < 800; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}

	// Frozen list must not exceed cap while flush is gated.
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.frozen) >= 1
	}, 5*time.Second, "some frozen to appear")

	// Sample the cap a few times — it must never exceed
	// MaxFrozenMemTables.
	for i := 0; i < 20; i++ {
		e.mu.RLock()
		require.LessOrEqual(t, len(e.frozen), 2)
		e.mu.RUnlock()
		time.Sleep(2 * time.Millisecond)
	}

	// Open the gate; frozen list should drain.
	release()
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.frozen) == 0
	}, 5*time.Second, "frozen list to drain after gate opened")
}

// === Config: retry counts respected ===

func TestRetryConfigsRespected(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int64
	e, err := Open(Config{
		DataDir:             dir,
		MemTableSizeLimit:   4 * 1024,
		MaxFlushRetries:     1,
		FlushRetryBaseDelay: 1 * time.Millisecond,
		FlushRetryMaxDelay:  1 * time.Millisecond,
		OnFatal: func(error) { calls.Add(1) },
	})
	require.NoError(t, err)
	defer e.Close()

	saved := dir
	e.flushHook = func() {
		if _, err := os.Stat(saved); err == nil {
			_ = os.RemoveAll(saved)
		}
	}

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_, _ = e.Write(ctx, makeBigPayload(i))
	}

	waitFor(t, func() bool { return calls.Load() >= 1 }, 5*time.Second, "fatal callback fired")
	require.Equal(t, int64(1), calls.Load(), "OnFatal must be called exactly once")

	// Restore dir for clean shutdown.
	_ = os.MkdirAll(saved, 0o755)
}

// === SSTable filenames are unique ===

func TestSSTablesGetUniqueFilenames(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 4 * 1024,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	for i := 0; i < 300; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}

	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.sstables) >= 2
	}, 5*time.Second, "multiple sstables")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	seen := map[string]bool{}
	for _, ent := range entries {
		if filepath.Ext(ent.Name()) == ".sst" {
			require.False(t, seen[ent.Name()], "duplicate sstable name: %s", ent.Name())
			seen[ent.Name()] = true
		}
	}
	require.GreaterOrEqual(t, len(seen), 2)
}
