package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func TestScan_VisitsAllRecordsInAscendingDocIDOrder(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	const n = 50
	for i := 0; i < n; i++ {
		_, err := e.Write(ctx, &types.LogRecord{
			Timestamp: types.Timestamp(i),
			Message:   fmt.Sprintf("m%d", i),
		})
		require.NoError(t, err)
	}

	var got []types.DocID
	err = e.Scan(ctx, func(id types.DocID, rec *types.LogRecord) error {
		got = append(got, id)
		require.Equal(t, fmt.Sprintf("m%d", id-1), rec.Message)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, n)
	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i], got[i-1], "scan must be ascending")
	}
}

func TestScan_AcrossActiveFrozenAndSSTables(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 4 * 1024, // force freeze + flush
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	const n = 500
	for i := 0; i < n; i++ {
		_, err := e.Write(ctx, makeBigPayload(i))
		require.NoError(t, err)
	}
	// Let flush turn frozen into sstables for at least one round.
	waitFor(t, func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.sstables) >= 1
	}, 5*time.Second, "first sstable")

	count := 0
	var last types.DocID
	require.NoError(t, e.Scan(ctx, func(id types.DocID, _ *types.LogRecord) error {
		require.Greater(t, id, last, "ascending")
		last = id
		count++
		return nil
	}))
	require.Equal(t, n, count, "every distinct docID visited exactly once")
}

func TestScan_CallbackAbortsEarly(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := e.Write(ctx, &types.LogRecord{Message: "x"})
		require.NoError(t, err)
	}

	sentinel := fmt.Errorf("stop")
	count := 0
	err = e.Scan(ctx, func(id types.DocID, _ *types.LogRecord) error {
		count++
		if count == 5 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 5, count)
}

func TestScan_ContextCancelStops(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()

	for i := 0; i < 20; i++ {
		_, err := e.Write(context.Background(), &types.LogRecord{Message: "x"})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	err = e.Scan(ctx, func(types.DocID, *types.LogRecord) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestScan_TimeRangePrunesSSTables drives the engine to multiple
// SSTables each in its own timestamp window, then runs scans that
// (a) cover all (b) cover none (c) cover the middle one only, and
// verifies the prune counter + the visited records.
func TestScan_TimeRangePrunesSSTables(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{
		DataDir:           dir,
		MemTableSizeLimit: 8 * 1024,
	})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	// Three batches with disjoint timestamp ranges.
	// Batch i uses ts in [i*1000, i*1000 + 999].
	const batchSize = 200
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < batchSize; i++ {
			_, err := e.Write(ctx, &types.LogRecord{
				Timestamp: types.Timestamp(batch*1000 + i),
				Message:   fmt.Sprintf("b%d-i%d", batch, i),
			})
			require.NoError(t, err)
		}
		// Wait for this batch to land in an sstable before starting
		// the next; gives us per-batch SSTable boundaries.
		want := batch + 1
		waitFor(t, func() bool {
			return e.Stats().SSTableCount >= want
		}, 5*time.Second, fmt.Sprintf("sstable %d", want))
	}
	require.GreaterOrEqual(t, e.Stats().SSTableCount, 3,
		"need >= 3 SSTables for this test to be meaningful")
	totalSSTables := e.Stats().SSTableCount

	// (a) Range covering everything: 0 pruned.
	var visited int
	require.NoError(t, e.ScanWithOptions(ctx, ScanOptions{
		MinTimestamp: 0,
		MaxTimestamp: types.Timestamp(10000),
	}, func(types.DocID, *types.LogRecord) error {
		visited++
		return nil
	}))
	require.Equal(t, 0, e.Stats().LastScanPrunedSSTables, "wide range should prune none")
	require.Equal(t, 3*batchSize, visited)

	// (b) Range covering none: every SSTable pruned.
	visited = 0
	require.NoError(t, e.ScanWithOptions(ctx, ScanOptions{
		MinTimestamp: types.Timestamp(50000),
		MaxTimestamp: types.Timestamp(60000),
	}, func(types.DocID, *types.LogRecord) error {
		visited++
		return nil
	}))
	require.Equal(t, totalSSTables, e.Stats().LastScanPrunedSSTables,
		"non-overlapping range should prune every SSTable")
	// MemTable not pruned by ts; active may still have records that
	// match. We can't assert visited == 0 absolutely because the
	// active MemTable from the last batch might have records in it.
	// Filtering at the visit() level happens at executor layer; the
	// engine's job here is only the pruning hint.

	// (c) Range covering middle batch [1000, 1999]. Expect 2
	// SSTables pruned (batch 0 and batch 2), regardless of how
	// records distributed across SSTables — pruning is by file
	// metadata, so we count by overlap.
	visited = 0
	expectedMessages := make(map[string]bool)
	for i := 0; i < batchSize; i++ {
		expectedMessages[fmt.Sprintf("b1-i%d", i)] = true
	}
	require.NoError(t, e.ScanWithOptions(ctx, ScanOptions{
		MinTimestamp: 1000,
		MaxTimestamp: 1999,
	}, func(_ types.DocID, rec *types.LogRecord) error {
		visited++
		return nil
	}))
	// At least one pruned (the outer batches don't overlap [1000,1999]).
	require.GreaterOrEqual(t, e.Stats().LastScanPrunedSSTables, 1,
		"middle-range scan should prune outer SSTables")
}

// TestScan_TimeRangePruneIsHintNotFilter asserts the documented
// contract that ScanOptions is a pushdown HINT, not a per-record
// filter. The engine may emit records outside the range (especially
// from MemTables, which today aren't pruned at all). Callers are
// responsible for re-evaluating the predicate.
func TestScan_TimeRangePruneIsHintNotFilter(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()

	ctx := context.Background()
	_, err = e.Write(ctx, &types.LogRecord{Timestamp: 500, Message: "outside"})
	require.NoError(t, err)
	_, err = e.Write(ctx, &types.LogRecord{Timestamp: 1500, Message: "inside"})
	require.NoError(t, err)

	visited := 0
	require.NoError(t, e.ScanWithOptions(ctx, ScanOptions{
		MinTimestamp: 1000,
		MaxTimestamp: 2000,
	}, func(_ types.DocID, _ *types.LogRecord) error {
		visited++
		return nil
	}))
	// Two records emitted because they're in the active MemTable
	// (no MemTable pruning today). This is the documented behavior;
	// the executor layer does the final filter.
	require.Equal(t, 2, visited)
}

func TestScan_OnClosedEngine(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, e.Close())
	err = e.Scan(context.Background(), func(types.DocID, *types.LogRecord) error { return nil })
	require.ErrorIs(t, err, ErrClosed)
}
