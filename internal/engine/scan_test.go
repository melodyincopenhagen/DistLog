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

func TestScan_OnClosedEngine(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, e.Close())
	err = e.Scan(context.Background(), func(types.DocID, *types.LogRecord) error { return nil })
	require.ErrorIs(t, err, ErrClosed)
}
