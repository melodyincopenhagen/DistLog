package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
	"github.com/yuexishen/distlog/internal/wal"
)

func mkRec(msg string) *types.LogRecord {
	return &types.LogRecord{
		Timestamp: types.Now(),
		TenantID:  "t1",
		Source:    "host-1",
		Message:   msg,
		Fields:    map[string]string{"level": "info"},
	}
}

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestOpenRequiresDataDir(t *testing.T) {
	_, err := Open(Config{})
	require.Error(t, err)
}

func TestOpenAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()
	require.Equal(t, int64(defaultMemTableSizeLimit), e.cfg.MemTableSizeLimit)
	require.Equal(t, defaultMaxFrozenMemTables, e.cfg.MaxFrozenMemTables)
}

func TestWriteAssignsMonotonicDocIDs(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	var prev types.DocID
	for i := 0; i < 50; i++ {
		id, err := e.Write(ctx, mkRec(fmt.Sprintf("m-%d", i)))
		require.NoError(t, err)
		require.Greater(t, id, prev)
		prev = id
	}
	require.Equal(t, types.DocID(1), types.DocID(1)) // sanity
}

func TestWriteFirstDocIDIsOne(t *testing.T) {
	e := openTestEngine(t)
	id, err := e.Write(context.Background(), mkRec("first"))
	require.NoError(t, err)
	require.Equal(t, types.DocID(1), id)
}

func TestWriteThenGet(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()

	ids := make([]types.DocID, 100)
	for i := 0; i < 100; i++ {
		id, err := e.Write(ctx, mkRec(fmt.Sprintf("msg-%d", i)))
		require.NoError(t, err)
		ids[i] = id
	}

	for i, id := range ids {
		rec, ok, err := e.Get(id)
		require.NoError(t, err)
		require.True(t, ok, "missing docID %d (i=%d)", id, i)
		require.Equal(t, fmt.Sprintf("msg-%d", i), rec.Message)
	}
}

func TestGetMissingDocID(t *testing.T) {
	e := openTestEngine(t)
	rec, ok, err := e.Get(99999)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, rec)
}

func TestWriteAfterCloseFails(t *testing.T) {
	e := openTestEngine(t)
	require.NoError(t, e.Close())
	_, err := e.Write(context.Background(), mkRec("x"))
	require.ErrorIs(t, err, ErrClosed)
}

func TestCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, e.Close())
	require.NoError(t, e.Close())
}

func TestWriteRespectsContextCancel(t *testing.T) {
	e := openTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.Write(ctx, mkRec("x"))
	require.Error(t, err)
}

// TestWriteWritesToWAL verifies the MemTable-subset-of-WAL invariant in the
// only direction observable in Stage B: every Write produces a WAL record.
// We Seal the WAL, replay it, and check that we recover all docID/payload
// pairs in order.
func TestWriteWritesToWAL(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)

	ctx := context.Background()
	var docIDs []types.DocID
	var messages []string
	for i := 0; i < 20; i++ {
		msg := fmt.Sprintf("m-%d", i)
		id, err := e.Write(ctx, mkRec(msg))
		require.NoError(t, err)
		docIDs = append(docIDs, id)
		messages = append(messages, msg)
	}
	require.NoError(t, e.Close())

	// Reopen WAL manager directly; seal active to make it replayable.
	mgr, err := wal.OpenManager(dir)
	require.NoError(t, err)
	defer mgr.Close()
	sealed, err := mgr.Seal()
	require.NoError(t, err)

	var i int
	require.NoError(t, sealed.Replay(func(rec wal.Record) error {
		require.Equal(t, wal.RecordPut, rec.Type)
		gotID, recordBytes, err := decodeWALPayload(rec.Payload)
		require.NoError(t, err)
		require.Equal(t, docIDs[i], gotID)
		got, err := types.Unmarshal(recordBytes)
		require.NoError(t, err)
		require.Equal(t, messages[i], got.Message)
		i++
		return nil
	}))
	require.Equal(t, len(docIDs), i)
}

// TestReopenWithoutFlushSeesAllData verifies that a write made before Close
// remains readable after reopen — the WAL replay path resurrects it into
// the active MemTable. This is the inverse of the Stage B placeholder
// (TestReopenWithoutFlushSeesEmptyEngine), flipped at Stage D once
// recovery was implemented.
func TestReopenWithoutFlushSeesAllData(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	id, err := e.Write(context.Background(), mkRec("survivor"))
	require.NoError(t, err)
	require.NoError(t, e.Close())

	e2, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e2.Close()
	rec, ok, err := e2.Get(id)
	require.NoError(t, err)
	require.True(t, ok, "Stage D recovery must replay WAL into active MemTable")
	require.NotNil(t, rec)
	require.Equal(t, "survivor", rec.Message)
}

func TestConcurrentWriteAndGet(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()

	const writers = 4
	const perWriter = 250
	type entry struct {
		id  types.DocID
		msg string
	}
	resultsCh := make(chan entry, writers*perWriter)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				msg := fmt.Sprintf("w%d-i%d", w, i)
				id, err := e.Write(ctx, mkRec(msg))
				require.NoError(t, err)
				resultsCh <- entry{id, msg}
			}
		}(w)
	}
	wg.Wait()
	close(resultsCh)

	count := 0
	for r := range resultsCh {
		got, ok, err := e.Get(r.id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, r.msg, got.Message)
		count++
	}
	require.Equal(t, writers*perWriter, count)
}

// TestDocIDsUniqueUnderConcurrency: belt-and-suspenders that DocIDAllocator
// + Engine path do not produce duplicate DocIDs under concurrent writes.
func TestDocIDsUniqueUnderConcurrency(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()

	const writers = 8
	const perWriter = 200

	idsCh := make(chan types.DocID, writers*perWriter)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id, err := e.Write(ctx, mkRec("x"))
				require.NoError(t, err)
				idsCh <- id
			}
		}()
	}
	wg.Wait()
	close(idsCh)

	seen := make(map[types.DocID]bool, writers*perWriter)
	for id := range idsCh {
		require.False(t, seen[id], "duplicate docID: %d", id)
		seen[id] = true
	}
	require.Len(t, seen, writers*perWriter)
}

// TestDataDirIsCreated checks that Open creates a non-existent DataDir
// (the WAL manager's MkdirAll behavior).
func TestDataDirIsCreated(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "nested", "engine-data")
	e, err := Open(Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()
}
