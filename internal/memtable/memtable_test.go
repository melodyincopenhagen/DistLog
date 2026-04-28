package memtable

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func rec(msg string) *types.LogRecord {
	return &types.LogRecord{
		Timestamp: types.Now(),
		TenantID:  "t1",
		Source:    "host-1",
		Message:   msg,
		Fields:    map[string]string{"level": "info"},
	}
}

func TestPutGet(t *testing.T) {
	m := New()
	require.NoError(t, m.Put(1, rec("a")))
	require.NoError(t, m.Put(2, rec("b")))

	got, ok := m.Get(1)
	require.True(t, ok)
	require.Equal(t, "a", got.Message)

	got, ok = m.Get(2)
	require.True(t, ok)
	require.Equal(t, "b", got.Message)

	_, ok = m.Get(3)
	require.False(t, ok)
}

func TestPutOverwriteAdjustsSize(t *testing.T) {
	m := New()
	require.NoError(t, m.Put(1, rec("short")))
	first := m.SizeBytes()

	require.NoError(t, m.Put(1, rec("a much much longer message body indeed")))
	second := m.SizeBytes()

	require.Greater(t, second, first)
	require.Equal(t, 1, m.Len(), "overwrite must not increase len")
}

func TestIteratorAscending(t *testing.T) {
	m := New()
	// Insert out of order.
	for _, id := range []types.DocID{5, 1, 3, 2, 4} {
		require.NoError(t, m.Put(id, rec("x")))
	}

	it := m.Iterator()
	defer it.Close()

	var got []types.DocID
	for it.Next() {
		got = append(got, it.DocID())
	}
	require.NoError(t, it.Err())
	require.Equal(t, []types.DocID{1, 2, 3, 4, 5}, got)
}

func TestIteratorOnEmpty(t *testing.T) {
	m := New()
	it := m.Iterator()
	defer it.Close()
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestFreezeRejectsPut(t *testing.T) {
	m := New()
	require.NoError(t, m.Put(1, rec("before")))
	m.Freeze()
	require.True(t, m.IsFrozen())

	err := m.Put(2, rec("after"))
	require.ErrorIs(t, err, ErrFrozen)

	// Get and Iterator still work.
	got, ok := m.Get(1)
	require.True(t, ok)
	require.Equal(t, "before", got.Message)

	it := m.Iterator()
	defer it.Close()
	require.True(t, it.Next())
	require.Equal(t, types.DocID(1), it.DocID())
	require.False(t, it.Next())
}

func TestFreezeIdempotent(t *testing.T) {
	m := New()
	m.Freeze()
	m.Freeze() // must not panic or deadlock
	require.True(t, m.IsFrozen())
}

func TestSizeBytesGrowsMonotonicallyAndIsNonZero(t *testing.T) {
	m := New()
	require.Zero(t, m.SizeBytes())
	prev := int64(0)
	for i := types.DocID(1); i <= 100; i++ {
		require.NoError(t, m.Put(i, rec("payload")))
		cur := m.SizeBytes()
		require.Greater(t, cur, prev)
		prev = cur
	}
}

func TestLargeInsertIteratorOrder(t *testing.T) {
	m := New()
	const n = 100_000

	ids := make([]types.DocID, n)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(n, func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

	for _, id := range ids {
		require.NoError(t, m.Put(id, rec("x")))
	}

	it := m.Iterator()
	defer it.Close()
	expected := types.DocID(1)
	count := 0
	for it.Next() {
		require.Equal(t, expected, it.DocID())
		expected++
		count++
	}
	require.Equal(t, n, count)
}

// TestConcurrentReadersAndWriter verifies that multiple Get callers and one
// writer can run concurrently without data races. Run with -race to be
// meaningful.
func TestConcurrentReadersAndWriter(t *testing.T) {
	m := New()
	const writes = 10_000
	const readers = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := types.DocID(1); i <= writes; i++ {
			require.NoError(t, m.Put(i, rec("x")))
		}
		cancel()
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, _ = m.Get(types.DocID(rand.Intn(writes) + 1))
			}
		}()
	}
	wg.Wait()

	require.Equal(t, writes, m.Len())
}

// TestIteratorBlocksWriterUntilClose verifies that Iterator holds a read lock
// that prevents concurrent Put — and that Close releases it. The writer must
// proceed only after the iterator is closed.
func TestIteratorBlocksWriterUntilClose(t *testing.T) {
	m := New()
	require.NoError(t, m.Put(1, rec("x")))

	it := m.Iterator()

	writeDone := make(chan struct{})
	go func() {
		_ = m.Put(2, rec("y"))
		close(writeDone)
	}()

	// Writer should be blocked while iterator is open.
	select {
	case <-writeDone:
		t.Fatal("writer completed while iterator held the read lock")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, it.Close())

	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not unblock after iterator close")
	}
}

// TestConcurrentPutNoLost verifies that concurrent Put with disjoint DocIDs
// loses no entries.
func TestConcurrentPutNoLost(t *testing.T) {
	m := New()
	const writers = 8
	const perW = 1000

	var wg sync.WaitGroup
	var ok atomic.Int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		base := types.DocID(w*perW + 1)
		go func() {
			defer wg.Done()
			for i := types.DocID(0); i < perW; i++ {
				if err := m.Put(base+i, rec("x")); err == nil {
					ok.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(writers*perW), ok.Load())
	require.Equal(t, writers*perW, m.Len())
}
