package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

// writeSST writes ids→records to a fresh SSTable and returns its path.
func writeSST(t *testing.T, dir string, ids []types.DocID) string {
	t.Helper()
	path := filepath.Join(dir, "x.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)
	for _, id := range ids {
		require.NoError(t, w.Add(id, mkRec(fmt.Sprintf("msg-%d", id), int64(id)*1000)))
	}
	require.NoError(t, w.Close())
	return path
}

func TestReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ids := make([]types.DocID, 1000)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	path := writeSST(t, dir, ids)

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	require.Equal(t, types.DocID(1), r.MinDocID())
	require.Equal(t, types.DocID(1000), r.MaxDocID())
	require.Equal(t, uint64(1000), r.RecordCount())
	require.Equal(t, types.Timestamp(1*1000), r.MinTimestamp())
	require.Equal(t, types.Timestamp(1000*1000), r.MaxTimestamp())

	for _, id := range ids {
		rec, ok, err := r.Get(id)
		require.NoError(t, err)
		require.True(t, ok, "missing docID %d", id)
		require.Equal(t, fmt.Sprintf("msg-%d", id), rec.Message)
		require.Equal(t, types.Timestamp(int64(id)*1000), rec.Timestamp)
	}
}

func TestReaderGetOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{10, 20, 30})

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	for _, id := range []types.DocID{1, 5, 9, 31, 100, 1000} {
		rec, ok, err := r.Get(id)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, rec)
	}
}

func TestReaderGetGap(t *testing.T) {
	dir := t.TempDir()
	// In-range gaps: 10, 20, 30 — query 15, 25 inside range but absent.
	path := writeSST(t, dir, []types.DocID{10, 20, 30})

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	for _, id := range []types.DocID{11, 15, 19, 21, 25, 29} {
		rec, ok, err := r.Get(id)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, rec)
	}
}

func TestReaderIteratorAscending(t *testing.T) {
	dir := t.TempDir()
	ids := []types.DocID{2, 5, 7, 11, 13, 17, 19, 23}
	path := writeSST(t, dir, ids)

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	it := r.Iterator()
	defer it.Close()

	var got []types.DocID
	for it.Next() {
		got = append(got, it.DocID())
		require.NotNil(t, it.Record())
	}
	require.NoError(t, it.Err())
	require.Equal(t, ids, got)
}

func TestReaderIteratorAcrossManyBlocks(t *testing.T) {
	dir := t.TempDir()
	ids := make([]types.DocID, 10_000)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	path := writeSST(t, dir, ids)

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()
	require.Greater(t, r.Meta().DataBlockCount, uint32(1))

	it := r.Iterator()
	defer it.Close()

	expected := types.DocID(1)
	count := 0
	for it.Next() {
		require.Equal(t, expected, it.DocID())
		expected++
		count++
	}
	require.NoError(t, it.Err())
	require.Equal(t, len(ids), count)
}

func TestReaderEmptyIterator(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1})

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	it := r.Iterator()
	defer it.Close()
	require.True(t, it.Next())
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

// === Corruption tests ===

func TestOpenRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1, 2, 3})

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xFF // last byte is part of magic
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = Open(path)
	require.ErrorIs(t, err, ErrBadMagic)
}

func TestOpenRejectsBadFooterCRC(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1, 2, 3})

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Flip a byte inside the footer body covered by footerCRC (offset 0..32
	// of the 48-byte footer at file end). Avoid the magic at [40..48).
	footerStart := len(data) - FooterSize
	data[footerStart] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = Open(path)
	require.ErrorIs(t, err, ErrBadCRC)
}

func TestOpenRejectsBadVersion(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1})

	// Recompute a footer with an invalid version and write back.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	footerStart := len(data) - FooterSize
	footer, err := UnmarshalFooter(data[footerStart:])
	require.NoError(t, err)
	footer.Version = 999
	newFooter := footer.Marshal()
	copy(data[footerStart:], newFooter)
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = Open(path)
	require.ErrorIs(t, err, ErrBadVersion)
}

func TestOpenRejectsBadIndexCRC(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1, 2, 3, 4, 5})

	// Pull the footer to find the index block, then flip a byte inside it.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	footer, err := UnmarshalFooter(data[len(data)-FooterSize:])
	require.NoError(t, err)
	// Flip one byte at the very start of the index block (an entry's
	// FirstDocID); CRC will fail.
	data[footer.IndexBlockOffset] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = Open(path)
	require.ErrorIs(t, err, ErrBadCRC)
}

func TestGetReturnsErrorOnBadDataBlockCRC(t *testing.T) {
	dir := t.TempDir()
	// Many records → multiple blocks. We'll corrupt block 0 specifically.
	ids := make([]types.DocID, 5000)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	path := writeSST(t, dir, ids)

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Open once cleanly to grab the first block's offset from indexEnt.
	r, err := Open(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(r.indexEnt), 2)
	firstBlockOff := r.indexEnt[0].BlockOffset
	firstBlockTargetDocID := r.indexEnt[0].FirstDocID
	require.NoError(t, r.Close())

	// Flip a byte deep inside block 0's body (well past the record header
	// of the first record so we're hitting payload bytes covered by CRC).
	data[firstBlockOff+50] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o644))

	r, err = Open(path) // Open still succeeds (footer/index/meta intact)
	require.NoError(t, err)
	defer r.Close()

	rec, ok, err := r.Get(firstBlockTargetDocID)
	require.Error(t, err, "block CRC failure must return error, not silent miss")
	require.ErrorIs(t, err, ErrBadCRC)
	require.False(t, ok)
	require.Nil(t, rec)

	// A docID in a later, intact block should still work.
	laterDocID := r.indexEnt[len(r.indexEnt)-1].FirstDocID
	rec, ok, err = r.Get(laterDocID)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, rec)
}

func TestIteratorTerminatesOnCorruptBlock(t *testing.T) {
	dir := t.TempDir()
	ids := make([]types.DocID, 5000)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	path := writeSST(t, dir, ids)

	// Corrupt a middle block.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	r, err := Open(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(r.indexEnt), 3)
	midOff := r.indexEnt[len(r.indexEnt)/2].BlockOffset
	require.NoError(t, r.Close())

	data[midOff+30] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o644))

	r, err = Open(path)
	require.NoError(t, err)
	defer r.Close()

	it := r.Iterator()
	defer it.Close()

	yielded := 0
	for it.Next() {
		yielded++
	}
	require.Error(t, it.Err(), "iterator must surface block CRC failure")
	require.ErrorIs(t, it.Err(), ErrBadCRC)
	require.Greater(t, yielded, 0, "should have iterated through clean blocks before hitting corruption")
	require.Less(t, yielded, len(ids), "must stop at corruption, not skip-and-continue")
}

func TestOpenMissingFile(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "nope.sst"))
	require.Error(t, err)
}

func TestOpenTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := writeSST(t, dir, []types.DocID{1, 2, 3})

	// Truncate to less than footer size.
	require.NoError(t, os.Truncate(path, 4))
	_, err := Open(path)
	require.Error(t, err)
}

// === Concurrency ===

func TestReaderConcurrentGet(t *testing.T) {
	dir := t.TempDir()
	const n = 2000
	ids := make([]types.DocID, n)
	for i := range ids {
		ids[i] = types.DocID(i + 1)
	}
	path := writeSST(t, dir, ids)

	r, err := Open(path)
	require.NoError(t, err)
	defer r.Close()

	const goroutines = 16
	const perG = 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				id := types.DocID(((seed*perG + i) % n) + 1)
				rec, ok, err := r.Get(id)
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, fmt.Sprintf("msg-%d", id), rec.Message)
			}
		}(g)
	}
	wg.Wait()
}
