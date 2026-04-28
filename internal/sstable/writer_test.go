package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func mkRec(msg string, ts int64) *types.LogRecord {
	return &types.LogRecord{
		Timestamp: types.Timestamp(ts),
		TenantID:  "t1",
		Source:    "host-1",
		Message:   msg,
		Fields:    map[string]string{"level": "info"},
	}
}

func TestWriterBasicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.sst")

	w, err := NewWriter(path)
	require.NoError(t, err)

	for i := types.DocID(1); i <= 100; i++ {
		require.NoError(t, w.Add(i, mkRec(fmt.Sprintf("msg-%d", i), int64(i)*1000)))
	}
	require.NoError(t, w.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(FooterSize))

	// Tmp file must be gone.
	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))

	// Read footer from the tail.
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	footerBuf := make([]byte, FooterSize)
	_, err = f.ReadAt(footerBuf, info.Size()-FooterSize)
	require.NoError(t, err)
	footer, err := UnmarshalFooter(footerBuf)
	require.NoError(t, err)
	require.Equal(t, FormatVersion, footer.Version)

	// Read meta block.
	metaBuf := make([]byte, footer.MetaBlockSize)
	_, err = f.ReadAt(metaBuf, int64(footer.MetaBlockOffset))
	require.NoError(t, err)
	meta, err := UnmarshalMetaBlock(metaBuf)
	require.NoError(t, err)
	require.Equal(t, types.DocID(1), meta.MinDocID)
	require.Equal(t, types.DocID(100), meta.MaxDocID)
	require.Equal(t, uint64(100), meta.RecordCount)
	require.Equal(t, types.Timestamp(1000), meta.MinTimestamp)
	require.Equal(t, types.Timestamp(100*1000), meta.MaxTimestamp)
	require.GreaterOrEqual(t, meta.DataBlockCount, uint32(1))
}

func TestWriterRejectsNonMonotonicDocID(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "bad.sst"))
	require.NoError(t, err)

	require.NoError(t, w.Add(5, mkRec("a", 1)))
	err = w.Add(5, mkRec("dup", 2)) // equal — must fail
	require.Error(t, err)
	err = w.Add(3, mkRec("backwards", 2)) // smaller — must fail
	require.Error(t, err)

	// Recover by adding a strictly greater id.
	require.NoError(t, w.Add(6, mkRec("ok", 3)))
	require.NoError(t, w.Close())
}

func TestWriterCloseRejectsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)

	require.Error(t, w.Close())

	// Both tmp and final should be absent.
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))
}

func TestWriterAddAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "x.sst"))
	require.NoError(t, err)
	require.NoError(t, w.Add(1, mkRec("a", 1)))
	require.NoError(t, w.Close())
	require.Error(t, w.Add(2, mkRec("b", 2)))
}

func TestWriterCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "x.sst"))
	require.NoError(t, err)
	require.NoError(t, w.Add(1, mkRec("a", 1)))
	require.NoError(t, w.Close())
	require.NoError(t, w.Close()) // second call is no-op
}

func TestWriterAbortRemovesTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "abort.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)
	require.NoError(t, w.Add(1, mkRec("a", 1)))

	// Tmp file must exist before abort.
	_, err = os.Stat(path + ".tmp")
	require.NoError(t, err)

	require.NoError(t, w.Abort())

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))

	// Abort is idempotent.
	require.NoError(t, w.Abort())
}

func TestWriterAbortAfterCloseIsNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)
	require.NoError(t, w.Add(1, mkRec("a", 1)))
	require.NoError(t, w.Close())
	require.NoError(t, w.Abort())

	// Final file is still there.
	_, err = os.Stat(path)
	require.NoError(t, err)
}

// TestWriterProducesMultipleBlocks pushes enough data to force several data
// blocks and verifies the index records them all in increasing first-docID
// order.
func TestWriterProducesMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "many.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)

	const n = 5000
	for i := types.DocID(1); i <= n; i++ {
		require.NoError(t, w.Add(i, mkRec("payload-large-enough-to-fill-blocks", int64(i))))
	}
	require.NoError(t, w.Close())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	info, err := f.Stat()
	require.NoError(t, err)

	footerBuf := make([]byte, FooterSize)
	_, err = f.ReadAt(footerBuf, info.Size()-FooterSize)
	require.NoError(t, err)
	footer, err := UnmarshalFooter(footerBuf)
	require.NoError(t, err)

	// Read meta block.
	metaBuf := make([]byte, footer.MetaBlockSize)
	_, err = f.ReadAt(metaBuf, int64(footer.MetaBlockOffset))
	require.NoError(t, err)
	meta, err := UnmarshalMetaBlock(metaBuf)
	require.NoError(t, err)
	require.Greater(t, meta.DataBlockCount, uint32(1), "should produce more than one data block")
	require.Equal(t, uint64(n), meta.RecordCount)

	// Read index block and verify entries are strictly ascending in
	// FirstDocID and that count matches the meta.
	indexBuf := make([]byte, footer.IndexBlockSize)
	_, err = f.ReadAt(indexBuf, int64(footer.IndexBlockOffset))
	require.NoError(t, err)

	body := indexBuf[:len(indexBuf)-indexBlockTrailerSize]
	trailer := indexBuf[len(indexBuf)-indexBlockTrailerSize:]
	gotEntryCount := binary.LittleEndian.Uint32(trailer[0:4])
	require.Equal(t, meta.DataBlockCount, gotEntryCount)
	require.Equal(t, crc(body), binary.LittleEndian.Uint32(trailer[4:8]))

	var prev types.DocID
	for i := uint32(0); i < gotEntryCount; i++ {
		off := i * IndexEntrySize
		e, err := UnmarshalIndexEntry(body[off : off+IndexEntrySize])
		require.NoError(t, err)
		if i > 0 {
			require.Greater(t, e.FirstDocID, prev)
		}
		prev = e.FirstDocID
	}
	require.Equal(t, types.DocID(1), func() types.DocID {
		e, _ := UnmarshalIndexEntry(body[0:IndexEntrySize])
		return e.FirstDocID
	}())
}

func TestWriterFooterMagicAndOffsetsMatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.sst")
	w, err := NewWriter(path)
	require.NoError(t, err)
	for i := types.DocID(1); i <= 10; i++ {
		require.NoError(t, w.Add(i, mkRec("m", int64(i))))
	}
	require.NoError(t, w.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Last 8 bytes must be the magic.
	require.Equal(t, Magic[:], data[len(data)-8:])

	// Footer offsets must point inside the file.
	footer, err := UnmarshalFooter(data[len(data)-FooterSize:])
	require.NoError(t, err)
	require.Less(t, int64(footer.IndexBlockOffset+uint64(footer.IndexBlockSize)), int64(len(data)))
	require.Less(t, int64(footer.MetaBlockOffset+uint64(footer.MetaBlockSize)), int64(len(data)))
	// Index comes before meta in our chosen layout.
	require.Less(t, footer.IndexBlockOffset, footer.MetaBlockOffset)
}
