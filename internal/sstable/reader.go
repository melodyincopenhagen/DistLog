package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/yuexishen/distlog/internal/iter"
	"github.com/yuexishen/distlog/internal/types"
)

// readerSource abstracts the byte-addressable storage backing a Reader.
// Today this is *os.File via fileSource. ADR-004 leaves room for an mmap
// implementation in Week 4 without changing Reader logic.
type readerSource interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Size() int64
	Close() error
}

type fileSource struct {
	f    *os.File
	size int64
}

func (s *fileSource) ReadAt(p []byte, off int64) (int, error) { return s.f.ReadAt(p, off) }
func (s *fileSource) Size() int64                             { return s.size }
func (s *fileSource) Close() error                            { return s.f.Close() }

// Reader provides random-access lookup and ordered iteration over a single
// SSTable file.
//
// Lifecycle: Open loads footer, meta block, and the entire index block into
// memory and verifies their CRCs. After Open succeeds the Reader is
// immutable: no field is ever mutated. Data blocks are read on demand via
// the source's ReadAt.
//
// Concurrency: Reader is safe for concurrent use by multiple goroutines.
// Iterators returned by Iterator() are NOT — each goroutine should obtain
// its own.
type Reader struct {
	src      readerSource
	path     string
	footer   Footer
	meta     MetaBlock
	indexEnt []IndexEntry
}

// Open opens an SSTable for reading. On any validation failure (bad magic,
// version mismatch, CRC error, truncated file) Open returns an error and
// no resources are leaked.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	src := &fileSource{f: f, size: info.Size()}
	r, err := openSource(src, path)
	if err != nil {
		_ = src.Close()
		return nil, err
	}
	return r, nil
}

func openSource(src readerSource, path string) (*Reader, error) {
	size := src.Size()
	if size < int64(FooterSize) {
		return nil, fmt.Errorf("sstable: file %s too small (%d < %d)", path, size, FooterSize)
	}

	footerBuf := make([]byte, FooterSize)
	if _, err := src.ReadAt(footerBuf, size-int64(FooterSize)); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	footer, err := UnmarshalFooter(footerBuf)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	// Sanity: block offsets/sizes must lie strictly inside the file, before
	// the footer.
	if int64(footer.IndexBlockOffset)+int64(footer.IndexBlockSize) > size-int64(FooterSize) {
		return nil, fmt.Errorf("sstable: index block extends past footer in %s", path)
	}
	if int64(footer.MetaBlockOffset)+int64(footer.MetaBlockSize) > size-int64(FooterSize) {
		return nil, fmt.Errorf("sstable: meta block extends past footer in %s", path)
	}

	metaBuf := make([]byte, footer.MetaBlockSize)
	if _, err := src.ReadAt(metaBuf, int64(footer.MetaBlockOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read meta: %w", err)
	}
	meta, err := UnmarshalMetaBlock(metaBuf)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	indexBuf := make([]byte, footer.IndexBlockSize)
	if _, err := src.ReadAt(indexBuf, int64(footer.IndexBlockOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	if int64(len(indexBuf)) < int64(indexBlockTrailerSize) {
		return nil, fmt.Errorf("sstable: %s: index block truncated", path)
	}
	body := indexBuf[:len(indexBuf)-indexBlockTrailerSize]
	trailer := indexBuf[len(indexBuf)-indexBlockTrailerSize:]
	entryCount := binary.LittleEndian.Uint32(trailer[0:4])
	wantCRC := binary.LittleEndian.Uint32(trailer[4:8])
	if crc(body) != wantCRC {
		return nil, fmt.Errorf("sstable: %s: %w: index", path, ErrBadCRC)
	}
	if int(entryCount) != len(body)/IndexEntrySize || len(body)%IndexEntrySize != 0 {
		return nil, fmt.Errorf("sstable: %s: index entry count mismatch (count=%d, bytes=%d)", path, entryCount, len(body))
	}
	if entryCount != meta.DataBlockCount {
		return nil, fmt.Errorf("sstable: %s: index/meta block-count disagree (%d vs %d)", path, entryCount, meta.DataBlockCount)
	}

	entries := make([]IndexEntry, entryCount)
	var prev types.DocID
	for i := uint32(0); i < entryCount; i++ {
		e, err := UnmarshalIndexEntry(body[i*IndexEntrySize:])
		if err != nil {
			return nil, fmt.Errorf("sstable: %s: index entry %d: %w", path, i, err)
		}
		if i > 0 && e.FirstDocID <= prev {
			return nil, fmt.Errorf("sstable: %s: index entries not strictly increasing at %d", path, i)
		}
		prev = e.FirstDocID
		entries[i] = e
	}

	return &Reader{
		src:      src,
		path:     path,
		footer:   footer,
		meta:     meta,
		indexEnt: entries,
	}, nil
}

// Close releases the underlying file. Calling any other method after Close
// is undefined behavior.
func (r *Reader) Close() error { return r.src.Close() }

// === Metadata accessors (O(1), pure memory) ===

func (r *Reader) Path() string                  { return r.path }
func (r *Reader) Meta() MetaBlock               { return r.meta }
func (r *Reader) MinDocID() types.DocID         { return r.meta.MinDocID }
func (r *Reader) MaxDocID() types.DocID         { return r.meta.MaxDocID }
func (r *Reader) MinTimestamp() types.Timestamp { return r.meta.MinTimestamp }
func (r *Reader) MaxTimestamp() types.Timestamp { return r.meta.MaxTimestamp }
func (r *Reader) RecordCount() uint64           { return r.meta.RecordCount }

// recordEntry is a decoded record from a data block, kept in memory for the
// duration of one block's use.
type recordEntry struct {
	docID  types.DocID
	record *types.LogRecord
}

// loadBlock reads, validates, and decodes the data block at indexEnt[idx].
//
// TODO(week4): cache decoded blocks in user space.
//
// The OS page cache covers the syscall, so re-reading the same block is
// fast at the kernel boundary. The dominant cost is JSON decoding (~µs per
// record), which this function repeats on every call. A small LRU keyed by
// blockOffset that holds []recordEntry will drop hot-block Get latency
// substantially. See ADR-004 for why this is deferred.
func (r *Reader) loadBlock(idx int) ([]recordEntry, error) {
	if idx < 0 || idx >= len(r.indexEnt) {
		return nil, fmt.Errorf("sstable: block index %d out of range", idx)
	}
	ent := r.indexEnt[idx]
	buf := make([]byte, ent.BlockSize)
	if _, err := r.src.ReadAt(buf, int64(ent.BlockOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read block %d: %w", idx, err)
	}
	if len(buf) < dataBlockTrailerSize {
		return nil, fmt.Errorf("sstable: block %d truncated", idx)
	}
	body := buf[:len(buf)-dataBlockTrailerSize]
	trailer := buf[len(buf)-dataBlockTrailerSize:]
	recordCount := binary.LittleEndian.Uint32(trailer[0:4])
	wantCRC := binary.LittleEndian.Uint32(trailer[4:8])
	if crc(body) != wantCRC {
		return nil, fmt.Errorf("sstable: block %d: %w", idx, ErrBadCRC)
	}

	out := make([]recordEntry, 0, recordCount)
	off := 0
	for i := uint32(0); i < recordCount; i++ {
		if off+recordHeaderSize > len(body) {
			return nil, fmt.Errorf("sstable: block %d: record %d header out of range", idx, i)
		}
		docID, ts, plen, err := decodeRecordHeader(body[off : off+recordHeaderSize])
		if err != nil {
			return nil, fmt.Errorf("sstable: block %d: record %d header: %w", idx, i, err)
		}
		off += recordHeaderSize
		if off+int(plen) > len(body) {
			return nil, fmt.Errorf("sstable: block %d: record %d payload out of range", idx, i)
		}
		payload := body[off : off+int(plen)]
		off += int(plen)
		rec, err := types.Unmarshal(payload)
		if err != nil {
			return nil, fmt.Errorf("sstable: block %d: record %d decode: %w", idx, i, err)
		}
		// The payload may not have round-tripped Timestamp through JSON if
		// the encoder ever changes; assert the header timestamp is the
		// source of truth and overwrite. Cheap insurance.
		rec.Timestamp = ts
		out = append(out, recordEntry{docID: docID, record: rec})
	}
	if off != len(body) {
		return nil, fmt.Errorf("sstable: block %d: %d trailing bytes after last record", idx, len(body)-off)
	}
	return out, nil
}

// findBlock returns the index of the data block that may contain docID, or
// -1 if docID is below the first block's first DocID. Uses binary search on
// indexEnt since each entry's FirstDocID is strictly increasing.
func (r *Reader) findBlock(docID types.DocID) int {
	// We want the largest i with indexEnt[i].FirstDocID <= docID.
	// sort.Search returns smallest i with predicate true; flip to find
	// the first i with FirstDocID > docID, then subtract one.
	n := len(r.indexEnt)
	hi := sort.Search(n, func(i int) bool { return r.indexEnt[i].FirstDocID > docID })
	return hi - 1
}

// Get looks up a record by docID.
//
// Return semantics (three cases — callers must distinguish):
//   - (record, true,  nil): found.
//   - (nil,    false, nil): docID is not present (out of range, or in a gap
//     between two records). Trustworthy only when err == nil.
//   - (nil,    false, err): block CRC failure, I/O error, decode error.
//     A CRC failure must NEVER be silently translated into "not found";
//     doing so would mask silent corruption.
func (r *Reader) Get(docID types.DocID) (*types.LogRecord, bool, error) {
	if docID < r.meta.MinDocID || docID > r.meta.MaxDocID {
		return nil, false, nil
	}
	idx := r.findBlock(docID)
	if idx < 0 {
		return nil, false, nil
	}
	records, err := r.loadBlock(idx)
	if err != nil {
		return nil, false, err
	}
	// Linear scan within the block. Block holds tens to a few hundred
	// records — cache-friendly and faster than a binary search at this size.
	for i := range records {
		if records[i].docID == docID {
			return records[i].record, true, nil
		}
		if records[i].docID > docID {
			return nil, false, nil
		}
	}
	return nil, false, nil
}

// Iterator returns a forward iterator over all records in this SSTable.
//
// Error handling: on block CRC failure or decode error mid-iteration,
// Next() returns false and Err() returns the error. Iteration terminates
// at the failing block; subsequent blocks are NOT read. Skip-on-corruption
// is intentionally NOT supported — corrupt iteration must surface to the
// caller (especially compaction, where silent corruption would propagate
// into new SSTables).
//
// Iterator is NOT safe for concurrent use. Reader is.
func (r *Reader) Iterator() iter.Iterator {
	return &sstableIterator{reader: r, blockIdx: -1}
}

type sstableIterator struct {
	reader   *Reader
	blockIdx int
	records  []recordEntry
	pos      int
	err      error
	closed   bool
}

func (it *sstableIterator) Next() bool {
	if it.err != nil || it.closed {
		return false
	}
	if it.blockIdx >= 0 {
		it.pos++
		if it.pos < len(it.records) {
			return true
		}
	}
	for {
		it.blockIdx++
		if it.blockIdx >= len(it.reader.indexEnt) {
			return false
		}
		recs, err := it.reader.loadBlock(it.blockIdx)
		if err != nil {
			it.err = err
			return false
		}
		if len(recs) == 0 {
			// Empty block shouldn't happen (writer rejects), but skip
			// defensively.
			continue
		}
		it.records = recs
		it.pos = 0
		return true
	}
}

func (it *sstableIterator) DocID() types.DocID {
	return it.records[it.pos].docID
}

func (it *sstableIterator) Record() *types.LogRecord {
	return it.records[it.pos].record
}

func (it *sstableIterator) Err() error { return it.err }

func (it *sstableIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.records = nil
	return nil
}

// Compile-time assertion that the iterator satisfies the shared interface.
var _ iter.Iterator = (*sstableIterator)(nil)
