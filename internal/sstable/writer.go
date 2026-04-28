package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yuexishen/distlog/internal/types"
)

// Writer builds a single SSTable file. Records are added with strictly
// increasing DocIDs (the natural order produced by a MemTable iterator).
// On Close, the writer flushes the trailing data block, writes the index
// block, meta block, and footer, fsyncs the file, and atomically renames
// the temporary file to its final path.
//
// Writer is NOT safe for concurrent use. The flush pipeline is single-
// threaded by design.
type Writer struct {
	path    string
	tmpPath string
	file    *os.File
	bufW    *bufio.Writer

	// Pending data block being assembled in memory.
	curBlock      bytes.Buffer
	curRecords    uint32
	curFirstDocID types.DocID

	// Accumulated state.
	indexEntries   []IndexEntry
	minDocID       types.DocID
	maxDocID       types.DocID
	minTS          types.Timestamp
	maxTS          types.Timestamp
	recordCount    uint64
	dataBlockCount uint32
	lastDocID      types.DocID
	hasRecord      bool

	// File-level offset tracking. Equals total bytes successfully written
	// to bufW (which buffers above the underlying file). Used to record
	// block offsets in the index.
	offset int64

	closed   bool
	aborted  bool
	finished bool // Close completed successfully

	blockTargetSize int
}

// NewWriter creates a Writer that will produce path on successful Close.
// During writing, content goes to path + ".tmp"; the rename is the atomic
// commit point.
func NewWriter(path string) (*Writer, error) {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", tmpPath, err)
	}
	return &Writer{
		path:            path,
		tmpPath:         tmpPath,
		file:            f,
		bufW:            bufio.NewWriterSize(f, 64*1024),
		blockTargetSize: DefaultBlockSize,
	}, nil
}

// Add appends one record. docID must be strictly greater than the previously
// added docID.
func (w *Writer) Add(docID types.DocID, record *types.LogRecord) error {
	if w.closed {
		return errors.New("sstable: writer is closed")
	}
	if w.hasRecord && docID <= w.lastDocID {
		return fmt.Errorf("sstable: docIDs must be strictly increasing, got %d after %d", docID, w.lastDocID)
	}

	payload, err := types.Marshal(record)
	if err != nil {
		return fmt.Errorf("sstable: marshal record %d: %w", docID, err)
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("sstable: payload of record %d exceeds uint32", docID)
	}

	if w.curRecords == 0 {
		w.curFirstDocID = docID
	}

	var hdr [recordHeaderSize]byte
	encodeRecordHeader(hdr[:], docID, record.Timestamp, uint32(len(payload)))
	w.curBlock.Write(hdr[:])
	w.curBlock.Write(payload)
	w.curRecords++

	// Update aggregate state.
	if !w.hasRecord {
		w.minDocID = docID
		w.maxDocID = docID
		w.minTS = record.Timestamp
		w.maxTS = record.Timestamp
		w.hasRecord = true
	} else {
		w.maxDocID = docID
		if record.Timestamp < w.minTS {
			w.minTS = record.Timestamp
		}
		if record.Timestamp > w.maxTS {
			w.maxTS = record.Timestamp
		}
	}
	w.lastDocID = docID
	w.recordCount++

	if w.curBlock.Len() >= w.blockTargetSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// flushBlock writes the in-memory data block to the file with a trailer
// (record_count + crc32 over the records bytes).
func (w *Writer) flushBlock() error {
	if w.curRecords == 0 {
		return nil
	}
	body := w.curBlock.Bytes()

	var trailer [dataBlockTrailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:4], w.curRecords)
	binary.LittleEndian.PutUint32(trailer[4:8], crc(body))

	blockOffset := uint64(w.offset)
	if _, err := w.bufW.Write(body); err != nil {
		return fmt.Errorf("sstable: write block body: %w", err)
	}
	if _, err := w.bufW.Write(trailer[:]); err != nil {
		return fmt.Errorf("sstable: write block trailer: %w", err)
	}
	blockSize := uint32(len(body) + dataBlockTrailerSize)
	w.offset += int64(blockSize)

	w.indexEntries = append(w.indexEntries, IndexEntry{
		FirstDocID:  w.curFirstDocID,
		BlockOffset: blockOffset,
		BlockSize:   blockSize,
	})
	w.dataBlockCount++

	w.curBlock.Reset()
	w.curRecords = 0
	w.curFirstDocID = 0
	return nil
}

// writeIndexBlock writes all accumulated index entries followed by the
// per-block trailer (entry_count + crc32).
func (w *Writer) writeIndexBlock() (offset uint64, size uint32, err error) {
	offset = uint64(w.offset)

	body := make([]byte, 0, len(w.indexEntries)*IndexEntrySize)
	for _, e := range w.indexEntries {
		b := e.Marshal()
		body = append(body, b[:]...)
	}

	var trailer [indexBlockTrailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(w.indexEntries)))
	binary.LittleEndian.PutUint32(trailer[4:8], crc(body))

	if _, err = w.bufW.Write(body); err != nil {
		return 0, 0, fmt.Errorf("sstable: write index block: %w", err)
	}
	if _, err = w.bufW.Write(trailer[:]); err != nil {
		return 0, 0, fmt.Errorf("sstable: write index trailer: %w", err)
	}
	size = uint32(len(body) + indexBlockTrailerSize)
	w.offset += int64(size)
	return offset, size, nil
}

func (w *Writer) writeMetaBlock() (offset uint64, size uint32, err error) {
	offset = uint64(w.offset)
	m := MetaBlock{
		MinDocID:       w.minDocID,
		MaxDocID:       w.maxDocID,
		MinTimestamp:   w.minTS,
		MaxTimestamp:   w.maxTS,
		RecordCount:    w.recordCount,
		DataBlockCount: w.dataBlockCount,
		CreatedAtUnix:  time.Now().Unix(),
	}
	b := m.Marshal()
	if _, err = w.bufW.Write(b[:]); err != nil {
		return 0, 0, fmt.Errorf("sstable: write meta block: %w", err)
	}
	w.offset += int64(MetaBlockSize)
	return offset, MetaBlockSize, nil
}

func (w *Writer) writeFooter(idxOff uint64, idxSize uint32, metaOff uint64, metaSize uint32) error {
	f := Footer{
		Version:          FormatVersion,
		IndexBlockOffset: idxOff,
		IndexBlockSize:   idxSize,
		MetaBlockOffset:  metaOff,
		MetaBlockSize:    metaSize,
	}
	if _, err := w.bufW.Write(f.Marshal()); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}
	w.offset += FooterSize
	return nil
}

// Close finalizes the file. Multiple calls are safe; only the first does work.
// On any failure during Close, the temporary file is removed and the error is
// returned — there will be no partial .sst at the final path.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if !w.hasRecord {
		// Empty SSTables are not useful and complicate downstream code.
		// Reject explicitly rather than producing a degenerate file.
		_ = w.cleanupOnError()
		return errors.New("sstable: refusing to close writer with zero records")
	}

	if err := w.flushBlock(); err != nil {
		_ = w.cleanupOnError()
		return err
	}
	idxOff, idxSize, err := w.writeIndexBlock()
	if err != nil {
		_ = w.cleanupOnError()
		return err
	}
	metaOff, metaSize, err := w.writeMetaBlock()
	if err != nil {
		_ = w.cleanupOnError()
		return err
	}
	if err := w.writeFooter(idxOff, idxSize, metaOff, metaSize); err != nil {
		_ = w.cleanupOnError()
		return err
	}
	if err := w.bufW.Flush(); err != nil {
		_ = w.cleanupOnError()
		return fmt.Errorf("sstable: flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		_ = w.cleanupOnError()
		return fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("sstable: close: %w", err)
	}
	w.file = nil

	if err := os.Rename(w.tmpPath, w.path); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("sstable: rename: %w", err)
	}
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sstable: sync dir: %w", err)
	}
	w.finished = true
	return nil
}

func (w *Writer) cleanupOnError() error {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	return os.Remove(w.tmpPath)
}

// Abort discards the writer without producing a file. Safe to call after
// Close (no-op in that case). Safe to call multiple times.
func (w *Writer) Abort() error {
	if w.finished {
		return nil // already committed; nothing to abort
	}
	if w.aborted {
		return nil
	}
	w.aborted = true
	w.closed = true
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if err := os.Remove(w.tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// syncDir fsyncs a directory so that a previous rename of a child entry is
// durable. On Linux this is required for crash safety; on macOS development
// environments it is best-effort (true durability requires F_FULLFSYNC via
// fcntl, not used here).
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
