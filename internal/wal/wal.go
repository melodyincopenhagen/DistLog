// Package wal implements a write-ahead log for the LSM-tree.
//
// The log is organized as a sequence of immutable, append-only segment files
// named "wal-000001.log", "wal-000002.log", ... Exactly one segment is
// active for writes at a time; the others are sealed and read-only. The
// storage engine seals the active segment when it freezes a MemTable and
// opens a fresh one for the next active MemTable. Sealed segments are
// deleted after the corresponding MemTable has been flushed to an SSTable.
//
// The first version uses synchronous fsync on every Append: when Append
// returns nil, the record is durable. The Writer interface is designed so
// a future group-commit implementation can be substituted without changing
// callers — Append is already a per-call boundary that returns only after
// durability is guaranteed.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LSN (log sequence number) is the byte offset of a record's first header
// byte within its segment file. Monotonic within a single segment; resets
// at zero in a new segment. Combined with SegmentID it uniquely identifies
// a record across the whole WAL.
type LSN uint64

// SegmentID identifies a WAL segment file. Strictly increasing across the
// lifetime of a WAL directory.
type SegmentID uint64

// Writer appends records to the active segment durably.
//
// Implementations must guarantee: when Append returns nil, the record and
// all preceding records are durable on disk (fsync'd). This contract is
// what allows a future group-commit implementation to be a drop-in
// replacement — callers depend only on the post-return durability guarantee,
// not on how it is achieved internally.
type Writer interface {
	// Append writes one record and returns its LSN once durable.
	Append(ctx context.Context, typ RecordType, payload []byte) (LSN, error)
	// Sync forces any buffered data to disk. For the synchronous
	// implementation this is a no-op (every Append already syncs); future
	// implementations may buffer.
	Sync() error
	// SegmentID returns the segment this writer is appending to.
	SegmentID() SegmentID
}

// segmentWriter is the synchronous-fsync implementation of Writer for a
// single open segment file. Owned by Manager — callers obtain it via
// Manager.Active().
//
// Concurrency: Append is safe for concurrent callers; calls are serialized
// by mu.
type segmentWriter struct {
	mu     sync.Mutex
	id     SegmentID
	path   string
	f      *os.File
	offset int64
	buf    []byte
	closed bool
}

func openSegmentWriter(dir string, id SegmentID) (*segmentWriter, error) {
	path := segmentPath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: seek %s: %w", path, err)
	}
	return &segmentWriter{
		id:     id,
		path:   path,
		f:      f,
		offset: offset,
		buf:    make([]byte, headerSize+4096),
	}, nil
}

func (w *segmentWriter) SegmentID() SegmentID { return w.id }

func (w *segmentWriter) Append(ctx context.Context, typ RecordType, payload []byte) (LSN, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("wal: writer is closed")
	}

	need := headerSize + len(payload)
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	}
	w.buf = w.buf[:need]

	if _, err := encodeRecord(w.buf, typ, payload); err != nil {
		return 0, err
	}

	lsn := LSN(w.offset)
	if _, err := w.f.Write(w.buf); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}
	w.offset += int64(need)
	return lsn, nil
}

func (w *segmentWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.f.Sync()
}

// close releases the file handle. After close, Append fails. Idempotent.
func (w *segmentWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// SealedSegment is the read-only metadata view of a closed WAL segment. It
// does not hold an open file descriptor; Replay opens the file on demand.
//
// SealedSegment is safe for concurrent use; methods are stateless.
type SealedSegment struct {
	id   SegmentID
	path string
}

func (s *SealedSegment) ID() SegmentID { return s.id }
func (s *SealedSegment) Path() string  { return s.path }

// Replay reads every record in the segment in order and invokes fn for each.
// If a record is corrupt or truncated (a torn tail from a crash mid-write),
// Replay stops at that point and returns nil — preceding records are the
// recovered state. Hard I/O errors are returned.
func (s *SealedSegment) Replay(fn func(Record) error) error {
	return replayPath(s.path, fn)
}

// Delete removes the segment file. Idempotent: missing file is not an
// error. The caller is responsible for fsync'ing the directory afterward
// if durability of the deletion matters (typically batched at the engine
// level).
func (s *SealedSegment) Delete() error {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("wal: delete segment %d: %w", s.id, err)
	}
	return nil
}

// Manager owns a WAL directory: the single active segment writer and the
// list of sealed segments. The engine layer is the sole user of Manager.
//
// Concurrency: Manager methods that mutate state (Seal, etc.) are
// serialized internally by mu. The Writer returned by Active() is itself
// concurrency-safe.
type Manager struct {
	dir string

	mu      sync.Mutex
	active  *segmentWriter
	sealed  []*SealedSegment // ordered by SegmentID ascending
	nextID  SegmentID
	closed  bool
}

// OpenManager opens (or creates) a WAL directory and prepares it for
// writing. On open it scans the directory for existing segment files,
// re-creates SealedSegment views for all but the newest, and reopens the
// newest as the active writer.
//
// If the directory contains no segments, a fresh segment with id=1 is
// created as the active writer.
func OpenManager(dir string) (*Manager, error) {
	if dir == "" {
		return nil, errors.New("wal: dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	ids, err := listSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	m := &Manager{dir: dir}
	if len(ids) == 0 {
		// Fresh directory: open segment 1 as active.
		w, err := openSegmentWriter(dir, 1)
		if err != nil {
			return nil, err
		}
		m.active = w
		m.nextID = 2
		return m, nil
	}

	// All but the last become sealed; the last is reopened as active. This
	// matches the "Close did not flush active" contract: on restart the
	// previous active segment continues being appended to. For the engine
	// this never happens in practice (the engine always seals before
	// reopening), but supporting it keeps Manager useful as a standalone
	// component and keeps the semantics simple: "the newest segment is
	// always the active one."
	for _, id := range ids[:len(ids)-1] {
		m.sealed = append(m.sealed, &SealedSegment{id: id, path: segmentPath(dir, id)})
	}
	last := ids[len(ids)-1]
	w, err := openSegmentWriter(dir, last)
	if err != nil {
		return nil, err
	}
	m.active = w
	m.nextID = last + 1
	return m, nil
}

// Active returns the writer for the currently active segment. The returned
// value is stable until Seal is called; after Seal, callers must request
// Active() again to get the new writer.
func (m *Manager) Active() Writer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// Sealed returns a snapshot of currently sealed segments in ascending
// SegmentID order.
func (m *Manager) Sealed() []*SealedSegment {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*SealedSegment, len(m.sealed))
	copy(out, m.sealed)
	return out
}

// Seal closes the current active segment and opens a fresh one. Returns
// the SealedSegment view of the just-sealed segment. After Seal, Active()
// returns the new writer.
func (m *Manager) Seal() (*SealedSegment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("wal: manager is closed")
	}
	prev := m.active
	if err := prev.close(); err != nil {
		return nil, fmt.Errorf("wal: seal: close prev: %w", err)
	}
	sealed := &SealedSegment{id: prev.id, path: prev.path}

	next, err := openSegmentWriter(m.dir, m.nextID)
	if err != nil {
		// Recovery: the prev segment is on disk and sealed; if we cannot
		// open the next one, surface the error. The previously-sealed
		// segment is intentionally NOT added to m.sealed because callers
		// rely on Sealed() being the source of truth. Add it now anyway
		// so the caller can still observe it; the engine will treat the
		// open failure as fatal.
		m.sealed = append(m.sealed, sealed)
		m.active = nil
		return sealed, fmt.Errorf("wal: seal: open next: %w", err)
	}
	m.sealed = append(m.sealed, sealed)
	m.active = next
	m.nextID++
	return sealed, nil
}

// Close releases the active segment's file handle. Sealed segments are
// untouched (they have no open handles). Idempotent.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.active != nil {
		err := m.active.close()
		m.active = nil
		return err
	}
	return nil
}

// Dir returns the WAL directory path.
func (m *Manager) Dir() string { return m.dir }

// === Helpers ===

const segmentPrefix = "wal-"
const segmentSuffix = ".log"
const segmentNumberWidth = 6

func segmentName(id SegmentID) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentNumberWidth, id, segmentSuffix)
}

func segmentPath(dir string, id SegmentID) string {
	return filepath.Join(dir, segmentName(id))
}

func parseSegmentName(name string) (SegmentID, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	mid := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	id, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return SegmentID(id), true
}

func listSegmentIDs(dir string) ([]SegmentID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir %s: %w", dir, err)
	}
	var ids []SegmentID
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseSegmentName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Record is a decoded WAL record returned by Replay.
type Record struct {
	SegmentID SegmentID
	LSN       LSN
	Type      RecordType
	Payload   []byte
}

func replayPath(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("wal: open %s: %w", path, err)
	}
	defer f.Close()

	// Derive segment id from filename for the Record header. If parsing
	// fails the record is still useful (LSN within file), so default to 0.
	id, _ := parseSegmentName(filepath.Base(path))

	var offset int64
	for {
		typ, payload, err := decodeRecord(f)
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrCorruptRecord) {
			// Torn tail from a crash mid-write. Stop here; preceding
			// records are valid.
			return nil
		}
		if err != nil {
			return fmt.Errorf("wal: replay at offset %d: %w", offset, err)
		}
		rec := Record{
			SegmentID: id,
			LSN:       LSN(offset),
			Type:      typ,
			Payload:   payload,
		}
		if err := fn(rec); err != nil {
			return err
		}
		offset += int64(headerSize + len(payload))
	}
}
