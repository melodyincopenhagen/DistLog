// Package sstable implements the on-disk Sorted String Table format used by
// the LSM-tree. SSTables are immutable: once a writer closes a file, its
// contents and layout are frozen.
//
// File layout (v1):
//
//	+----------------------+
//	| Data Block 1         |
//	| Data Block 2         |
//	| ...                  |
//	| Data Block N         |
//	+----------------------+
//	| Index Block          |   (firstDocID, blockOffset, blockSize) per data block
//	+----------------------+
//	| Meta Block           |   min/max docID, min/max ts, counts, ...
//	+----------------------+
//	| Footer (48 B)        |   offsets of index/meta blocks; magic at very end
//	+----------------------+
//
// All multi-byte integers are little-endian (see docs/DECISIONS.md ADR-001).
// All CRCs use crc32.IEEE (matches WAL; see ADR-002).
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/yuexishen/distlog/internal/types"
)

// Magic is the 8-byte tag at the very end of every SSTable file. Reading
// these bytes successfully implies the footer was not truncated.
var Magic = [8]byte{'D', 'L', 'S', 'S', 'T', 'B', 'L', 0x00}

const (
	// FormatVersion is bumped when the on-disk format changes
	// incompatibly. Readers must reject files with a newer version.
	FormatVersion uint32 = 1

	// FooterSize is the fixed footer length in bytes.
	FooterSize = 48

	// IndexEntrySize is the fixed size of one entry in the index block.
	IndexEntrySize = 24

	// DefaultBlockSize is the target size of one data block. Actual blocks
	// may exceed this slightly because we flush after the record that
	// crosses the threshold, not before.
	DefaultBlockSize = 16 * 1024

	// dataBlockTrailerSize is the per-block tail: record_count(4) + crc32(4).
	dataBlockTrailerSize = 8

	// indexBlockTrailerSize is the per-block tail: entry_count(4) + crc32(4).
	indexBlockTrailerSize = 8

	// MetaBlockSize is fixed: 8+8+8+8+8+4+8+32+4 = 88 bytes.
	MetaBlockSize = 88
)

var (
	ErrBadMagic       = errors.New("sstable: bad magic")
	ErrBadVersion     = errors.New("sstable: unsupported format version")
	ErrShortBuffer    = errors.New("sstable: buffer too short")
	ErrBadCRC         = errors.New("sstable: crc mismatch")
	ErrMalformedBlock = errors.New("sstable: malformed block")
)

var crcTable = crc32.MakeTable(crc32.IEEE)

func crc(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// IndexEntry is one entry in the index block, pointing to a data block.
//
// Reserved is currently unused (always 0) and kept for forward-compatible
// extensions such as per-block min/max timestamp.
type IndexEntry struct {
	FirstDocID  types.DocID
	BlockOffset uint64
	BlockSize   uint32
	Reserved    uint32
}

func (e IndexEntry) Marshal() [IndexEntrySize]byte {
	var b [IndexEntrySize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(e.FirstDocID))
	binary.LittleEndian.PutUint64(b[8:16], e.BlockOffset)
	binary.LittleEndian.PutUint32(b[16:20], e.BlockSize)
	binary.LittleEndian.PutUint32(b[20:24], e.Reserved)
	return b
}

func UnmarshalIndexEntry(b []byte) (IndexEntry, error) {
	if len(b) < IndexEntrySize {
		return IndexEntry{}, ErrShortBuffer
	}
	return IndexEntry{
		FirstDocID:  types.DocID(binary.LittleEndian.Uint64(b[0:8])),
		BlockOffset: binary.LittleEndian.Uint64(b[8:16]),
		BlockSize:   binary.LittleEndian.Uint32(b[16:20]),
		Reserved:    binary.LittleEndian.Uint32(b[20:24]),
	}, nil
}

// MetaBlock holds whole-file metadata used for shard pruning and diagnostics.
//
// The 32-byte Reserved field is intentional headroom for fields we know we'll
// want later (bloom filter offset/size, compression codec id, tenant id).
// New fields steal bytes from Reserved without bumping FormatVersion.
type MetaBlock struct {
	MinDocID       types.DocID
	MaxDocID       types.DocID
	MinTimestamp   types.Timestamp
	MaxTimestamp   types.Timestamp
	RecordCount    uint64
	DataBlockCount uint32
	CreatedAtUnix  int64
	Reserved       [32]byte
	// CRC is computed over the preceding 84 bytes; not user-set.
}

func (m *MetaBlock) Marshal() [MetaBlockSize]byte {
	var b [MetaBlockSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(m.MinDocID))
	binary.LittleEndian.PutUint64(b[8:16], uint64(m.MaxDocID))
	binary.LittleEndian.PutUint64(b[16:24], uint64(m.MinTimestamp))
	binary.LittleEndian.PutUint64(b[24:32], uint64(m.MaxTimestamp))
	binary.LittleEndian.PutUint64(b[32:40], m.RecordCount)
	binary.LittleEndian.PutUint32(b[40:44], m.DataBlockCount)
	binary.LittleEndian.PutUint64(b[44:52], uint64(m.CreatedAtUnix))
	copy(b[52:84], m.Reserved[:])
	binary.LittleEndian.PutUint32(b[84:88], crc(b[0:84]))
	return b
}

func UnmarshalMetaBlock(b []byte) (MetaBlock, error) {
	if len(b) < MetaBlockSize {
		return MetaBlock{}, ErrShortBuffer
	}
	wantCRC := binary.LittleEndian.Uint32(b[84:88])
	if crc(b[0:84]) != wantCRC {
		return MetaBlock{}, fmt.Errorf("%w: meta block", ErrBadCRC)
	}
	var m MetaBlock
	m.MinDocID = types.DocID(binary.LittleEndian.Uint64(b[0:8]))
	m.MaxDocID = types.DocID(binary.LittleEndian.Uint64(b[8:16]))
	m.MinTimestamp = types.Timestamp(binary.LittleEndian.Uint64(b[16:24]))
	m.MaxTimestamp = types.Timestamp(binary.LittleEndian.Uint64(b[24:32]))
	m.RecordCount = binary.LittleEndian.Uint64(b[32:40])
	m.DataBlockCount = binary.LittleEndian.Uint32(b[40:44])
	m.CreatedAtUnix = int64(binary.LittleEndian.Uint64(b[44:52]))
	copy(m.Reserved[:], b[52:84])
	return m, nil
}

// Footer points to the index and meta blocks. Located at the very end of the
// file. The magic occupies the last 8 bytes so a successful magic read implies
// the footer is fully present.
//
// Layout (48 bytes):
//
//	[0:4)    version           uint32
//	[4:8)    flags             uint32
//	[8:16)   indexBlockOffset  uint64
//	[16:20)  indexBlockSize    uint32
//	[20:28)  metaBlockOffset   uint64
//	[28:32)  metaBlockSize     uint32
//	[32:36)  footerCRC         uint32  (covers bytes [0:32))
//	[36:40)  reserved          uint32
//	[40:48)  magic             8 bytes
type Footer struct {
	Version          uint32
	Flags            uint32
	IndexBlockOffset uint64
	IndexBlockSize   uint32
	MetaBlockOffset  uint64
	MetaBlockSize    uint32
}

func (f *Footer) Marshal() []byte {
	b := make([]byte, FooterSize)
	binary.LittleEndian.PutUint32(b[0:4], f.Version)
	binary.LittleEndian.PutUint32(b[4:8], f.Flags)
	binary.LittleEndian.PutUint64(b[8:16], f.IndexBlockOffset)
	binary.LittleEndian.PutUint32(b[16:20], f.IndexBlockSize)
	binary.LittleEndian.PutUint64(b[20:28], f.MetaBlockOffset)
	binary.LittleEndian.PutUint32(b[28:32], f.MetaBlockSize)
	binary.LittleEndian.PutUint32(b[32:36], crc(b[0:32]))
	// b[36:40] reserved, left zero
	copy(b[40:48], Magic[:])
	return b
}

// UnmarshalFooter decodes the 48-byte footer. Returns ErrBadMagic if the
// trailing magic does not match (treat as not-an-sstable), ErrBadVersion if
// the format version is unsupported, ErrBadCRC if the footer is corrupt.
func UnmarshalFooter(b []byte) (Footer, error) {
	if len(b) < FooterSize {
		return Footer{}, ErrShortBuffer
	}
	var magic [8]byte
	copy(magic[:], b[40:48])
	if magic != Magic {
		return Footer{}, ErrBadMagic
	}
	wantCRC := binary.LittleEndian.Uint32(b[32:36])
	if crc(b[0:32]) != wantCRC {
		return Footer{}, fmt.Errorf("%w: footer", ErrBadCRC)
	}
	f := Footer{
		Version:          binary.LittleEndian.Uint32(b[0:4]),
		Flags:            binary.LittleEndian.Uint32(b[4:8]),
		IndexBlockOffset: binary.LittleEndian.Uint64(b[8:16]),
		IndexBlockSize:   binary.LittleEndian.Uint32(b[16:20]),
		MetaBlockOffset:  binary.LittleEndian.Uint64(b[20:28]),
		MetaBlockSize:    binary.LittleEndian.Uint32(b[28:32]),
	}
	if f.Version != FormatVersion {
		return Footer{}, fmt.Errorf("%w: got %d, want %d", ErrBadVersion, f.Version, FormatVersion)
	}
	return f, nil
}

// recordHeaderSize is the fixed prefix of one record within a data block:
// docID(8) + timestamp(8) + payload_len(4).
const recordHeaderSize = 20

// encodeRecordHeader writes the fixed-size record header into dst. dst must
// have at least recordHeaderSize bytes.
func encodeRecordHeader(dst []byte, docID types.DocID, ts types.Timestamp, payloadLen uint32) {
	binary.LittleEndian.PutUint64(dst[0:8], uint64(docID))
	binary.LittleEndian.PutUint64(dst[8:16], uint64(ts))
	binary.LittleEndian.PutUint32(dst[16:20], payloadLen)
}

// decodeRecordHeader reads the fixed prefix.
func decodeRecordHeader(src []byte) (docID types.DocID, ts types.Timestamp, payloadLen uint32, err error) {
	if len(src) < recordHeaderSize {
		return 0, 0, 0, ErrShortBuffer
	}
	docID = types.DocID(binary.LittleEndian.Uint64(src[0:8]))
	ts = types.Timestamp(binary.LittleEndian.Uint64(src[8:16]))
	payloadLen = binary.LittleEndian.Uint32(src[16:20])
	return
}
