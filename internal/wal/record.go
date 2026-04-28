package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// On-disk record layout:
//
//   +--------+--------+------+------------------+
//   | CRC32  | Length | Type |     Payload      |
//   |  4 B   |  4 B   | 1 B  |   Length bytes   |
//   +--------+--------+------+------------------+
//
// CRC32 (IEEE) covers Length, Type, and Payload. Length is the payload size,
// not including the header. All integers are little-endian.

const (
	headerSize    = 9
	maxPayload    = 64 << 20 // 64 MiB sanity bound
	recordVersion = 1
)

// RecordType discriminates payload semantics. New types append; never reuse.
type RecordType uint8

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

var (
	ErrCorruptRecord = errors.New("wal: corrupt record")
	ErrShortRead     = errors.New("wal: short read")
	ErrPayloadTooBig = errors.New("wal: payload exceeds max size")
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// encodeRecord writes a single record to dst. Returns total bytes written.
func encodeRecord(dst []byte, typ RecordType, payload []byte) (int, error) {
	if len(payload) > maxPayload {
		return 0, ErrPayloadTooBig
	}
	if len(dst) < headerSize+len(payload) {
		return 0, io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint32(dst[4:8], uint32(len(payload)))
	dst[8] = byte(typ)
	copy(dst[headerSize:], payload)

	crc := crc32.Checksum(dst[4:headerSize+len(payload)], crcTable)
	binary.LittleEndian.PutUint32(dst[0:4], crc)
	return headerSize + len(payload), nil
}

// decodeRecord reads one record from r. On clean EOF returns io.EOF. On a
// torn tail (truncated header or payload) returns io.ErrUnexpectedEOF so the
// caller can decide whether to treat it as end-of-log.
func decodeRecord(r io.Reader) (RecordType, []byte, error) {
	var hdr [headerSize]byte
	n, err := io.ReadFull(r, hdr[:])
	if err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		if n > 0 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}

	wantCRC := binary.LittleEndian.Uint32(hdr[0:4])
	length := binary.LittleEndian.Uint32(hdr[4:8])
	typ := RecordType(hdr[8])

	if length > maxPayload {
		return 0, nil, fmt.Errorf("%w: length %d exceeds max", ErrCorruptRecord, length)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}

	gotCRC := crc32.Update(0, crcTable, hdr[4:])
	gotCRC = crc32.Update(gotCRC, crcTable, payload)
	if gotCRC != wantCRC {
		return 0, nil, fmt.Errorf("%w: crc mismatch", ErrCorruptRecord)
	}
	return typ, payload, nil
}
