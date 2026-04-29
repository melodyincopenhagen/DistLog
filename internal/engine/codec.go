package engine

import (
	"encoding/binary"
	"errors"

	"github.com/yuexishen/distlog/internal/types"
)

// WAL payload framing (Engine layer):
//
//   +------------+--------------------------+
//   | DocID (8B) | Marshaled LogRecord ...  |
//   +------------+--------------------------+
//
// The WAL itself treats the payload as opaque bytes; this framing lives in
// the engine layer because the WAL is a generic durable append log and
// should not know about DocIDs. Future framing changes (e.g. a leading
// schema-version byte) belong here as well — keeping all WAL framing in one
// file means the change is a single edit.

const walFramingDocIDSize = 8

var errShortWALPayload = errors.New("engine: wal payload shorter than docID framing")

func encodeWALPayload(docID types.DocID, recordBytes []byte) []byte {
	out := make([]byte, walFramingDocIDSize+len(recordBytes))
	binary.LittleEndian.PutUint64(out[0:walFramingDocIDSize], uint64(docID))
	copy(out[walFramingDocIDSize:], recordBytes)
	return out
}

func decodeWALPayload(b []byte) (types.DocID, []byte, error) {
	if len(b) < walFramingDocIDSize {
		return 0, nil, errShortWALPayload
	}
	docID := types.DocID(binary.LittleEndian.Uint64(b[0:walFramingDocIDSize]))
	return docID, b[walFramingDocIDSize:], nil
}
