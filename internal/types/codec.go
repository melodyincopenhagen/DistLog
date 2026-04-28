package types

import "encoding/json"

// Marshal serializes a LogRecord to bytes. The format is currently JSON; it
// will be replaced by Protocol Buffers around Day 6 once the .proto schema is
// settled. All callers must go through this function so that swap is a single
// point of change.
//
// Marshal is the canonical record encoding used by both the WAL payload and
// the SSTable record payload. Keeping them identical means flush does not
// re-encode.
func Marshal(r *LogRecord) ([]byte, error) {
	return json.Marshal(r)
}

// Unmarshal is the inverse of Marshal.
func Unmarshal(b []byte) (*LogRecord, error) {
	var r LogRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
