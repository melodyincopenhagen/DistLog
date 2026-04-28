package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalRoundTrip(t *testing.T) {
	in := &LogRecord{
		Timestamp: Timestamp(1_700_000_000_000_000_000),
		TenantID:  "tenant-7",
		Source:    "host-42",
		Message:   "request completed",
		Fields:    map[string]string{"level": "info", "trace_id": "abc123"},
	}
	b, err := Marshal(in)
	require.NoError(t, err)

	out, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestUnmarshalEmpty(t *testing.T) {
	in := &LogRecord{}
	b, err := Marshal(in)
	require.NoError(t, err)

	out, err := Unmarshal(b)
	require.NoError(t, err)
	require.Equal(t, in.Timestamp, out.Timestamp)
	require.Equal(t, in.Message, out.Message)
}

func TestUnmarshalGarbage(t *testing.T) {
	_, err := Unmarshal([]byte("not json"))
	require.Error(t, err)
}
