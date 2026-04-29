package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func TestWALPayloadRoundTrip(t *testing.T) {
	cases := []struct {
		docID   types.DocID
		payload []byte
	}{
		{1, []byte("hello")},
		{0xCAFEBABEDEADBEEF, []byte("")},
		{0xFFFFFFFFFFFFFFFF, []byte{0x00, 0x01, 0xFF}},
	}
	for _, c := range cases {
		framed := encodeWALPayload(c.docID, c.payload)
		gotID, gotPayload, err := decodeWALPayload(framed)
		require.NoError(t, err)
		require.Equal(t, c.docID, gotID)
		require.Equal(t, c.payload, gotPayload)
	}
}

func TestDecodeWALPayloadShort(t *testing.T) {
	_, _, err := decodeWALPayload([]byte{0x01, 0x02})
	require.ErrorIs(t, err, errShortWALPayload)
}
