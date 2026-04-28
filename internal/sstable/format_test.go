package sstable

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func TestIndexEntryRoundTrip(t *testing.T) {
	in := IndexEntry{
		FirstDocID:  types.DocID(0xCAFEBABE_DEADBEEF),
		BlockOffset: 0x0102030405060708,
		BlockSize:   0x11223344,
		Reserved:    0,
	}
	b := in.Marshal()
	out, err := UnmarshalIndexEntry(b[:])
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestIndexEntryShortBuffer(t *testing.T) {
	_, err := UnmarshalIndexEntry(make([]byte, IndexEntrySize-1))
	require.ErrorIs(t, err, ErrShortBuffer)
}

func TestMetaBlockRoundTrip(t *testing.T) {
	in := MetaBlock{
		MinDocID:       1,
		MaxDocID:       1_000_000,
		MinTimestamp:   types.Timestamp(1_700_000_000_000_000_000),
		MaxTimestamp:   types.Timestamp(1_800_000_000_000_000_000),
		RecordCount:    1_000_000,
		DataBlockCount: 64,
		CreatedAtUnix:  1_700_000_000,
	}
	b := in.Marshal()
	out, err := UnmarshalMetaBlock(b[:])
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestMetaBlockBadCRC(t *testing.T) {
	in := MetaBlock{MinDocID: 1, MaxDocID: 2}
	b := in.Marshal()
	b[0] ^= 0xFF
	_, err := UnmarshalMetaBlock(b[:])
	require.ErrorIs(t, err, ErrBadCRC)
}

func TestMetaBlockShortBuffer(t *testing.T) {
	_, err := UnmarshalMetaBlock(make([]byte, MetaBlockSize-1))
	require.ErrorIs(t, err, ErrShortBuffer)
}

func TestFooterRoundTrip(t *testing.T) {
	in := Footer{
		Version:          FormatVersion,
		Flags:            0,
		IndexBlockOffset: 0xAABBCCDD,
		IndexBlockSize:   1024,
		MetaBlockOffset:  0xAABBCCDD + 1024,
		MetaBlockSize:    MetaBlockSize,
	}
	b := in.Marshal()
	require.Len(t, b, FooterSize)

	out, err := UnmarshalFooter(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestFooterMagicAtEnd(t *testing.T) {
	f := Footer{Version: FormatVersion}
	b := f.Marshal()
	require.Equal(t, Magic[:], b[FooterSize-8:])
}

func TestFooterBadMagic(t *testing.T) {
	f := Footer{Version: FormatVersion}
	b := f.Marshal()
	b[FooterSize-1] ^= 0xFF
	_, err := UnmarshalFooter(b)
	require.ErrorIs(t, err, ErrBadMagic)
}

func TestFooterBadCRC(t *testing.T) {
	f := Footer{Version: FormatVersion, IndexBlockSize: 99}
	b := f.Marshal()
	b[8] ^= 0xFF // corrupt indexBlockOffset
	_, err := UnmarshalFooter(b)
	require.True(t, errors.Is(err, ErrBadCRC))
}

func TestFooterUnsupportedVersion(t *testing.T) {
	f := Footer{Version: 999}
	b := f.Marshal()
	_, err := UnmarshalFooter(b)
	require.ErrorIs(t, err, ErrBadVersion)
}

func TestFooterShortBuffer(t *testing.T) {
	_, err := UnmarshalFooter(make([]byte, FooterSize-1))
	require.ErrorIs(t, err, ErrShortBuffer)
}

func TestRecordHeaderRoundTrip(t *testing.T) {
	var buf [recordHeaderSize]byte
	encodeRecordHeader(buf[:], 42, types.Timestamp(123456789), 1000)

	docID, ts, plen, err := decodeRecordHeader(buf[:])
	require.NoError(t, err)
	require.Equal(t, types.DocID(42), docID)
	require.Equal(t, types.Timestamp(123456789), ts)
	require.Equal(t, uint32(1000), plen)
}

func TestRecordHeaderShortBuffer(t *testing.T) {
	_, _, _, err := decodeRecordHeader(make([]byte, recordHeaderSize-1))
	require.ErrorIs(t, err, ErrShortBuffer)
}
