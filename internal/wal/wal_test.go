package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := OpenManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m, dir
}

func TestOpenManagerFreshDir(t *testing.T) {
	m, dir := newManager(t)
	require.Equal(t, SegmentID(1), m.Active().SegmentID())
	require.Empty(t, m.Sealed())

	// Segment file exists on disk with id 1.
	_, err := os.Stat(filepath.Join(dir, segmentName(1)))
	require.NoError(t, err)
}

func TestAppendAndReplay(t *testing.T) {
	m, _ := newManager(t)
	w := m.Active()

	cases := []struct {
		typ     RecordType
		payload []byte
	}{
		{RecordPut, []byte("hello")},
		{RecordPut, []byte("world")},
		{RecordDelete, []byte("key1")},
		{RecordPut, []byte("")},
	}

	var lsns []LSN
	for _, c := range cases {
		lsn, err := w.Append(context.Background(), c.typ, c.payload)
		require.NoError(t, err)
		lsns = append(lsns, lsn)
	}

	sealed, err := m.Seal()
	require.NoError(t, err)
	require.Equal(t, SegmentID(1), sealed.ID())

	var got []Record
	require.NoError(t, sealed.Replay(func(r Record) error {
		got = append(got, Record{SegmentID: r.SegmentID, LSN: r.LSN, Type: r.Type, Payload: append([]byte(nil), r.Payload...)})
		return nil
	}))
	require.Len(t, got, len(cases))
	for i, c := range cases {
		require.Equal(t, c.typ, got[i].Type)
		require.Equal(t, string(c.payload), string(got[i].Payload))
		require.Equal(t, lsns[i], got[i].LSN)
		require.Equal(t, SegmentID(1), got[i].SegmentID)
	}
}

func TestLSNsMonotonicWithinSegment(t *testing.T) {
	m, _ := newManager(t)
	w := m.Active()
	var prev LSN
	for i := 0; i < 100; i++ {
		lsn, err := w.Append(context.Background(), RecordPut, []byte("payload"))
		require.NoError(t, err)
		if i > 0 {
			require.Greater(t, lsn, prev)
		}
		prev = lsn
	}
}

func TestSealOpensFreshSegment(t *testing.T) {
	m, dir := newManager(t)

	_, err := m.Active().Append(context.Background(), RecordPut, []byte("a"))
	require.NoError(t, err)

	sealed, err := m.Seal()
	require.NoError(t, err)
	require.Equal(t, SegmentID(1), sealed.ID())

	// Active is now segment 2 with offset 0.
	require.Equal(t, SegmentID(2), m.Active().SegmentID())
	lsn, err := m.Active().Append(context.Background(), RecordPut, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, LSN(0), lsn)

	// Both files exist.
	_, err = os.Stat(filepath.Join(dir, segmentName(1)))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, segmentName(2)))
	require.NoError(t, err)
}

func TestSealedListInOrder(t *testing.T) {
	m, _ := newManager(t)
	for i := 0; i < 3; i++ {
		_, err := m.Active().Append(context.Background(), RecordPut, []byte("x"))
		require.NoError(t, err)
		_, err = m.Seal()
		require.NoError(t, err)
	}
	sealed := m.Sealed()
	require.Len(t, sealed, 3)
	require.Equal(t, SegmentID(1), sealed[0].ID())
	require.Equal(t, SegmentID(2), sealed[1].ID())
	require.Equal(t, SegmentID(3), sealed[2].ID())
}

func TestReopenScansExistingSegments(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := m.Active().Append(context.Background(), RecordPut, []byte("x"))
		require.NoError(t, err)
		_, err = m.Seal()
		require.NoError(t, err)
	}
	// Now active segment is 4 with no records; close manager.
	require.NoError(t, m.Close())

	m2, err := OpenManager(dir)
	require.NoError(t, err)
	defer m2.Close()

	sealed := m2.Sealed()
	require.Len(t, sealed, 3, "the three sealed segments should be discovered")
	require.Equal(t, SegmentID(1), sealed[0].ID())
	require.Equal(t, SegmentID(3), sealed[2].ID())

	// The newest segment is reopened as active.
	require.Equal(t, SegmentID(4), m2.Active().SegmentID())

	// Next seal produces id 5.
	_, err = m2.Active().Append(context.Background(), RecordPut, []byte("y"))
	require.NoError(t, err)
	s, err := m2.Seal()
	require.NoError(t, err)
	require.Equal(t, SegmentID(4), s.ID())
	require.Equal(t, SegmentID(5), m2.Active().SegmentID())
}

func TestDeleteRemovesSegment(t *testing.T) {
	m, dir := newManager(t)
	_, err := m.Active().Append(context.Background(), RecordPut, []byte("x"))
	require.NoError(t, err)
	sealed, err := m.Seal()
	require.NoError(t, err)

	require.NoError(t, sealed.Delete())
	_, err = os.Stat(filepath.Join(dir, segmentName(sealed.ID())))
	require.True(t, os.IsNotExist(err))

	// Idempotent.
	require.NoError(t, sealed.Delete())
}

func TestDeleteMiddleSegmentDoesNotAffectOthers(t *testing.T) {
	m, dir := newManager(t)
	for i := 0; i < 3; i++ {
		_, err := m.Active().Append(context.Background(), RecordPut, []byte("x"))
		require.NoError(t, err)
		_, err = m.Seal()
		require.NoError(t, err)
	}
	sealed := m.Sealed()
	require.NoError(t, sealed[1].Delete())

	// 1 and 3 still on disk; 2 gone.
	_, err := os.Stat(filepath.Join(dir, segmentName(1)))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, segmentName(2)))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, segmentName(3)))
	require.NoError(t, err)
}

func TestReplayStopsOnTornTail(t *testing.T) {
	m, _ := newManager(t)
	for i := 0; i < 3; i++ {
		_, err := m.Active().Append(context.Background(), RecordPut, []byte("good"))
		require.NoError(t, err)
	}
	sealed, err := m.Seal()
	require.NoError(t, err)

	// Append garbage simulating a partial write after crash.
	f, err := os.OpenFile(sealed.Path(), os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xAB, 0xCD, 0xEF})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	count := 0
	require.NoError(t, sealed.Replay(func(r Record) error {
		count++
		require.Equal(t, "good", string(r.Payload))
		return nil
	}))
	require.Equal(t, 3, count)
}

func TestReplayDetectsCorruptCRC(t *testing.T) {
	m, _ := newManager(t)
	_, err := m.Active().Append(context.Background(), RecordPut, []byte("hello"))
	require.NoError(t, err)
	sealed, err := m.Seal()
	require.NoError(t, err)

	data, err := os.ReadFile(sealed.Path())
	require.NoError(t, err)
	data[len(data)-1] ^= 0xFF
	require.NoError(t, os.WriteFile(sealed.Path(), data, 0o644))

	count := 0
	require.NoError(t, sealed.Replay(func(r Record) error {
		count++
		return nil
	}))
	require.Equal(t, 0, count)
}

func TestAppendRespectsContextCancel(t *testing.T) {
	m, _ := newManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Active().Append(ctx, RecordPut, []byte("x"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestConcurrentAppend(t *testing.T) {
	m, _ := newManager(t)
	w := m.Active()

	const writers = 8
	const perWriter = 50
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				_, err := w.Append(context.Background(), RecordPut, []byte("payload"))
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	sealed, err := m.Seal()
	require.NoError(t, err)

	count := 0
	require.NoError(t, sealed.Replay(func(Record) error {
		count++
		return nil
	}))
	require.Equal(t, writers*perWriter, count)
}

func TestPayloadTooBig(t *testing.T) {
	m, _ := newManager(t)
	big := make([]byte, maxPayload+1)
	_, err := m.Active().Append(context.Background(), RecordPut, big)
	require.True(t, errors.Is(err, ErrPayloadTooBig))
}

func TestAppendAfterCloseFails(t *testing.T) {
	m, _ := newManager(t)
	w := m.Active()
	require.NoError(t, m.Close())
	_, err := w.Append(context.Background(), RecordPut, []byte("x"))
	require.Error(t, err)
}

func TestCloseIdempotent(t *testing.T) {
	m, _ := newManager(t)
	require.NoError(t, m.Close())
	require.NoError(t, m.Close())
}

func TestSegmentNameRoundTrip(t *testing.T) {
	for _, id := range []SegmentID{1, 2, 99, 1000, 999999, 1234567} {
		name := segmentName(id)
		got, ok := parseSegmentName(name)
		require.True(t, ok)
		require.Equal(t, id, got)
	}
}

func TestParseSegmentNameRejectsNonMatching(t *testing.T) {
	cases := []string{
		"random.txt",
		"wal-abc.log",
		"wal-1.txt",
		"foo-000001.log",
		"wal-000001.bin",
	}
	for _, c := range cases {
		_, ok := parseSegmentName(c)
		require.False(t, ok, "should reject: %s", c)
	}
}

func TestOpenManagerIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	// Drop unrelated files into the dir.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.txt"), []byte("hi"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal-bad.log"), []byte("nope"), 0o644))

	m, err := OpenManager(dir)
	require.NoError(t, err)
	defer m.Close()
	require.Equal(t, SegmentID(1), m.Active().SegmentID())
	require.Empty(t, m.Sealed())
}
