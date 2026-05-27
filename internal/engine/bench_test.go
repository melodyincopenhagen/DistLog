package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yuexishen/distlog/internal/types"
)

// makeRecord returns a synthetic LogRecord whose marshaled payload is
// around 350 bytes — representative of a small structured log line
// (timestamp + a few hundred bytes of message + 2-3 tag fields).
func makeRecord(i int) *types.LogRecord {
	return &types.LogRecord{
		Timestamp: types.Timestamp(int64(i)),
		TenantID:  "tenant-bench",
		Source:    fmt.Sprintf("host-%d", i%32),
		Message:   "request completed in " + strings.Repeat("x", 200),
		Fields: map[string]string{
			"level":    "info",
			"trace_id": fmt.Sprintf("trace-%d", i),
		},
	}
}

// openBenchEngine opens an engine sized so that flushes happen during
// the benchmark, exercising the realistic write-then-flush path rather
// than the pure-in-memory degenerate case.
func openBenchEngine(b *testing.B) *Engine {
	b.Helper()
	e, err := Open(Config{
		DataDir:           b.TempDir(),
		MemTableSizeLimit: 256 * 1024, // small enough to force flushes
		MemTableHardLimit: 1 << 30,    // disable stall — benches measure happy path
	})
	if err != nil {
		b.Fatalf("open engine: %v", err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}

// BenchmarkEngine_Write measures the end-to-end Write path: WAL append
// + MemTable put + occasional freeze/flush triggered by MemTableSizeLimit.
func BenchmarkEngine_Write(b *testing.B) {
	e := openBenchEngine(b)
	ctx := context.Background()
	rec := makeRecord(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Write(ctx, rec); err != nil {
			b.Fatalf("write %d: %v", i, err)
		}
	}
}

// BenchmarkEngine_Get_ActiveHit measures Get latency when the record
// is still in the active MemTable — the no-IO best case.
func BenchmarkEngine_Get_ActiveHit(b *testing.B) {
	e := openBenchEngine(b)
	ctx := context.Background()
	id, err := e.Write(ctx, makeRecord(0))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := e.Get(id); !ok || err != nil {
			b.Fatalf("get: ok=%v err=%v", ok, err)
		}
	}
}

// BenchmarkEngine_Get_SSTableHit measures Get latency when the record
// has been flushed to disk — includes the SSTable block read +
// in-block binary search + JSON decode.
func BenchmarkEngine_Get_SSTableHit(b *testing.B) {
	e := openBenchEngine(b)
	ctx := context.Background()
	const n = 5000
	ids := make([]types.DocID, n)
	for i := 0; i < n; i++ {
		id, err := e.Write(ctx, makeRecord(i))
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}
	waitFor(b, func() bool {
		return e.Stats().SSTableCount >= 1
	}, 5*time.Second, "first sstable")
	target := ids[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := e.Get(target); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngine_Scan reports records/sec across a full scan.
func BenchmarkEngine_Scan(b *testing.B) {
	e := openBenchEngine(b)
	const total = 20_000
	seedScanData(b, e, total)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		err := e.Scan(ctx, func(types.DocID, *types.LogRecord) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != total {
			b.Fatalf("expected %d records, got %d", total, count)
		}
	}
	b.ReportMetric(float64(total*b.N)/b.Elapsed().Seconds(), "records/sec")
}

// seedScanData writes `total` records evenly distributed across 10
// timestamp buckets, driving freeze+flush so the engine ends up with
// multiple SSTables.
func seedScanData(b *testing.B, e *Engine, total int) {
	b.Helper()
	ctx := context.Background()
	const buckets = 10
	for i := 0; i < total; i++ {
		bucket := i / (total / buckets)
		if bucket >= buckets {
			bucket = buckets - 1
		}
		rec := makeRecord(i)
		rec.Timestamp = types.Timestamp(int64(bucket*1_000_000 + i))
		if _, err := e.Write(ctx, rec); err != nil {
			b.Fatal(err)
		}
	}
	waitFor(b, func() bool {
		return e.Stats().SSTableCount >= 3
	}, 10*time.Second, "multiple sstables")
}
