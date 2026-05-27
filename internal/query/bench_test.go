package query_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yuexishen/distlog/internal/engine"
	"github.com/yuexishen/distlog/internal/query"
	"github.com/yuexishen/distlog/internal/types"
)

// BenchmarkQuery_TsPushdown compares scan-only vs ts-pushdown for a
// query that targets ~10% of the timestamp space across an engine
// holding ~20k records across many SSTables.
//
// Each sub-benchmark reports two custom metrics in addition to ns/op:
//
//	sstables_scanned   how many SSTables the executor's underlying
//	                   scan actually opened (= total - pruned)
//	sstables_pruned    the engine's pruning hit count
//
// The pushdown variant should show meaningful pruning; the no-pushdown
// variant should always scan everything.
func BenchmarkQuery_TsPushdown(b *testing.B) {
	const (
		total      = 20_000
		buckets    = 20
		windowDays = 30
	)
	e := openBenchEngine(b)
	seedTsBucketed(b, e, total, buckets, windowDays)

	totalSSTables := e.Stats().SSTableCount
	if totalSSTables < 5 {
		b.Skipf("not enough SSTables produced (%d) for pruning to be observable", totalSSTables)
	}

	// Pick a window covering ~10% of the time range: days 14-16.
	windowStart := time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 1, 16, 23, 59, 59, 0, time.UTC)

	b.Run("with_pushdown", func(b *testing.B) {
		sql := fmt.Sprintf(
			"SELECT message FROM logs WHERE ts >= '%s' AND ts <= '%s'",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
		runQueryBench(b, e, sql, totalSSTables)
	})

	b.Run("without_pushdown", func(b *testing.B) {
		// Same record-level predicate, but expressed in a way that
		// disables ts pushdown: wrap the ts conditions in an OR
		// with a vacuously-false branch. The planner conservatively
		// drops ts pushdown whenever OR is present, so this gives a
		// fair "same answer, no pruning" comparison.
		sql := fmt.Sprintf(
			"SELECT message FROM logs WHERE (ts >= '%s' AND ts <= '%s') OR ts = -1",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
		runQueryBench(b, e, sql, totalSSTables)
	})
}

func runQueryBench(b *testing.B, e *engine.Engine, sql string, totalSSTables int) {
	stmt, err := query.Parse(sql)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	var lastPruned int
	for i := 0; i < b.N; i++ {
		if _, err := query.Execute(ctx, e, stmt); err != nil {
			b.Fatal(err)
		}
		lastPruned = e.Stats().LastScanPrunedSSTables
	}
	b.StopTimer()
	b.ReportMetric(float64(totalSSTables-lastPruned), "sstables_scanned")
	b.ReportMetric(float64(lastPruned), "sstables_pruned")
}

// openBenchEngine + seedTsBucketed are query-package versions of the
// engine-package helpers, kept inline here so the query benchmark
// doesn't depend on engine-package test files (which aren't exported).

func openBenchEngine(b *testing.B) *engine.Engine {
	b.Helper()
	e, err := engine.Open(engine.Config{
		DataDir:           b.TempDir(),
		MemTableSizeLimit: 256 * 1024,
		MemTableHardLimit: 1 << 30,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}

func seedTsBucketed(b *testing.B, e *engine.Engine, total, buckets, windowDays int) {
	b.Helper()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bucketDur := time.Duration(windowDays) * 24 * time.Hour / time.Duration(buckets)
	perBucket := total / buckets
	// Important: write each bucket's records contiguously so SSTables
	// inherit narrow ts ranges. Round-robin writes would give every
	// SSTable a wide ts range and defeat pruning.
	for bucket := 0; bucket < buckets; bucket++ {
		for j := 0; j < perBucket; j++ {
			ts := base.Add(time.Duration(bucket) * bucketDur).
				Add(time.Duration(j) * time.Microsecond)
			rec := &types.LogRecord{
				Timestamp: types.Timestamp(ts.UnixNano()),
				Source:    fmt.Sprintf("host-%d", j%32),
				Message:   "request handled " + strings.Repeat("x", 200),
				Fields:    map[string]string{"level": "info"},
			}
			if _, err := e.Write(ctx, rec); err != nil {
				b.Fatal(err)
			}
		}
	}
	// Wait for several SSTables.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if e.Stats().SSTableCount >= 5 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatalf("only %d SSTables after seeding", e.Stats().SSTableCount)
}
