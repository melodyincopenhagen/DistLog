package query

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/engine"
	"github.com/yuexishen/distlog/internal/types"
)

// fakeScanner replays a fixed in-memory slice. Lets us test the
// executor without spinning up a real engine. lastOpts records the
// most recent ScanOptions so planner integration tests can assert on
// what the executor actually pushed down.
type fakeScanner struct {
	records []struct {
		id  types.DocID
		rec *types.LogRecord
	}
	lastOpts engine.ScanOptions
}

func (f *fakeScanner) ScanWithOptions(ctx context.Context, opts engine.ScanOptions, visit func(types.DocID, *types.LogRecord) error) error {
	f.lastOpts = opts
	for _, r := range f.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(r.id, r.rec); err != nil {
			return err
		}
	}
	return nil
}

func mkScanner(records ...*types.LogRecord) *fakeScanner {
	s := &fakeScanner{}
	for i, r := range records {
		s.records = append(s.records, struct {
			id  types.DocID
			rec *types.LogRecord
		}{types.DocID(i + 1), r})
	}
	return s
}

func TestExecute_SelectStarReturnsAllRows(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Message: "a"},
		&types.LogRecord{Message: "b"},
		&types.LogRecord{Message: "c"},
	)
	stmt, err := Parse("SELECT * FROM logs")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)
	require.Contains(t, res.Columns, "message")
	require.Equal(t, "a", res.Rows[0].Values["message"])
}

func TestExecute_WhereFiltersOnTopLevelField(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Source: "api", Message: "hit"},
		&types.LogRecord{Source: "db", Message: "miss"},
		&types.LogRecord{Source: "api", Message: "hit2"},
	)
	stmt, err := Parse("SELECT message FROM logs WHERE source = 'api'")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "hit", res.Rows[0].Values["message"])
	require.Equal(t, "hit2", res.Rows[1].Values["message"])
}

func TestExecute_WhereFiltersOnFieldsMap(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Message: "a", Fields: map[string]string{"level": "info"}},
		&types.LogRecord{Message: "b", Fields: map[string]string{"level": "error"}},
		&types.LogRecord{Message: "c", Fields: map[string]string{"level": "error"}},
	)
	stmt, err := Parse("SELECT message FROM logs WHERE fields.level = 'error'")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
}

func TestExecute_AndOrPrecedence(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Source: "a", Message: "hit"},
		&types.LogRecord{Source: "b", Message: ""},
		&types.LogRecord{Source: "c", Message: "hit"},
	)
	// (source = 'a' OR source = 'c') AND message = 'hit'
	stmt, err := Parse("SELECT message FROM logs WHERE (source = 'a' OR source = 'c') AND message = 'hit'")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
}

func TestExecute_LimitTruncates(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Message: "a"},
		&types.LogRecord{Message: "b"},
		&types.LogRecord{Message: "c"},
		&types.LogRecord{Message: "d"},
	)
	stmt, err := Parse("SELECT message FROM logs LIMIT 2")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
}

func TestExecute_TimestampComparison(t *testing.T) {
	t1 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC).UnixNano()
	t2 := time.Date(2026, 5, 26, 13, 0, 0, 0, time.UTC).UnixNano()
	s := mkScanner(
		&types.LogRecord{Timestamp: types.Timestamp(t1), Message: "early"},
		&types.LogRecord{Timestamp: types.Timestamp(t2), Message: "late"},
	)
	stmt, err := Parse("SELECT message FROM logs WHERE ts = '2026-05-26T13:00:00Z'")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "late", res.Rows[0].Values["message"])
}

func TestExecute_DocIDComparison(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Message: "a"},
		&types.LogRecord{Message: "b"},
		&types.LogRecord{Message: "c"},
	)
	stmt, err := Parse("SELECT * FROM logs WHERE doc_id = 2")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "b", res.Rows[0].Values["message"])
}

func TestExecute_NotEqualOp(t *testing.T) {
	s := mkScanner(
		&types.LogRecord{Source: "api"},
		&types.LogRecord{Source: "db"},
	)
	stmt, err := Parse("SELECT source FROM logs WHERE source != 'api'")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "db", res.Rows[0].Values["source"])
}

func TestExecute_ProjectsRequestedColumnsOnly(t *testing.T) {
	s := mkScanner(&types.LogRecord{
		Source: "api", Message: "x",
		Fields: map[string]string{"level": "info"},
	})
	stmt, err := Parse("SELECT message, fields.level FROM logs")
	require.NoError(t, err)
	res, err := Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Equal(t, []string{"message", "fields.level"}, res.Columns)
	require.Equal(t, "x", res.Rows[0].Values["message"])
	require.Equal(t, "info", res.Rows[0].Values["fields.level"])
	require.NotContains(t, res.Rows[0].Values, "source", "source must not be projected")
}

func TestExecute_BadTimestampLiteralIsError(t *testing.T) {
	s := mkScanner(&types.LogRecord{Timestamp: types.Now()})
	stmt, err := Parse("SELECT * FROM logs WHERE ts = 'not-a-timestamp'")
	require.NoError(t, err)
	_, err = Execute(context.Background(), s, stmt)
	require.Error(t, err)
}

func TestExecute_PushesDownTsRangeToScanner(t *testing.T) {
	s := mkScanner(&types.LogRecord{Message: "x"})
	stmt, err := Parse("SELECT * FROM logs WHERE ts >= '2026-01-01T00:00:00Z' AND ts <= '2026-12-31T23:59:59Z'")
	require.NoError(t, err)
	_, err = Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.NotZero(t, s.lastOpts.MinTimestamp, "executor must pass planned MinTimestamp to scanner")
	require.NotZero(t, s.lastOpts.MaxTimestamp, "executor must pass planned MaxTimestamp to scanner")
}

func TestExecute_NoPushdownWhenWhereHasNoTs(t *testing.T) {
	s := mkScanner(&types.LogRecord{Message: "x"})
	stmt, err := Parse("SELECT * FROM logs WHERE source = 'api'")
	require.NoError(t, err)
	_, err = Execute(context.Background(), s, stmt)
	require.NoError(t, err)
	require.Zero(t, s.lastOpts.MinTimestamp)
	require.Zero(t, s.lastOpts.MaxTimestamp)
}

func TestExecute_UnknownFieldIsError(t *testing.T) {
	// Parser accepts any ident; evaluator must reject at eval time.
	s := mkScanner(&types.LogRecord{Message: "x"})
	stmt, err := Parse("SELECT * FROM logs WHERE nonexistent = 'x'")
	require.NoError(t, err)
	_, err = Execute(context.Background(), s, stmt)
	require.Error(t, err)
}
