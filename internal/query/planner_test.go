package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/types"
)

func mustParse(t *testing.T, sql string) *Statement {
	t.Helper()
	stmt, err := Parse(sql)
	require.NoError(t, err)
	return stmt
}

func TestPlan_NoWhereNoOpts(t *testing.T) {
	opts := Plan(mustParse(t, "SELECT * FROM logs"))
	require.Zero(t, opts.MinTimestamp)
	require.Zero(t, opts.MaxTimestamp)
}

func TestPlan_TsEqualSetsBothBounds(t *testing.T) {
	t1 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	opts := Plan(mustParse(t, "SELECT * FROM logs WHERE ts = '"+t1.Format(time.RFC3339)+"'"))
	require.Equal(t, types.Timestamp(t1.UnixNano()), opts.MinTimestamp)
	require.Equal(t, types.Timestamp(t1.UnixNano()), opts.MaxTimestamp)
}

func TestPlan_TsGreaterSetsMinOnly(t *testing.T) {
	t1 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	opts := Plan(mustParse(t, "SELECT * FROM logs WHERE ts >= '"+t1.Format(time.RFC3339)+"'"))
	require.Equal(t, types.Timestamp(t1.UnixNano()), opts.MinTimestamp)
	require.Zero(t, opts.MaxTimestamp)
}

func TestPlan_TsLessSetsMaxOnly(t *testing.T) {
	t1 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	opts := Plan(mustParse(t, "SELECT * FROM logs WHERE ts <= '"+t1.Format(time.RFC3339)+"'"))
	require.Zero(t, opts.MinTimestamp)
	require.Equal(t, types.Timestamp(t1.UnixNano()), opts.MaxTimestamp)
}

func TestPlan_RangeAcrossAndChain(t *testing.T) {
	lo := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	hi := time.Date(2026, 5, 26, 18, 0, 0, 0, time.UTC)
	opts := Plan(mustParse(t,
		"SELECT * FROM logs WHERE ts >= '"+lo.Format(time.RFC3339)+
			"' AND ts <= '"+hi.Format(time.RFC3339)+"' AND source = 'api'"))
	require.Equal(t, types.Timestamp(lo.UnixNano()), opts.MinTimestamp)
	require.Equal(t, types.Timestamp(hi.UnixNano()), opts.MaxTimestamp)
}

func TestPlan_NarrowsToTighterBounds(t *testing.T) {
	// Two lower-bound predicates: keep the tighter (larger) lower bound.
	// Two upper-bound predicates: keep the tighter (smaller) upper bound.
	opts := Plan(mustParse(t,
		"SELECT * FROM logs WHERE ts >= '2026-01-01T00:00:00Z' AND ts >= '2026-06-01T00:00:00Z'"+
			" AND ts <= '2027-01-01T00:00:00Z' AND ts <= '2026-12-01T00:00:00Z'"))
	wantLo, _ := time.Parse(time.RFC3339, "2026-06-01T00:00:00Z")
	wantHi, _ := time.Parse(time.RFC3339, "2026-12-01T00:00:00Z")
	require.Equal(t, types.Timestamp(wantLo.UnixNano()), opts.MinTimestamp)
	require.Equal(t, types.Timestamp(wantHi.UnixNano()), opts.MaxTimestamp)
}

func TestPlan_OrAtTopLevelDisablesPushdown(t *testing.T) {
	// OR means we might match records on a branch where ts isn't
	// constrained — pushing down would risk dropping those.
	opts := Plan(mustParse(t,
		"SELECT * FROM logs WHERE ts >= '2026-01-01T00:00:00Z' OR source = 'api'"))
	require.Zero(t, opts.MinTimestamp, "OR present -> no ts pushdown")
	require.Zero(t, opts.MaxTimestamp)
}

func TestPlan_ParenthesizedSubexprDoesNotPushDown(t *testing.T) {
	// Documented limitation: planner only walks the top-level AND chain.
	// (ts >= X) in a paren is recognized by the parser but the planner
	// does not recurse into the sub-expr.
	opts := Plan(mustParse(t,
		"SELECT * FROM logs WHERE (ts >= '2026-01-01T00:00:00Z') AND source = 'api'"))
	require.Zero(t, opts.MinTimestamp, "parens not recursed today")
	require.Zero(t, opts.MaxTimestamp)
}

func TestPlan_TsLiteralAsUnixNanosInt(t *testing.T) {
	opts := Plan(mustParse(t, "SELECT * FROM logs WHERE ts >= 1700000000000000000"))
	require.Equal(t, types.Timestamp(1700000000000000000), opts.MinTimestamp)
}

func TestPlan_NonTsFieldsIgnored(t *testing.T) {
	opts := Plan(mustParse(t,
		"SELECT * FROM logs WHERE source = 'api' AND fields.level = 'error'"))
	require.Zero(t, opts.MinTimestamp)
	require.Zero(t, opts.MaxTimestamp)
}
