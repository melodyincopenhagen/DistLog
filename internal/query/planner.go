package query

import (
	"time"

	"github.com/yuexishen/distlog/internal/engine"
	"github.com/yuexishen/distlog/internal/types"
)

// Plan derives engine pushdown hints from a parsed Statement. Today
// the only hint is the ts time-range; in the future this is where
// other predicate-pushdown extractors would live.
//
// Extraction is intentionally conservative: it only walks the top-
// level AND chain of the WHERE clause. The presence of any OR
// branches at the top level disables ts pushdown for the whole
// statement, because pruning by ts would risk dropping SSTables that
// contain records matching the *other* branch of the OR. Parenthesized
// sub-expressions are not recursed into for the same reason.
//
// This conservative-by-default policy mirrors how production query
// planners introduce pushdown: incorrect pushdown is a correctness
// bug; missed pushdown is "only" a performance loss. The latter is
// always preferable when in doubt.
func Plan(stmt *Statement) engine.ScanOptions {
	var opts engine.ScanOptions
	if stmt == nil || stmt.Where == nil {
		return opts
	}
	or := stmt.Where.Or
	if len(or.Rest) > 0 {
		// Top-level OR present — abort ts extraction.
		return opts
	}
	// Single AND chain at the top level.
	extractFromAnd(or.And, &opts)
	return opts
}

func extractFromAnd(and *AndExpr, opts *engine.ScanOptions) {
	applyCmp(and.Cmp, opts)
	for _, c := range and.Rest {
		applyCmp(c, opts)
	}
}

// applyCmp updates opts in place if c is a top-level Compare on ts.
// Parenthesized subexpressions are not recursed: the conservative
// rule is "OR anywhere disables pushdown," and detecting OR-inside-
// parens reliably is bookkeeping we'll add when there's a real
// use case. Today, `WHERE (ts > X) AND ...` simply doesn't push down.
func applyCmp(c *CmpExpr, opts *engine.ScanOptions) {
	if c.Cmp == nil {
		return
	}
	if c.Cmp.Field.Map != "" || c.Cmp.Field.Name != "ts" {
		return
	}
	ts, ok := literalToTimestamp(c.Cmp.Value)
	if !ok {
		return
	}
	switch c.Cmp.Op {
	case "=":
		narrowMin(opts, ts)
		narrowMax(opts, ts)
	case ">", ">=":
		// > is one nano stricter than >= in nanos space; for pruning
		// purposes treating them identically is safe (it only ever
		// keeps an extra SSTable that boundary-matches; the per-record
		// predicate still filters exactly).
		narrowMin(opts, ts)
	case "<", "<=":
		narrowMax(opts, ts)
	}
}

// narrowMin sets opts.MinTimestamp to max(current, ts). Treats 0 as
// "unset" — DocID-allocator-style monotonic-on-set semantics.
func narrowMin(opts *engine.ScanOptions, ts types.Timestamp) {
	if opts.MinTimestamp == 0 || ts > opts.MinTimestamp {
		opts.MinTimestamp = ts
	}
}

// narrowMax sets opts.MaxTimestamp to min(current, ts). Treats 0 as
// "unset."
func narrowMax(opts *engine.ScanOptions, ts types.Timestamp) {
	if opts.MaxTimestamp == 0 || ts < opts.MaxTimestamp {
		opts.MaxTimestamp = ts
	}
}

// literalToTimestamp converts a literal to a types.Timestamp if it
// makes sense as one: RFC3339 string or unix-nanos integer. Anything
// else returns ok=false (and we skip pushdown for that compare).
func literalToTimestamp(lit *Literal) (types.Timestamp, bool) {
	if lit.Int != nil {
		return types.Timestamp(*lit.Int), true
	}
	if lit.Str == nil {
		return 0, false
	}
	t, err := time.Parse(time.RFC3339Nano, *lit.Str)
	if err != nil {
		t, err = time.Parse(time.RFC3339, *lit.Str)
		if err != nil {
			return 0, false
		}
	}
	return types.Timestamp(t.UnixNano()), true
}
