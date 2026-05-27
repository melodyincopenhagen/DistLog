package query

import (
	"context"
	"fmt"
	"time"

	"github.com/yuexishen/distlog/internal/types"
)

// Scanner is the subset of *engine.Engine the executor depends on.
// Defined here (not in engine) so the executor can be tested with a
// fake scanner without spinning up a real engine.
type Scanner interface {
	Scan(ctx context.Context, visit func(types.DocID, *types.LogRecord) error) error
}

// Row is one result record. Fields holds the projected columns, keyed
// by their SQL name ("ts", "message", "fields.level", ...). Values are
// the canonical Go types — string, int64, map[string]string — so the
// HTTP layer can JSON-encode them directly without further coercion.
type Row struct {
	DocID  types.DocID    `json:"doc_id"`
	Values map[string]any `json:"values"`
}

// Result is what the executor returns.
type Result struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

// Execute runs the parsed statement against the scanner. Returns an
// error if a row evaluation hits an error (e.g. unknown field — but
// parser already constrains the field name set, so in practice this
// only fires on bad time literals).
//
// Sentinel internal error used to abort engine.Scan early once LIMIT
// is reached. Translated back to a clean return in Execute.
var errLimitReached = fmt.Errorf("query: limit reached")

func Execute(ctx context.Context, s Scanner, stmt *Statement) (*Result, error) {
	cols, err := resolveColumns(stmt.Select)
	if err != nil {
		return nil, err
	}
	result := &Result{Columns: cols}
	limit := -1
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	err = s.Scan(ctx, func(id types.DocID, rec *types.LogRecord) error {
		match, err := Eval(stmt.Where, id, rec)
		if err != nil {
			return err
		}
		if !match {
			return nil
		}
		row := Row{DocID: id, Values: project(cols, id, rec)}
		result.Rows = append(result.Rows, row)
		if limit >= 0 && len(result.Rows) >= limit {
			return errLimitReached
		}
		return nil
	})
	if err != nil && err != errLimitReached {
		return nil, err
	}
	return result, nil
}

// resolveColumns turns SelectClause into the ordered list of output
// column names. For SELECT *, returns the fixed top-level columns
// (no per-record map keys — those vary across records and explicit
// projection is required to surface them).
func resolveColumns(sel *SelectClause) ([]string, error) {
	if sel.Star {
		return []string{"doc_id", "ts", "tenant_id", "source", "message", "fields"}, nil
	}
	out := make([]string, 0, len(sel.Columns))
	for _, c := range sel.Columns {
		if c.Map != "" {
			if c.Name != "fields" {
				return nil, fmt.Errorf("query: only fields.<key> supported, got %s.%s", c.Name, c.Map)
			}
			out = append(out, "fields."+c.Map)
			continue
		}
		switch c.Name {
		case "doc_id", "ts", "tenant_id", "source", "message", "fields":
			out = append(out, c.Name)
		default:
			return nil, fmt.Errorf("query: unknown column %q", c.Name)
		}
	}
	return out, nil
}

// project pulls the named columns out of (docID, rec) into a value
// map ready for JSON encoding. Timestamps go to RFC3339Nano UTC
// strings to match the HTTP server's other timestamp emissions.
func project(cols []string, id types.DocID, rec *types.LogRecord) map[string]any {
	out := make(map[string]any, len(cols))
	for _, c := range cols {
		if len(c) > 7 && c[:7] == "fields." {
			key := c[7:]
			if rec.Fields != nil {
				out[c] = rec.Fields[key]
			} else {
				out[c] = ""
			}
			continue
		}
		switch c {
		case "doc_id":
			out[c] = uint64(id)
		case "ts":
			out[c] = rec.Timestamp.Time().UTC().Format(time.RFC3339Nano)
		case "tenant_id":
			out[c] = rec.TenantID
		case "source":
			out[c] = rec.Source
		case "message":
			out[c] = rec.Message
		case "fields":
			if rec.Fields != nil {
				out[c] = rec.Fields
			} else {
				out[c] = map[string]string{}
			}
		}
	}
	return out
}
