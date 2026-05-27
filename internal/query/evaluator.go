package query

import (
	"fmt"
	"strconv"
	"time"

	"github.com/yuexishen/distlog/internal/types"
)

// evalCtx is what an Expr is evaluated against: one record plus its
// assigned DocID. DocID is part of the context (not the record) because
// `doc_id` is a queryable column but does not live on LogRecord.
type evalCtx struct {
	docID types.DocID
	rec   *types.LogRecord
}

// Eval reports whether the Expr matches the given record. A nil Expr
// matches everything.
func Eval(e *Expr, docID types.DocID, rec *types.LogRecord) (bool, error) {
	if e == nil {
		return true, nil
	}
	return e.Or.eval(evalCtx{docID: docID, rec: rec})
}

func (o *OrExpr) eval(ctx evalCtx) (bool, error) {
	ok, err := o.And.eval(ctx)
	if err != nil || ok {
		return ok, err
	}
	for _, r := range o.Rest {
		ok, err = r.eval(ctx)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func (a *AndExpr) eval(ctx evalCtx) (bool, error) {
	ok, err := a.Cmp.eval(ctx)
	if err != nil || !ok {
		return ok, err
	}
	for _, r := range a.Rest {
		ok, err = r.eval(ctx)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func (c *CmpExpr) eval(ctx evalCtx) (bool, error) {
	if c.Sub != nil {
		return c.Sub.Or.eval(ctx)
	}
	return c.Cmp.eval(ctx)
}

func (c *Compare) eval(ctx evalCtx) (bool, error) {
	got, err := loadField(c.Field, ctx)
	if err != nil {
		return false, err
	}
	want, err := loadLiteral(c.Field, c.Value)
	if err != nil {
		return false, err
	}
	switch c.Op {
	case "=":
		return got == want, nil
	case "!=":
		return got != want, nil
	case "<", "<=", ">", ">=":
		// Ordered comparison: only meaningful on numeric fields. ts
		// and doc_id are canonicalized to base-10 integer strings by
		// loadField/loadLiteral, so we can re-parse to int64 here.
		if !isOrderedField(c.Field) {
			return false, fmt.Errorf("query: operator %q only supported on numeric fields (ts, doc_id), not %q",
				c.Op, fieldDisplay(c.Field))
		}
		l, err := strconv.ParseInt(got, 10, 64)
		if err != nil {
			return false, fmt.Errorf("query: lhs not numeric: %w", err)
		}
		r, err := strconv.ParseInt(want, 10, 64)
		if err != nil {
			return false, fmt.Errorf("query: rhs not numeric: %w", err)
		}
		switch c.Op {
		case "<":
			return l < r, nil
		case "<=":
			return l <= r, nil
		case ">":
			return l > r, nil
		case ">=":
			return l >= r, nil
		}
	}
	return false, fmt.Errorf("query: unknown operator %q", c.Op)
}

func isOrderedField(f *FieldRef) bool {
	return f.Map == "" && (f.Name == "ts" || f.Name == "doc_id")
}

func fieldDisplay(f *FieldRef) string {
	if f.Map != "" {
		return f.Name + "." + f.Map
	}
	return f.Name
}

// loadField extracts the queried column from the record, coercing to a
// canonical comparable string form. For `ts` the canonical form is
// strconv-formatted unix-nanos; for `doc_id` it's the integer's string;
// for everything else it's the raw string. This keeps comparisons
// type-uniform without inventing a typed value system for one feature.
func loadField(f *FieldRef, ctx evalCtx) (string, error) {
	if f.Map != "" {
		if f.Name != "fields" {
			return "", fmt.Errorf("query: only fields.<key> supported for nested access, got %s.%s", f.Name, f.Map)
		}
		if ctx.rec.Fields == nil {
			return "", nil
		}
		return ctx.rec.Fields[f.Map], nil
	}
	switch f.Name {
	case "ts":
		return fmt.Sprintf("%d", ctx.rec.Timestamp.UnixNano()), nil
	case "tenant_id":
		return ctx.rec.TenantID, nil
	case "source":
		return ctx.rec.Source, nil
	case "message":
		return ctx.rec.Message, nil
	case "doc_id":
		return fmt.Sprintf("%d", uint64(ctx.docID)), nil
	default:
		return "", fmt.Errorf("query: unknown field %q", f.Name)
	}
}

// loadLiteral renders the SQL literal into the same canonical string
// shape loadField returns for that field. The conversion is field-
// dependent because the same string literal "2026-05-26T12:00:00Z"
// means different things compared against `ts` (parse as time, take
// nanos) vs against `message` (verbatim string).
func loadLiteral(f *FieldRef, lit *Literal) (string, error) {
	switch f.Name {
	case "ts":
		if lit.Int != nil {
			return fmt.Sprintf("%d", *lit.Int), nil
		}
		if lit.Str == nil {
			return "", fmt.Errorf("query: ts literal must be string or int")
		}
		t, err := time.Parse(time.RFC3339Nano, *lit.Str)
		if err != nil {
			t, err = time.Parse(time.RFC3339, *lit.Str)
			if err != nil {
				return "", fmt.Errorf("query: ts literal %q not RFC3339", *lit.Str)
			}
		}
		return fmt.Sprintf("%d", t.UnixNano()), nil
	case "doc_id":
		if lit.Int == nil {
			return "", fmt.Errorf("query: doc_id literal must be int")
		}
		return fmt.Sprintf("%d", *lit.Int), nil
	default:
		// String fields: literal must be string. Integer compared
		// against a string column is a type error.
		if lit.Str == nil {
			return "", fmt.Errorf("query: literal for field %q must be a quoted string", f.Name)
		}
		return *lit.Str, nil
	}
}
