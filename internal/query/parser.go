// Package query parses and executes a tiny SQL-like dialect against
// the storage engine.
//
// The dialect is intentionally minimal — this is a scan + filter +
// project executor, not a query optimizer. Predicate pushdown,
// inverted-index acceleration, ORDER BY, GROUP BY, joins, and
// subqueries are explicitly out of scope.
//
// Grammar (informal):
//
//	SELECT  '*' | <col> (',' <col>)*
//	FROM    'logs'
//	[WHERE  <expr>]
//	[LIMIT  <int>]
//
//	<expr>   := <orExpr>
//	<orExpr> := <andExpr> ('OR' <andExpr>)*
//	<andExpr>:= <cmpExpr> ('AND' <cmpExpr>)*
//	<cmpExpr>:= '(' <expr> ')' | <field> <op> <literal>
//	<op>     := '=' | '!='
//	<field>  := 'ts' | 'tenant_id' | 'source' | 'message' | 'doc_id'
//	          | 'fields.' IDENT
//	<literal>:= STRING | INT | TIMESTAMP-STRING
//
// `ts` and timestamp literals: literals are RFC3339 strings; comparisons
// against `ts` compare unix-nanos under the hood. `doc_id` is integer.
// `tenant_id`, `source`, `message`, and `fields.*` are strings.
package query

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// Statement is the parsed root.
type Statement struct {
	Select  *SelectClause `parser:"@@"`
	From    string        `parser:"'FROM' @Ident"`
	Where   *Expr         `parser:"('WHERE' @@)?"`
	Limit   *int          `parser:"('LIMIT' @Int)?"`
}

// SelectClause is either "*" (all columns) or a list of column names.
// `fields.<key>` is represented as a column with a Map field set.
type SelectClause struct {
	Star    bool       `parser:"'SELECT' (  @'*'"`
	Columns []*Column  `parser:"            | @@ (',' @@)* )"`
}

// Column is one entry in the SELECT list. Map is non-empty for
// `fields.<key>`; otherwise Name is one of the fixed top-level columns.
type Column struct {
	Name string `parser:"@Ident"`
	Map  string `parser:"( '.' @Ident )?"`
}

// Expr -> orExpr.
type Expr struct {
	Or *OrExpr `parser:"@@"`
}

type OrExpr struct {
	And  *AndExpr   `parser:"@@"`
	Rest []*AndExpr `parser:"( 'OR' @@ )*"`
}

type AndExpr struct {
	Cmp  *CmpExpr   `parser:"@@"`
	Rest []*CmpExpr `parser:"( 'AND' @@ )*"`
}

// CmpExpr is either a parenthesized subexpression or a leaf comparison.
type CmpExpr struct {
	Sub  *Expr     `parser:"  '(' @@ ')'"`
	Cmp  *Compare  `parser:"| @@"`
}

type Compare struct {
	Field *FieldRef `parser:"@@"`
	Op    string    `parser:"@( '=' | '!' '=' )"`
	Value *Literal  `parser:"@@"`
}

// FieldRef names a column. Map is non-empty for `fields.<key>`.
type FieldRef struct {
	Name string `parser:"@Ident"`
	Map  string `parser:"( '.' @Ident )?"`
}

// Literal is one of: quoted string, integer, or the absence thereof.
// Strings that parse as RFC3339 are interpreted as timestamps when
// compared against `ts`; the evaluator does the conversion.
type Literal struct {
	Str *string `parser:"@String"`
	Int *int64  `parser:"| @Int"`
}

// === Lexer + parser singletons ===

var sqlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Whitespace", Pattern: `\s+`},
	{Name: "Keyword", Pattern: `(?i)\b(SELECT|FROM|WHERE|LIMIT|AND|OR)\b`},
	{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
	{Name: "String", Pattern: `'(?:[^'\\]|\\.)*'`},
	{Name: "Int", Pattern: `-?\d+`},
	{Name: "Punct", Pattern: `[*=!.(),]`},
})

var parser = participle.MustBuild[Statement](
	participle.Lexer(sqlLexer),
	participle.Elide("Whitespace"),
	participle.Unquote("String"),
	participle.CaseInsensitive("Keyword"),
	// Keywords are also valid identifiers in the lexer; map them
	// back so e.g. "from" parses as the SELECT/FROM keyword.
	participle.UseLookahead(2),
)

// Parse turns SQL text into a Statement. Returns a wrapped error on
// any syntax or grammar failure; the error string starts with "query:".
func Parse(sql string) (*Statement, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("query: empty statement")
	}
	stmt, err := parser.ParseString("", sql)
	if err != nil {
		return nil, fmt.Errorf("query: parse: %w", err)
	}
	if !strings.EqualFold(stmt.From, "logs") {
		return nil, fmt.Errorf("query: FROM must be 'logs', got %q", stmt.From)
	}
	if stmt.Limit != nil && *stmt.Limit < 0 {
		return nil, fmt.Errorf("query: LIMIT must be non-negative")
	}
	return stmt, nil
}
