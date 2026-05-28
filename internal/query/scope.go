package query

// AndTenantFilter returns a new Expr equivalent to
//
//	(orig) AND tenant_id = <tenant>
//
// constructed against the parsed AST rather than as a string rewrite.
// String rewriting would have to quote and escape the tenant id; AST
// rewriting cannot inject SQL syntax even if the tenant id contains
// hostile characters (it'll be a string literal in a Compare node).
//
// If orig is nil (no caller-supplied WHERE), the returned Expr is
// just the tenant predicate. If orig already contains an OR at the
// top level, that OR is parenthesized by being made the LHS of the
// AND chain — equivalent to wrapping (caller WHERE) in parens.
//
// Tenant scoping must always be applied; do not skip it on the
// special case of caller-supplied "WHERE tenant_id = X" — the AND
// chain is still correct (it tightens X to the authenticated tenant
// if they match; gives empty results if they don't, which is the
// intended behavior).
func AndTenantFilter(orig *Expr, tenant string) *Expr {
	tenantValue := tenant
	tenantCmp := &CmpExpr{
		Cmp: &Compare{
			Field: &FieldRef{Name: "tenant_id"},
			Op:    "=",
			Value: &Literal{Str: &tenantValue},
		},
	}

	if orig == nil {
		return &Expr{Or: &OrExpr{And: &AndExpr{Cmp: tenantCmp}}}
	}

	// The caller's WHERE goes into a parenthesized CmpExpr so any
	// internal OR retains its scope.
	wrapped := &CmpExpr{Sub: orig}

	return &Expr{
		Or: &OrExpr{
			And: &AndExpr{
				Cmp:  wrapped,
				Rest: []*CmpExpr{tenantCmp},
			},
		},
	}
}
