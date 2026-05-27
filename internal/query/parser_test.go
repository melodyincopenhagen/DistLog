package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse_AcceptsValid(t *testing.T) {
	cases := []struct {
		sql       string
		wantStar  bool
		wantCols  int
		wantWhere bool
		wantLimit *int
	}{
		{
			sql:      "SELECT * FROM logs",
			wantStar: true,
		},
		{
			sql:      "SELECT message FROM logs",
			wantCols: 1,
		},
		{
			sql:       "SELECT message, source FROM logs WHERE source = 'api'",
			wantCols:  2,
			wantWhere: true,
		},
		{
			sql:       "SELECT * FROM logs WHERE fields.level = 'error' AND tenant_id = 't1' LIMIT 10",
			wantStar:  true,
			wantWhere: true,
			wantLimit: intPtr(10),
		},
		{
			sql:       "SELECT ts, message FROM logs WHERE ts = '2026-05-26T12:00:00Z'",
			wantCols:  2,
			wantWhere: true,
		},
		{
			sql:       "SELECT * FROM logs WHERE (source = 'a' OR source = 'b') AND message != ''",
			wantStar:  true,
			wantWhere: true,
		},
		{
			sql:       "select * from logs where doc_id = 42",
			wantStar:  true,
			wantWhere: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			require.NoError(t, err)
			require.NotNil(t, stmt.Select)
			require.Equal(t, tc.wantStar, stmt.Select.Star)
			if !tc.wantStar {
				require.Len(t, stmt.Select.Columns, tc.wantCols)
			}
			require.Equal(t, tc.wantWhere, stmt.Where != nil)
			if tc.wantLimit != nil {
				require.NotNil(t, stmt.Limit)
				require.Equal(t, *tc.wantLimit, *stmt.Limit)
			}
		})
	}
}

func TestParse_RejectsInvalid(t *testing.T) {
	cases := []string{
		"",                                       // empty
		"DROP TABLE logs",                        // not a SELECT
		"SELECT * FROM users",                    // wrong table
		"SELECT FROM logs",                       // missing columns
		"SELECT * FROM logs WHERE",               // dangling WHERE
		"SELECT * FROM logs LIMIT -5",            // negative limit
		"SELECT * FROM logs WHERE message =",     // missing value
		"SELECT * FROM logs WHERE = 'x'",         // missing field
		"SELECT * FROM logs WHERE message ~ 'x'", // unsupported op
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			require.Error(t, err)
		})
	}
}

func intPtr(i int) *int { return &i }
