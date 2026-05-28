package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Stage K — token auth + per-tenant isolation.

func TestAuth_Disabled_NoTokenAccepted(t *testing.T) {
	// No tokens in cfg -> auth disabled. Requests without a header
	// succeed and the record gets the anonymous tenant.
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Post(h.baseURL+"/api/ingest", "application/json",
		strings.NewReader(`{"message":"hello"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_Enabled_MissingHeaderRejected(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{"tok-a": "tenant-a"},
	})
	defer h.close(t)

	resp, err := http.Post(h.baseURL+"/api/ingest", "application/json",
		strings.NewReader(`{"message":"x"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_Enabled_UnknownTokenRejected(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{"tok-a": "tenant-a"},
	})
	defer h.close(t)

	req, _ := http.NewRequest("POST", h.baseURL+"/api/ingest",
		strings.NewReader(`{"message":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer not-a-real-token")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_Enabled_HealthzNotGated(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{"tok-a": "tenant-a"},
	})
	defer h.close(t)

	// No header at all — must still get 200.
	resp, err := http.Get(h.baseURL + "/api/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_IngestOverridesBodyTenant(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{"tok-a": "tenant-a"},
	})
	defer h.close(t)

	// Client claims tenant-b in body, but their token is tenant-a.
	body := strings.NewReader(`{"tenant_id":"tenant-b","message":"x"}`)
	req, _ := http.NewRequest("POST", h.baseURL+"/api/ingest", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-a")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ing ingestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ing))

	// Read it back via /query (also tenant-scoped). The tenant must
	// be tenant-a, not the forged tenant-b.
	res := runQuery(t, h, "tok-a", "SELECT tenant_id FROM logs")
	require.Len(t, res.Rows, 1)
	require.Equal(t, "tenant-a", res.Rows[0].Values["tenant_id"])
}

func TestAuth_QueryIsolatedAcrossTenants(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{
			"tok-a": "tenant-a",
			"tok-b": "tenant-b",
		},
	})
	defer h.close(t)

	// Each tenant writes a unique message.
	ingestAs(t, h, "tok-a", `{"message":"alpha"}`)
	ingestAs(t, h, "tok-b", `{"message":"bravo"}`)
	ingestAs(t, h, "tok-a", `{"message":"alpha-2"}`)

	resA := runQuery(t, h, "tok-a", "SELECT message FROM logs")
	require.Len(t, resA.Rows, 2)
	for _, r := range resA.Rows {
		msg := r.Values["message"].(string)
		require.True(t, strings.HasPrefix(msg, "alpha"), "tenant-a should not see tenant-b's records, got %q", msg)
	}

	resB := runQuery(t, h, "tok-b", "SELECT message FROM logs")
	require.Len(t, resB.Rows, 1)
	require.Equal(t, "bravo", resB.Rows[0].Values["message"])
}

func TestAuth_GetCrossTenantIs404(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{
			"tok-a": "tenant-a",
			"tok-b": "tenant-b",
		},
	})
	defer h.close(t)

	// tenant-a writes one record; we get its docID from the response.
	resp := ingestAs(t, h, "tok-a", `{"message":"private to a"}`)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ing ingestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ing))

	// tenant-b tries to fetch it by docID -> 404, not 403 (don't leak
	// existence).
	req, _ := http.NewRequest("GET", h.baseURL+"/api/logs/"+itoa(uint64(ing.DocID)), nil)
	req.Header.Set("Authorization", "Bearer tok-b")
	got, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer got.Body.Close()
	require.Equal(t, http.StatusNotFound, got.StatusCode)
}

func TestAuth_QueryUserWhereStillScoped(t *testing.T) {
	// Even with a OR-y caller WHERE, the tenant filter must apply.
	h := newHarness(t, openEngine(t), Config{
		Tokens: map[string]string{
			"tok-a": "tenant-a",
			"tok-b": "tenant-b",
		},
	})
	defer h.close(t)

	ingestAs(t, h, "tok-a", `{"source":"api","message":"a1"}`)
	ingestAs(t, h, "tok-b", `{"source":"api","message":"b1"}`)
	ingestAs(t, h, "tok-b", `{"source":"db","message":"b2"}`)

	// tenant-a queries with an OR that, without scoping, would match
	// all three records.
	res := runQuery(t, h, "tok-a", "SELECT message FROM logs WHERE source = 'api' OR source = 'db'")
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a1", res.Rows[0].Values["message"])
}

// === helpers ===

func ingestAs(t *testing.T, h *testHarness, token, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", h.baseURL+"/api/ingest", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "ingest as %s should succeed", token)
	return resp
}

type queryResult struct {
	Columns []string `json:"columns"`
	Rows    []struct {
		DocID  uint64                 `json:"doc_id"`
		Values map[string]interface{} `json:"values"`
	} `json:"rows"`
}

func runQuery(t *testing.T, h *testHarness, token, sql string) queryResult {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	req, _ := http.NewRequest("POST", h.baseURL+"/api/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "query as %s should succeed", token)
	var out queryResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// itoa avoids strconv import in this file for one call.
func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
