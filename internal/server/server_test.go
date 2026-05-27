package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yuexishen/distlog/internal/engine"
)

// testHarness brings up engine + server on a random port. Caller gets
// a base URL ("http://127.0.0.1:PORT") and a cleanup func.
type testHarness struct {
	baseURL string
	engine  *engine.Engine
	cancel  context.CancelFunc
	done    chan struct{}
}

func newHarness(t *testing.T, eng *engine.Engine, cfg Config) *testHarness {
	t.Helper()
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 2 * time.Second
	}
	if cfg.WriteRequestTimeout == 0 {
		cfg.WriteRequestTimeout = 2 * time.Second
	}
	s := New(cfg, eng)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.ServeListener(ctx, ln)
		close(done)
	}()
	return &testHarness{
		baseURL: "http://" + ln.Addr().String(),
		engine:  eng,
		cancel:  cancel,
		done:    done,
	}
}

func (h *testHarness) close(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func openEngine(t *testing.T) *engine.Engine {
	t.Helper()
	eng, err := engine.Open(engine.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func TestIngest_RoundTrip(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	body := strings.NewReader(`{
		"ts": "2026-05-26T12:00:00Z",
		"tenant_id": "t1",
		"source": "host-1",
		"message": "hello",
		"fields": {"level": "info"}
	}`)
	resp, err := http.Post(h.baseURL+"/ingest", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ingResp ingestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ingResp))
	require.NotZero(t, ingResp.DocID)

	// Read it back.
	getResp, err := http.Get(fmt.Sprintf("%s/logs/%d", h.baseURL, ingResp.DocID))
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var rec logRecordResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&rec))
	require.Equal(t, ingResp.DocID, rec.DocID)
	require.Equal(t, "t1", rec.TenantID)
	require.Equal(t, "host-1", rec.Source)
	require.Equal(t, "hello", rec.Message)
	require.Equal(t, "2026-05-26T12:00:00Z", rec.Timestamp)
	require.Equal(t, "info", rec.Fields["level"])
}

func TestIngest_TimestampAsUnixNanos(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	body := strings.NewReader(`{"ts": 1700000000000000000, "message": "x"}`)
	resp, err := http.Post(h.baseURL+"/ingest", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIngest_OmittedTimestampDefaultsToNow(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	before := time.Now().UnixNano()
	body := strings.NewReader(`{"message": "x"}`)
	resp, err := http.Post(h.baseURL+"/ingest", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ingResp ingestResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ingResp))

	// Fetch and check ts is sensible (within a few seconds of "now").
	getResp, err := http.Get(fmt.Sprintf("%s/logs/%d", h.baseURL, ingResp.DocID))
	require.NoError(t, err)
	defer getResp.Body.Close()
	var rec logRecordResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&rec))
	ts, err := time.Parse(time.RFC3339Nano, rec.Timestamp)
	require.NoError(t, err)
	require.GreaterOrEqual(t, ts.UnixNano(), before)
	require.LessOrEqual(t, ts.UnixNano(), time.Now().UnixNano())
}

func TestIngest_RejectsInvalidJSON(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Post(h.baseURL+"/ingest", "application/json", strings.NewReader(`{not json}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestIngest_RejectsUnknownFields(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Post(h.baseURL+"/ingest", "application/json",
		strings.NewReader(`{"messag": "typo"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGet_NotFound(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Get(h.baseURL + "/logs/999999")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGet_InvalidDocID(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Get(h.baseURL + "/logs/abc")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGet_DocIDZeroRejected(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Get(h.baseURL + "/logs/0")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestQuery_EndToEnd(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	// Ingest a handful of records.
	for _, p := range []struct {
		src, msg, level string
	}{
		{"api", "hello", "info"},
		{"api", "boom", "error"},
		{"db", "slow query", "warn"},
		{"api", "another error", "error"},
	} {
		body, _ := json.Marshal(map[string]any{
			"source":  p.src,
			"message": p.msg,
			"fields":  map[string]string{"level": p.level},
		})
		resp, err := http.Post(h.baseURL+"/ingest", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		resp.Body.Close()
	}

	// SELECT errors only.
	queryBody, _ := json.Marshal(map[string]string{
		"sql": "SELECT message, source FROM logs WHERE fields.level = 'error'",
	})
	resp, err := http.Post(h.baseURL+"/query", "application/json", bytes.NewReader(queryBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Columns []string `json:"columns"`
		Rows    []struct {
			DocID  uint64                 `json:"doc_id"`
			Values map[string]interface{} `json:"values"`
		} `json:"rows"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, []string{"message", "source"}, result.Columns)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "boom", result.Rows[0].Values["message"])
	require.Equal(t, "another error", result.Rows[1].Values["message"])
}

func TestQuery_BadSQLReturns400(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	body, _ := json.Marshal(map[string]string{"sql": "DROP TABLE logs"})
	resp, err := http.Post(h.baseURL+"/query", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestQuery_LimitTruncates(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	for i := 0; i < 10; i++ {
		body, _ := json.Marshal(map[string]string{"message": "x"})
		resp, err := http.Post(h.baseURL+"/ingest", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		resp.Body.Close()
	}

	body, _ := json.Marshal(map[string]string{"sql": "SELECT message FROM logs LIMIT 3"})
	resp, err := http.Post(h.baseURL+"/query", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Rows []any `json:"rows"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Rows, 3)
}

func TestConsole_RootServesHTML(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Get(h.baseURL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	ct := resp.Header.Get("Content-Type")
	require.Contains(t, ct, "html")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "DistLog console")
}

func TestConsole_ServesCSSAndJS(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	for _, path := range []string{"/console.css", "/console.js"} {
		resp, err := http.Get(h.baseURL + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "path %s should serve", path)
	}
}

func TestAPI_AliasesAndLegacyPathsBothWork(t *testing.T) {
	// Both /api/ingest and the legacy /ingest must accept writes.
	// Both /api/healthz and /healthz must return the same JSON shape.
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	for _, path := range []string{"/ingest", "/api/ingest"} {
		body := strings.NewReader(`{"message":"alias-test"}`)
		resp, err := http.Post(h.baseURL+path, "application/json", body)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "POST %s", path)
	}
	for _, path := range []string{"/healthz", "/api/healthz"} {
		resp, err := http.Get(h.baseURL + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
	}
}

func TestHealthz_HealthyEngine(t *testing.T) {
	h := newHarness(t, openEngine(t), Config{})
	defer h.close(t)

	resp, err := http.Get(h.baseURL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var hr healthResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&hr))
	require.Equal(t, "ok", hr.Status)
	require.False(t, hr.Fatal)
	require.False(t, hr.Closed)
}

// TestWriteEngineError_MappingTable is a unit test for the
// engine-error -> HTTP-status mapping. Keeps the mapping rules
// (which are the contract clients depend on) testable without
// needing to put a real engine into each terminal state.
func TestWriteEngineError_MappingTable(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantStatus     int
		wantRetryAfter string
		wantBodyMatch  string
	}{
		{
			name:           "backpressure -> 429 + Retry-After",
			err:            engine.ErrBackpressure,
			wantStatus:     http.StatusTooManyRequests,
			wantRetryAfter: "1",
			wantBodyMatch:  "backpressure",
		},
		{
			name:          "closed -> 503",
			err:           engine.ErrClosed,
			wantStatus:    http.StatusServiceUnavailable,
			wantBodyMatch: "closed",
		},
		{
			name:          "fatal -> 500",
			err:           engine.ErrEngineFatal,
			wantStatus:    http.StatusInternalServerError,
			wantBodyMatch: "fatal",
		},
		{
			name:          "unknown -> 500",
			err:           fmt.Errorf("disk on fire"),
			wantStatus:    http.StatusInternalServerError,
			wantBodyMatch: "disk on fire",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecordingResponse()
			writeEngineError(rec, tc.err)
			require.Equal(t, tc.wantStatus, rec.status)
			require.Equal(t, tc.wantRetryAfter, rec.header.Get("Retry-After"))
			require.Contains(t, rec.body.String(), tc.wantBodyMatch)
		})
	}
}

// recordingResponse is a minimal http.ResponseWriter for the unit test
// above. httptest.ResponseRecorder would work too but pulls a larger
// dependency surface; this stays in-package.
type recordingResponse struct {
	header http.Header
	body   *bytes.Buffer
	status int
}

func newRecordingResponse() *recordingResponse {
	return &recordingResponse{header: http.Header{}, body: &bytes.Buffer{}}
}

func (r *recordingResponse) Header() http.Header        { return r.header }
func (r *recordingResponse) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recordingResponse) WriteHeader(s int)          { r.status = s }
