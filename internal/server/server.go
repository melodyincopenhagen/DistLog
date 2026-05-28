// Package server is the HTTP boundary in front of the storage engine.
//
// Endpoints:
//
//	POST /api/ingest        single LogRecord JSON -> {"doc_id": N}
//	GET  /api/logs/{docID}  -> LogRecord JSON, or 404
//	POST /api/query         {"sql": "..."} -> {"columns": [...], "rows": [...]}
//	GET  /api/healthz       -> engine stats JSON
//	GET  /                  embedded HTML/JS console for the above
//
// The legacy paths POST /ingest, GET /logs/{docID}, POST /query, and
// GET /healthz remain registered as backwards-compatible aliases —
// existing curl examples, scripts, and clients continue to work.
//
// Error mapping (chosen to be HTTP-standard so generic client retry
// logic does the right thing):
//
//	engine.ErrBackpressure -> 429 Too Many Requests + Retry-After: 1
//	engine.ErrClosed       -> 503 Service Unavailable (shutting down)
//	engine.ErrEngineFatal  -> 500 Internal Server Error
//	JSON decode errors     -> 400 Bad Request
//	docID parse errors     -> 400 Bad Request
//	SQL parse errors       -> 400 Bad Request
//	SQL eval errors        -> 400 Bad Request (caller's query is wrong)
//	Get not-found          -> 404 Not Found
//	any other engine error -> 500 Internal Server Error
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yuexishen/distlog/internal/engine"
	"github.com/yuexishen/distlog/internal/query"
	"github.com/yuexishen/distlog/internal/types"
)

// Config is the slice of process config the server takes ownership of.
// Mirrors config.ServerConfig but the server does not import config —
// keeping the dependency direction one-way (cmd -> config + server).
type Config struct {
	ListenAddr          string
	ShutdownTimeout     time.Duration
	WriteRequestTimeout time.Duration

	// Tokens maps bearer token -> tenant_id. Empty map means "auth
	// disabled" — requests with no Authorization header succeed and
	// are tagged as the anonymous tenant.
	Tokens map[string]string
	// AnonymousTenant is the tenant assigned when auth is disabled.
	// Pass config.AnonymousTenant. Kept here so internal/server does
	// not import internal/config.
	AnonymousTenant string
}

// authMode reports whether at least one token is configured.
func (c Config) authEnabled() bool { return len(c.Tokens) > 0 }

// Server is the HTTP front end. New constructs one bound to an Engine;
// Serve runs it until Shutdown is called.
type Server struct {
	cfg    Config
	engine *engine.Engine
	mux    *http.ServeMux
	httpd  *http.Server
}

// ctxTenantKey is the context key that auth middleware uses to stash
// the resolved tenant for the downstream handler. Unexported and typed
// so it cannot collide with any other package's context keys.
type ctxKey int

const ctxTenantKey ctxKey = iota

// New constructs a Server. The Engine must be Open. The Server does
// not own the Engine — Close the Engine separately from Shutdown.
func New(cfg Config, eng *engine.Engine) *Server {
	s := &Server{
		cfg:    cfg,
		engine: eng,
		mux:    http.NewServeMux(),
	}
	s.routes()
	s.httpd = &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: s.mux,
		// Conservative timeouts. Read/Write timeouts here are HTTP
		// transport-level, distinct from WriteRequestTimeout which
		// scopes the engine call itself.
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) routes() {
	// auth wraps a handler with bearer-token resolution. Tenanted
	// handlers — Ingest, Get, Query — go through it. Healthz does
	// not (operational probe).
	auth := s.authMiddleware

	// Canonical /api/* routes. Console fetches these.
	s.mux.HandleFunc("POST /api/ingest", auth(s.handleIngest))
	s.mux.HandleFunc("GET /api/logs/{docID}", auth(s.handleGet))
	s.mux.HandleFunc("POST /api/query", auth(s.handleQuery))
	s.mux.HandleFunc("GET /api/healthz", s.handleHealth)

	// Legacy unprefixed aliases. Kept indefinitely — README curl
	// examples and any existing clients depend on them. The handlers
	// are identical references, so semantics cannot drift.
	s.mux.HandleFunc("POST /ingest", auth(s.handleIngest))
	s.mux.HandleFunc("GET /logs/{docID}", auth(s.handleGet))
	s.mux.HandleFunc("POST /query", auth(s.handleQuery))
	s.mux.HandleFunc("GET /healthz", s.handleHealth)

	// Static console served from embedded files. The console is a
	// single-page vanilla-JS app; no build step.
	s.mux.Handle("GET /", http.FileServer(http.FS(consoleFS)))
}

// authMiddleware resolves the request's tenant from the Authorization
// header and stashes it in the request context.
//
//   - Auth disabled (no tokens configured): every request is accepted
//     and tagged as cfg.AnonymousTenant. Authorization header is
//     allowed but ignored.
//   - Auth enabled: requests MUST present "Authorization: Bearer <t>"
//     where t is a known token. Missing/malformed/unknown -> 401.
//
// Tenant is stored as a string in request context under ctxTenantKey;
// handlers retrieve it via tenantFromCtx.
func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var tenant string
		if !s.cfg.authEnabled() {
			tenant = s.cfg.AnonymousTenant
		} else {
			h := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) {
				writeError(w, http.StatusUnauthorized, "missing or malformed Authorization: Bearer header")
				return
			}
			token := h[len(prefix):]
			t, ok := s.cfg.Tokens[token]
			if !ok {
				writeError(w, http.StatusUnauthorized, "invalid token")
				return
			}
			tenant = t
		}
		ctx := context.WithValue(r.Context(), ctxTenantKey, tenant)
		next(w, r.WithContext(ctx))
	}
}

// tenantFromCtx pulls the tenant the middleware resolved. The
// middleware always sets it on every reachable handler path; the
// fallback to AnonymousTenant defends against accidental routing
// changes that bypass the middleware.
func (s *Server) tenantFromCtx(r *http.Request) string {
	if v := r.Context().Value(ctxTenantKey); v != nil {
		if t, ok := v.(string); ok && t != "" {
			return t
		}
	}
	return s.cfg.AnonymousTenant
}

// Serve binds and serves until ctx is canceled or ListenAndServe
// returns an error other than http.ErrServerClosed. Returns nil on
// clean shutdown.
func (s *Server) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := s.httpd.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := s.httpd.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server: shutdown: %w", err)
		}
		<-errCh
		return nil
	}
}

// ServeListener is like Serve but takes an already-bound listener.
// Used by tests so they can use port 0 and discover the chosen port.
func (s *Server) ServeListener(ctx context.Context, ln net.Listener) error {
	s.httpd.Addr = ln.Addr().String()
	errCh := make(chan error, 1)
	go func() {
		err := s.httpd.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		if err := s.httpd.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server: shutdown: %w", err)
		}
		<-errCh
		return nil
	}
}

// === Wire formats ===

// ingestRequest is the on-wire shape for POST /ingest. Timestamp is
// accepted as either an RFC3339 string ("2026-05-26T12:00:00Z") or a
// raw unix-nanos integer. Either is fine; the second form is for
// machine-generated traffic that already has nanos.
type ingestRequest struct {
	Timestamp jsonTimestamp     `json:"ts"`
	TenantID  string            `json:"tenant_id"`
	Source    string            `json:"source"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

type ingestResponse struct {
	DocID types.DocID `json:"doc_id"`
}

// logRecordResponse is the on-wire shape for GET /logs/{docID}.
// Timestamp is always emitted as RFC3339-with-nanos so humans reading
// curl output get something legible.
type logRecordResponse struct {
	DocID     types.DocID       `json:"doc_id"`
	Timestamp string            `json:"ts"`
	TenantID  string            `json:"tenant_id"`
	Source    string            `json:"source"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// errorResponse is the on-wire shape for any non-2xx body.
type errorResponse struct {
	Error string `json:"error"`
}

// === Handlers ===

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	rec := &types.LogRecord{
		Timestamp: types.Timestamp(req.Timestamp),
		TenantID:  req.TenantID,
		Source:    req.Source,
		Message:   req.Message,
		Fields:    req.Fields,
	}
	if rec.Timestamp == 0 {
		rec.Timestamp = types.Now()
	}
	// Tenant override: regardless of what the client sent, the
	// authenticated tenant wins. This is the security invariant —
	// clients cannot forge tenancy. Silently overwriting (rather than
	// 400'ing on mismatch) lets old scripts that don't know about
	// auth keep working when the server is in auth-disabled mode.
	rec.TenantID = s.tenantFromCtx(r)

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.WriteRequestTimeout)
	defer cancel()
	docID, err := s.engine.Write(ctx, rec)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ingestResponse{DocID: docID})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("docID")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid docID %q: must be a positive integer", idStr))
		return
	}
	if id == 0 {
		writeError(w, http.StatusBadRequest, "invalid docID 0: reserved")
		return
	}
	rec, ok, err := s.engine.Get(types.DocID(id))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("docID %d not found", id))
		return
	}
	// Tenant scope: a record belongs to one tenant. If the caller's
	// tenant doesn't match, treat as 404 (not 403) so existence
	// information doesn't leak across tenants.
	if rec.TenantID != s.tenantFromCtx(r) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("docID %d not found", id))
		return
	}
	resp := logRecordResponse{
		DocID:     types.DocID(id),
		Timestamp: rec.Timestamp.Time().UTC().Format(time.RFC3339Nano),
		TenantID:  rec.TenantID,
		Source:    rec.Source,
		Message:   rec.Message,
		Fields:    rec.Fields,
	}
	writeJSON(w, http.StatusOK, resp)
}

// queryRequest is the on-wire shape for POST /query.
type queryRequest struct {
	SQL string `json:"sql"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	stmt, err := query.Parse(req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Tenant scope: rewrite the parsed AST so the executor's WHERE
	// is the caller's-WHERE-AND-tenant_id-equals-callers-tenant. This
	// happens after parse so the SQL the user typed is never modified
	// at the string layer (no quoting / escaping concerns).
	stmt.Where = query.AndTenantFilter(stmt.Where, s.tenantFromCtx(r))
	// Query execution doesn't use WriteRequestTimeout — that knob is
	// for the write path's backpressure stall. Use the request ctx
	// directly; clients that want fail-fast set their own client-side
	// timeout.
	res, err := query.Execute(r.Context(), s.engine, stmt)
	if err != nil {
		// Evaluator errors (unknown field, bad ts literal) are caller
		// errors -> 400. ErrClosed surfaces through Scan.
		if errors.Is(err, engine.ErrClosed) {
			writeEngineError(w, err)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// healthResponse is the on-wire shape for /healthz. Kept separate
// from engine.Stats so adding fields to internal stats doesn't
// silently change the HTTP contract.
type healthResponse struct {
	Status                       string `json:"status"` // "ok" | "unhealthy"
	ActiveMemTableSize           int64  `json:"active_memtable_size"`
	FrozenMemTableCount          int    `json:"frozen_memtable_count"`
	SSTableCount                 int    `json:"sstable_count"`
	CorruptedSSTablesQuarantined int    `json:"corrupted_sstables_quarantined"`
	NextDocID                    uint64 `json:"next_doc_id"`
	Fatal                        bool   `json:"fatal"`
	Closed                       bool   `json:"closed"`
	LastScanPrunedSSTables       int    `json:"last_scan_pruned_sstables"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats := s.engine.Stats()
	status := http.StatusOK
	healthStatus := "ok"
	if stats.Fatal || stats.Closed {
		status = http.StatusServiceUnavailable
		healthStatus = "unhealthy"
	}
	writeJSON(w, status, healthResponse{
		Status:                       healthStatus,
		ActiveMemTableSize:           stats.ActiveMemTableSize,
		FrozenMemTableCount:          stats.FrozenMemTableCount,
		SSTableCount:                 stats.SSTableCount,
		CorruptedSSTablesQuarantined: stats.CorruptedSSTablesQuarantined,
		NextDocID:                    uint64(stats.NextDocID),
		Fatal:                        stats.Fatal,
		Closed:                       stats.Closed,
		LastScanPrunedSSTables:       stats.LastScanPrunedSSTables,
	})
}

// === Helpers ===

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrBackpressure):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, engine.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, engine.ErrEngineFatal):
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// === Timestamp JSON unmarshaling ===

// jsonTimestamp accepts either an RFC3339 string or a unix-nanos
// integer and unmarshals to a types.Timestamp value (nanos since
// epoch). Marshaling is delegated to logRecordResponse, which always
// emits RFC3339Nano.
type jsonTimestamp types.Timestamp

func (t *jsonTimestamp) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*t = 0
		return nil
	}
	// Integer form: not surrounded by quotes.
	if b[0] != '"' {
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return fmt.Errorf("ts: not a unix-nanos integer: %w", err)
		}
		*t = jsonTimestamp(n)
		return nil
	}
	// String form: RFC3339 (with or without nanos).
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		*t = 0
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		// Fallback to RFC3339 without nanos.
		parsed, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return fmt.Errorf("ts: not RFC3339: %w", err)
		}
	}
	*t = jsonTimestamp(parsed.UnixNano())
	return nil
}
