// Package server is the HTTP boundary in front of the storage engine.
//
// Three endpoints:
//
//	POST /ingest          single LogRecord JSON -> {"doc_id": N}
//	GET  /logs/{docID}    -> LogRecord JSON, or 404
//	GET  /healthz         -> engine.Stats() JSON
//
// Error mapping (chosen to be HTTP-standard so generic client retry
// logic does the right thing):
//
//	engine.ErrBackpressure -> 429 Too Many Requests + Retry-After: 1
//	engine.ErrClosed       -> 503 Service Unavailable (shutting down)
//	engine.ErrEngineFatal  -> 500 Internal Server Error
//	JSON decode errors     -> 400 Bad Request
//	docID parse errors     -> 400 Bad Request
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
	"github.com/yuexishen/distlog/internal/types"
)

// Config is the slice of process config the server takes ownership of.
// Mirrors config.ServerConfig but the server does not import config —
// keeping the dependency direction one-way (cmd -> config + server).
type Config struct {
	ListenAddr          string
	ShutdownTimeout     time.Duration
	WriteRequestTimeout time.Duration
}

// Server is the HTTP front end. New constructs one bound to an Engine;
// Serve runs it until Shutdown is called.
type Server struct {
	cfg    Config
	engine *engine.Engine
	mux    *http.ServeMux
	httpd  *http.Server
}

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
	s.mux.HandleFunc("POST /ingest", s.handleIngest)
	s.mux.HandleFunc("GET /logs/{docID}", s.handleGet)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
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
