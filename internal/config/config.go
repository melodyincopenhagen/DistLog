// Package config defines the on-disk YAML configuration schema and the
// loader that produces a typed Config from a file path.
//
// All configuration enters the process through this package — no other
// code reads environment variables or parses YAML. Defaults live in
// applyDefaults, never in the YAML files themselves; YAML is purely an
// override layer.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the parsed, defaulted configuration for a single distlog
// process. Subsystems take the sub-struct relevant to them rather than
// the whole Config — this keeps the dependency surface narrow.
type Config struct {
	Server ServerConfig `yaml:"server"`
	Engine EngineConfig `yaml:"engine"`
	Auth   AuthConfig   `yaml:"auth"`
}

// AuthConfig configures token-based tenant authentication.
//
// If Tokens is empty, the server runs in "auth disabled" mode: requests
// without an Authorization header are accepted and treated as tenant
// "_anonymous". This keeps single-user dev setups unbreakingly simple.
//
// If Tokens is non-empty, the server requires Authorization: Bearer
// <token> on /api/ingest and /api/query and /api/logs/{docID}. Tokens
// not in the map -> 401 Unauthorized. /api/healthz is never gated
// (operational probe). The console's login page accepts a token and
// stashes it in sessionStorage.
//
// Storage model on disk does not include a tenant index — multi-tenancy
// is a query-time filter, not a partitioning concern. See
// docs/DECISIONS.md ADR-010.
type AuthConfig struct {
	// Tokens maps bearer tokens to tenant IDs. The token value MUST
	// be opaque (not the tenant_id itself) so leaking a tenant_id
	// in logs doesn't compromise auth.
	//
	// Tokens here are STATIC — config reload is not supported in this
	// stage. Rotation requires a process restart.
	Tokens map[string]string `yaml:"tokens"`
}

// AnonymousTenant is the tenant identity assigned to requests in
// "auth disabled" mode (no tokens configured) and used as the default
// tenant on Write when no body tenant_id is supplied.
const AnonymousTenant = "_anonymous"

// ServerConfig configures the HTTP server.
type ServerConfig struct {
	// ListenAddr is the host:port the HTTP server binds to.
	// Default: ":8080".
	ListenAddr string `yaml:"listen_addr"`

	// ShutdownTimeout caps how long graceful shutdown will wait for
	// in-flight requests to drain. After expiry, the server closes
	// active connections. Default: 10s.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	// WriteRequestTimeout is the per-request ctx deadline applied to
	// POST /ingest. Bounds how long a single Write may stall on
	// backpressure before returning 429 to the client. Default: 5s.
	WriteRequestTimeout time.Duration `yaml:"write_request_timeout"`
}

// EngineConfig mirrors the subset of engine.Config that operators are
// expected to tune. Fields here are passed through verbatim to
// engine.Open. Engine defaults are applied inside the engine package;
// we only override when YAML sets a non-zero value.
type EngineConfig struct {
	// DataDir is the directory holding WAL segments and SSTable files.
	// Required — no default. Created if it does not exist.
	DataDir string `yaml:"data_dir"`

	// MemTableSizeLimit is the freeze trigger threshold in bytes. Zero
	// means "use engine default" (4 MiB).
	MemTableSizeLimit int64 `yaml:"memtable_size_limit"`

	// MemTableHardLimit is the Write-stall threshold in bytes. Zero
	// means "use engine default" (2 x MemTableSizeLimit).
	MemTableHardLimit int64 `yaml:"memtable_hard_limit"`

	// MaxFrozenMemTables caps the frozen MemTable queue length. Zero
	// means "use engine default" (4).
	MaxFrozenMemTables int `yaml:"max_frozen_memtables"`
}

// ErrMissingDataDir is returned when the engine.data_dir field is
// empty after defaulting. DataDir has no default — it must be set
// explicitly because the wrong default would silently write logs to
// the wrong place.
var ErrMissingDataDir = errors.New("config: engine.data_dir is required")

// Load reads YAML from path, applies defaults, and validates.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // typo-catch: unknown fields fail loudly
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyDefaults fills in zero-valued fields with their defaults. Engine
// fields are intentionally left as zero values when the YAML omits them
// so engine.Open's applyDefaults handles them in one place.
func (c *Config) applyDefaults() {
	if c.Server.ListenAddr == "" {
		c.Server.ListenAddr = ":8080"
	}
	if c.Server.ShutdownTimeout == 0 {
		c.Server.ShutdownTimeout = 10 * time.Second
	}
	if c.Server.WriteRequestTimeout == 0 {
		c.Server.WriteRequestTimeout = 5 * time.Second
	}
}

func (c *Config) validate() error {
	if c.Engine.DataDir == "" {
		return ErrMissingDataDir
	}
	return nil
}
