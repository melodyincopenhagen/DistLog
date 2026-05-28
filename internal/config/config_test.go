package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestLoad_DefaultsAppliedWhenFieldsOmitted(t *testing.T) {
	path := writeYAML(t, `
engine:
  data_dir: /tmp/distlog
`)
	c, err := Load(path)
	require.NoError(t, err)

	require.Equal(t, ":8080", c.Server.ListenAddr)
	require.Equal(t, 10*time.Second, c.Server.ShutdownTimeout)
	require.Equal(t, 5*time.Second, c.Server.WriteRequestTimeout)
	require.Equal(t, "/tmp/distlog", c.Engine.DataDir)
	// Engine subsystem fields stay zero — engine.Open is the one place
	// that defaults them.
	require.Zero(t, c.Engine.MemTableSizeLimit)
	require.Zero(t, c.Engine.MemTableHardLimit)
	require.Zero(t, c.Engine.MaxFrozenMemTables)
}

func TestLoad_YAMLOverridesDefaults(t *testing.T) {
	path := writeYAML(t, `
server:
  listen_addr: "127.0.0.1:9000"
  shutdown_timeout: 30s
  write_request_timeout: 2s
engine:
  data_dir: /var/lib/distlog
  memtable_size_limit: 8388608
  memtable_hard_limit: 16777216
  max_frozen_memtables: 8
`)
	c, err := Load(path)
	require.NoError(t, err)

	require.Equal(t, "127.0.0.1:9000", c.Server.ListenAddr)
	require.Equal(t, 30*time.Second, c.Server.ShutdownTimeout)
	require.Equal(t, 2*time.Second, c.Server.WriteRequestTimeout)
	require.Equal(t, "/var/lib/distlog", c.Engine.DataDir)
	require.Equal(t, int64(8*1024*1024), c.Engine.MemTableSizeLimit)
	require.Equal(t, int64(16*1024*1024), c.Engine.MemTableHardLimit)
	require.Equal(t, 8, c.Engine.MaxFrozenMemTables)
}

func TestLoad_MissingDataDirIsError(t *testing.T) {
	path := writeYAML(t, `
server:
  listen_addr: ":9000"
`)
	_, err := Load(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrMissingDataDir))
}

func TestLoad_UnknownFieldIsError(t *testing.T) {
	// Typo-catch: an unknown YAML field must fail loudly rather than
	// silently default. KnownFields(true) gives us this for free.
	path := writeYAML(t, `
engine:
  data_dir: /tmp/x
  memtabel_size_limit: 1234  # typo: memtabel vs memtable
`)
	_, err := Load(path)
	require.Error(t, err)
}

func TestLoad_AuthTokensParsed(t *testing.T) {
	path := writeYAML(t, `
engine:
  data_dir: /tmp/x
auth:
  tokens:
    "tok-abc123": "tenant-a"
    "tok-def456": "tenant-b"
`)
	c, err := Load(path)
	require.NoError(t, err)
	require.Len(t, c.Auth.Tokens, 2)
	require.Equal(t, "tenant-a", c.Auth.Tokens["tok-abc123"])
	require.Equal(t, "tenant-b", c.Auth.Tokens["tok-def456"])
}

func TestLoad_AuthOmittedIsEmptyMap(t *testing.T) {
	path := writeYAML(t, `engine: { data_dir: /tmp/x }`)
	c, err := Load(path)
	require.NoError(t, err)
	require.Empty(t, c.Auth.Tokens, "no tokens configured -> auth disabled mode")
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	require.Error(t, err)
}
