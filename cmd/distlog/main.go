// Command distlog runs a single-node DistLog process: loads YAML
// config, opens the storage engine, starts the HTTP server, and waits
// for SIGINT/SIGTERM to shut down cleanly.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yuexishen/distlog/internal/config"
	"github.com/yuexishen/distlog/internal/engine"
	"github.com/yuexishen/distlog/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "distlog: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to YAML config file (required)")
	flag.Parse()
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	eng, err := engine.Open(engine.Config{
		DataDir:            cfg.Engine.DataDir,
		MemTableSizeLimit:  cfg.Engine.MemTableSizeLimit,
		MemTableHardLimit:  cfg.Engine.MemTableHardLimit,
		MaxFrozenMemTables: cfg.Engine.MaxFrozenMemTables,
		OnFatal: func(err error) {
			// Fatal-state callback: log and let the next request
			// surface ErrEngineFatal -> 500. The OS process stays
			// up so operators can drain it deliberately.
			fmt.Fprintf(os.Stderr, "distlog: engine fatal: %v\n", err)
		},
	})
	if err != nil {
		return fmt.Errorf("open engine: %w", err)
	}

	srv := server.New(server.Config{
		ListenAddr:          cfg.Server.ListenAddr,
		ShutdownTimeout:     cfg.Server.ShutdownTimeout,
		WriteRequestTimeout: cfg.Server.WriteRequestTimeout,
	}, eng)

	// Bind ctx to SIGINT/SIGTERM so the server shuts down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "distlog: listening on %s, data dir %s\n",
		cfg.Server.ListenAddr, cfg.Engine.DataDir)
	serveErr := srv.Serve(ctx)

	// Close the engine after the server has stopped accepting requests
	// so no in-flight write hits a half-closed engine.
	if cerr := eng.Close(); cerr != nil && serveErr == nil {
		serveErr = fmt.Errorf("close engine: %w", cerr)
	}
	return serveErr
}
