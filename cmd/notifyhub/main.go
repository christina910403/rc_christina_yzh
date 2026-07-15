package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/christina910403/rc_christina_yzh/internal/config"
	"github.com/christina910403/rc_christina_yzh/internal/httpapi"
	"github.com/christina910403/rc_christina_yzh/internal/idgen"
	"github.com/christina910403/rc_christina_yzh/internal/service"
	"github.com/christina910403/rc_christina_yzh/internal/storage"
	workerpkg "github.com/christina910403/rc_christina_yzh/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return usage()
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	configPath := flags.String("config", "configs/config.yaml", "path to YAML configuration")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	runtime, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if command == "config-validate" {
		fmt.Printf("configuration valid: version=%s events=%d routes=%d operations=%d\n", runtime.Version, len(runtime.Events), len(runtime.File.EventRoutes), len(runtime.Operations))
		return nil
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := storage.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if command == "migrate" {
		return store.Migrate(ctx)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	switch command {
	case "api":
		ingestor := service.NewIngestor(runtime, store)
		api := httpapi.New(runtime, ingestor, store, logger)
		server := &http.Server{
			Addr: runtime.File.Server.Address, Handler: api.Handler(),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
			WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		logger.Info("api started", "address", server.Addr, "config_version", runtime.Version)
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case "worker":
		owner, err := idgen.New()
		if err != nil {
			return err
		}
		worker := workerpkg.New(store, workerpkg.Config{
			OwnerID: owner, BatchSize: runtime.File.Worker.BatchSize,
			PollInterval:        time.Duration(runtime.File.Worker.PollIntervalMS) * time.Millisecond,
			LeaseDuration:       time.Duration(runtime.File.Worker.LeaseSeconds) * time.Second,
			AllowPrivateNetwork: runtime.File.Security.AllowPrivateNetwork,
		}, logger)
		logger.Info("worker started", "owner", owner, "config_version", runtime.Version)
		if err := worker.Run(ctx); errors.Is(err, context.Canceled) {
			return nil
		} else {
			return err
		}
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage: notifyhub <config-validate|migrate|api|worker> [-config path]")
}
