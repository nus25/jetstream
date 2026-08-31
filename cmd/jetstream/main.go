package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/nus25/jetstream-proxy/pkg/consumer"
	proxy "github.com/nus25/jetstream-proxy/pkg/proxy"
	"github.com/nus25/jetstream-proxy/pkg/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:    "jetstream-proxy",
		Usage:   "jetstream proxy service",
		Version: "1.0.7",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "ingress-host",
			Usage:   "full websocket path to the jetstream endpoint",
			Value:   "localhost:6008",
			EnvVars: []string{"JETSTREAM_HOST"},
		},
		&cli.StringSliceFlag{
			Name:    "wanted-collections",
			Usage:   "comma-separated list of collections",
			EnvVars: []string{"WANTED_COLLECTIONS"},
		},
		&cli.StringSliceFlag{
			Name:    "wanted-dids",
			Usage:   "comma-separated list of DIDs",
			EnvVars: []string{"WANTED_DIDS"},
		},
		&cli.UintFlag{
			Name:    "max-msg-size-bytes",
			Usage:   "max message size Bytes of incoming jetstream. default is unlimited.",
			Value:   0,
			EnvVars: []string{"MAX_MSG_SIZE_BYTES"},
		},
		&cli.IntFlag{
			Name:    "worker-count",
			Usage:   "number of workers to process events",
			Value:   100,
			EnvVars: []string{"JETSTREAM_WORKER_COUNT"},
		},
		&cli.IntFlag{
			Name:    "max-queue-size",
			Usage:   "max number of events to queue",
			Value:   1000,
			EnvVars: []string{"JETSTREAM_MAX_QUEUE_SIZE"},
		},
		&cli.StringFlag{
			Name:    "listen-addr",
			Usage:   "addr to serve echo on",
			Value:   ":6008",
			EnvVars: []string{"JETSTREAM_LISTEN_ADDR"},
		},
		&cli.StringFlag{
			Name:    "metrics-listen-addr",
			Usage:   "addr to serve prometheus metrics on",
			Value:   ":6009",
			EnvVars: []string{"JETSTREAM_METRICS_LISTEN_ADDR"},
		},
		&cli.StringFlag{
			Name:    "data-dir",
			Usage:   "directory to store data (pebbleDB)",
			Value:   "./data",
			EnvVars: []string{"JETSTREAM_DATA_DIR"},
		},
		&cli.DurationFlag{
			Name:    "event-ttl",
			Usage:   "time to live for events",
			Value:   24 * time.Hour,
			EnvVars: []string{"JETSTREAM_EVENT_TTL"},
		},
		&cli.Float64Flag{
			Name:    "max-sub-rate",
			Usage:   "max rate of events per second we can send to a subscriber",
			Value:   5_000,
			EnvVars: []string{"JETSTREAM_MAX_SUB_RATE"},
		},
		&cli.Uint64Flag{
			Name:    "override-relay-seq",
			Usage:   "override sequence to start from, if not set will start from the last sequence in the database, if no sequence in the database will start from live, if set 0 force from live",
			Value:   0,
			EnvVars: []string{"JETSTREAM_OVERRIDE_RELAY_SEQ"},
		},
		&cli.DurationFlag{
			Name:    "liveness-ttl",
			Usage:   "time to restart when no event detected",
			Value:   15 * time.Second,
			EnvVars: []string{"JETSTREAM_LIVENESS_TTL"},
		},
		&cli.BoolFlag{
			Name:    "zstd-compression",
			Usage:   "enable zstd compression for incoming events",
			Value:   true,
			EnvVars: []string{"JETSTREAM_ZSTD_COMPRESSION"},
		},
		&cli.DurationFlag{
			Name:    "max-event-age",
			Usage:   "maximum age of an event createdAt timestamp, after which it is considered expired",
			Value:   24 * time.Hour,
			EnvVars: []string{"JETSTREAM_MAX_EVENT_AGE"},
		},
	}

	app.Action = Jetstream

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// Jetstream is the main function for jetstream
func Jetstream(cctx *cli.Context) error {
	ctx := cctx.Context

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	log.Info("starting jetstream")

	u := &url.URL{
		Host: cctx.String("ingress-host"),
	}
	switch u.Host {
	case "":
		return fmt.Errorf("failed to parse ingress-host: %s", cctx.String("ingress-host"))
	default:
		if strings.Contains(u.Host, "://") {
			return fmt.Errorf("ingress-host should not contain scheme: %s", u.Host)
		}
	}

	s, err := server.NewServer(cctx.Float64("max-sub-rate"))
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	c, err := consumer.NewConsumer(
		ctx,
		log,
		u.Host,
		cctx.String("data-dir"),
		cctx.Duration("event-ttl"),
		s.Emit,
	)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	s.Consumer = c

	// Start a goroutine to manage the cursor, saving the current cursor every 5 seconds.
	shutdownCursorManager := make(chan struct{})
	cursorManagerShutdown := make(chan struct{})
	go func() {
		ctx := context.Background()
		ticker := time.NewTicker(5 * time.Second)
		log := log.With("source", "cursor_manager")

		for {
			select {
			case <-shutdownCursorManager:
				log.Info("shutting down cursor manager")
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Error("failed to write cursor", "error", err)
				}
				log.Info("cursor manager shut down successfully")
				close(cursorManagerShutdown)
				return
			case <-ticker.C:
				err := c.WriteCursor(ctx)
				if err != nil {
					log.Error("failed to write cursor", "error", err)
				}
			}
		}
	}()

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	livenessKill := make(chan struct{})

	// Start a goroutine to manage the liveness checker, shutting down if no events are received for liveness-ttl
	shutdownLivenessChecker := make(chan struct{})
	livenessCheckerShutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cctx.Duration("liveness-ttl"))
		lastSeq := consumer.UnsetSeq
		log := log.With("source", "liveness_checker")

		for {
			select {
			case <-shutdownLivenessChecker:
				log.Info("shutting down liveness checker")
				close(livenessCheckerShutdown)
				return
			case <-ticker.C:
				seq, _ := c.Progress.Get()
				if seq == consumer.UnsetSeq {
					log.Error("no events received yet.")
					continue
				}
				if seq == lastSeq {
					log.Error("no new events in last "+cctx.Duration("liveness-ttl").String()+", shutting down for docker to restart me", "seq", seq)
					close(livenessKill)
				} else {
					// Trim the database
					err := c.TrimEvents(ctx)
					if err != nil {
						log.Error("failed to trim events", "error", err)
					}
					log.Info("successful liveness check and trim", "seq", seq)
					lastSeq = seq
				}
			}
		}
	}()

	m := echo.New()
	m.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	m.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	metricsServer := &http.Server{
		Addr:    cctx.String("metrics-listen-addr"),
		Handler: m,
	}

	e := echo.New()
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodHead, http.MethodOptions},
	}))
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Welcome to Jetstream proxy")
	})
	e.GET("/subscribe", s.HandleSubscribe)

	jetServer := &http.Server{
		Addr:    cctx.String("listen-addr"),
		Handler: e,
	}

	// Startup echo server
	shutdownEcho := make(chan struct{})
	echoShutdown := make(chan struct{})
	go func() {
		logger := log.With("source", "echo_server")

		logger.Info("echo server listening", "addr", cctx.String("listen-addr"))

		go func() {
			if err := jetServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start echo server", "error", err)
			}
		}()

		<-shutdownEcho
		if err := jetServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown echo server", "error", err)
		}
		logger.Info("echo server shut down")
		close(echoShutdown)
	}()

	// Startup metrics server
	shutdownMetrics := make(chan struct{})
	metricsShutdown := make(chan struct{})
	go func() {
		logger := log.With("source", "metrics_server")

		logger.Info("metrics server listening", "addr", cctx.String("metrics-listen-addr"))

		go func() {
			if err := metricsServer.ListenAndServe(); err != http.ErrServerClosed {
				logger.Error("failed to start metrics server", "error", err)
			}
		}()

		<-shutdownMetrics
		if err := metricsServer.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown metrics server", "error", err)
		}
		logger.Info("metrics server shut down")
		close(metricsShutdown)
	}()

	var seq uint64 = consumer.UnsetSeq

	// If the last cursor in the database is set, use that as the cursor
	if c.Progress.LastSeq > 0 {
		seq = c.Progress.LastSeq
	}

	// If the override cursor is set, use that instead of the last cursor in the database
	if cctx.IsSet("override-relay-seq") {
		cursorOverride := cctx.Uint64("override-relay-seq")
		if cursorOverride == 0 {
			log.Warn("override-relay-seq is set to 0, start from livetail")
			seq = consumer.UnsetSeq
		} else {
			log.Info("overriding cursor", "cursor", cursorOverride)
			seq = cursorOverride
		}
	}

	config := proxy.DefaultClientConfig()
	config.Host = u.Host
	config.WantedCollections = cctx.StringSlice("wanted-collections")
	config.WantedDids = cctx.StringSlice("wanted-dids")
	config.MaxMsgSizeBytes = uint32(cctx.Uint("max-msg-size-bytes"))

	// Create a channel that will be closed when we want to stop the application
	// Usually when a critical routine returns an error
	eventsKill := make(chan struct{})

	shutdownRepoStream := make(chan struct{})
	repoStreamShutdown := make(chan struct{})
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			//jetstream proxy
			logger := log.With("source", "repo_stream")
			err = proxy.HandleRepoStream(ctx, config, seq, cctx.Duration("max-event-age"), c, logger)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Info("handleRepoStream returned unexpectedly, killing jetstream proxy", "error", err)
					close(eventsKill)
				} else {
					logger.Info("handleRepoStream closed on context cancel")
				}
			} else {
				logger.Info("handleRepoStream closed normally")
			}
			close(repoStreamShutdown)
		}()
		<-shutdownRepoStream
		cancel()
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		log.Info("shutting down on signal")
	case <-ctx.Done():
		log.Info("shutting down on context done")
	case <-livenessKill:
		log.Info("shutting down on liveness kill")
	case <-eventsKill:
		log.Info("shutting down on events kill")
	}

	log.Info("shutting down, waiting for workers to clean up...")

	close(shutdownRepoStream)
	close(shutdownLivenessChecker)
	close(shutdownCursorManager)
	close(shutdownEcho)
	close(shutdownMetrics)

	shutdownTimeout := time.After(10 * time.Second)
	minShutdownWait := time.After(3 * time.Second)

	select {
	case <-repoStreamShutdown:
		log.Info("Repo stream shutdown completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for repo stream")
	}

	select {
	case <-livenessCheckerShutdown:
		log.Info("Liveness checker shutdown completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for liveness checker")
	}

	select {
	case <-cursorManagerShutdown:
		log.Info("Cursor manager shutdown completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for cursor manager")
	}

	select {
	case <-echoShutdown:
		log.Info("Echo shutdown completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for echo server")
	}

	select {
	case <-metricsShutdown:
		log.Info("Metrics shutdown completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for metrics server")
	}

	c.Shutdown()

	err = c.UncompressedDB.Close()
	if err != nil {
		log.Error("failed to close pebble db", "error", err)
	}

	err = c.CompressedDB.Close()
	if err != nil {
		log.Error("failed to close compressed pebble db", "error", err)
	}

	select {
	case <-minShutdownWait:
		log.Info("min shutdown wait completed")
	case <-shutdownTimeout:
		log.Warn("Shutdown timeout reached for min shutdown wait")
	}
	log.Info("shut down successfully")

	return nil
}
