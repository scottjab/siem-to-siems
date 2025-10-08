package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scottjab/siem-to-siems/internal/config"
	"github.com/scottjab/siem-to-siems/internal/destinations"
	"tailscale.com/tsnet"
)

func main() {
	// Load config (env SIEM_TO_SIEMS_CONFIG or ./config.json)
	cfg, err := config.Load("")
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Prepare destination fanout
	var sinks []destinations.Destination

	if cfg.Destinations.NDJSON != nil && cfg.Destinations.NDJSON.Directory != "" {
		ndw, err := destinations.NewNDJSONWriter(
			cfg.Destinations.NDJSON.Directory,
			config.ParseDurationOr(cfg.Destinations.NDJSON.Rotate, time.Hour),
		)
		if err != nil {
			log.Fatalf("ndjson writer init failed: %v", err)
		}
		sinks = append(sinks, ndw)
		defer ndw.Close()
	}

	for _, h := range cfg.Destinations.HTTP {
		opts := destinations.HTTPForwarderOptions{
			JournalDirectory: h.JournalDirectory,
			InitialBackoff:   config.ParseDurationOr(h.InitialBackoff, 1*time.Second),
			MaxBackoff:       config.ParseDurationOr(h.MaxBackoff, 1*time.Minute),
			Token:            h.Token,
		}
		hf, err := destinations.NewHTTPForwarder(h.URL, opts)
		if err != nil {
			log.Fatalf("http forwarder init failed for %s: %v", h.URL, err)
		}
		sinks = append(sinks, hf)
		defer hf.Close()
	}

	if len(sinks) == 0 {
		log.Fatal("no destinations configured; set --ndjson_dir and/or --http_targets")
	}

	fanout := destinations.NewFanout(sinks...)
	defer fanout.Close()

	// Create tsnet server
	srv := &tsnet.Server{
		Hostname: cfg.TSNet.Hostname,
	}
	if cfg.TSNet.AuthKey != "" {
		srv.AuthKey = cfg.TSNet.AuthKey
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("can't start tsnet server: %v", err)
	}
	defer srv.Close()

	// Choose TLS or plain listener based on config (TLS default on)
	useTLS := cfg.Server.TLSEnabled == nil || *cfg.Server.TLSEnabled
	var ln net.Listener
	if useTLS {
		ln, err = srv.ListenTLS("tcp", cfg.Server.Addr)
	} else {
		ln, err = srv.Listen("tcp", cfg.Server.Addr)
	}
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	defer ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/streaming", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()

		// Forward event to all sinks; we always 200 regardless of downstream errors.
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		_ = fanout.Send(ctx, body)
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM
	shutdownCh := make(chan os.Signal, 2)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-shutdownCh
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	log.Printf("listening on tsnet %s as %s", cfg.Server.Addr, cfg.TSNet.Hostname)
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}
