package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/streamq/streamq/internal/broker"
)

func main() {
	cfg := broker.DefaultConfig()

	// Basic flags
	flag.IntVar(&cfg.Port, "port", cfg.Port, "broker listen port")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory for persistent storage (empty = in-memory)")

	// Cluster flags
	flag.StringVar(&cfg.BrokerID, "id", "", "broker ID (required for cluster mode)")
	flag.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", "address other brokers use to reach us (e.g. http://broker1:8081)")
	var etcdFlag string
	flag.StringVar(&etcdFlag, "etcd", "", "comma-separated etcd endpoints (e.g. localhost:2379)")
	flag.Parse()

	// Parse etcd endpoints
	if etcdFlag != "" {
		cfg.EtcdEndpoints = strings.Split(etcdFlag, ",")
	}

	// In cluster mode, broker ID and advertise addr are required
	if cfg.IsClusterMode() {
		if cfg.BrokerID == "" {
			log.Fatal("-id is required in cluster mode")
		}
		if cfg.AdvertiseAddr == "" {
			cfg.AdvertiseAddr = fmt.Sprintf("http://localhost:%d", cfg.Port)
			log.Printf("WARNING: -advertise-addr not set, defaulting to %s (won't work in Docker)", cfg.AdvertiseAddr)
		}
		log.Printf("cluster mode: broker %s, advertise %s, etcd %v", cfg.BrokerID, cfg.AdvertiseAddr, cfg.EtcdEndpoints)
	} else {
		if cfg.BrokerID == "" {
			cfg.BrokerID = "standalone"
		}
		log.Println("standalone mode (no etcd)")
	}

	if cfg.DataDir != "" {
		log.Printf("persistent storage enabled: %s", cfg.DataDir)
	} else {
		log.Println("running in-memory mode (data lost on restart)")
	}

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("failed to start broker: %v", err)
	}

	// Join cluster if configured
	if err := b.JoinCluster(); err != nil {
		log.Fatalf("failed to join cluster: %v", err)
	}

	srv := broker.NewServer(b)

	done := make(chan error, 1)
	go func() {
		done <- srv.Start()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-done:
		log.Printf("server exited unexpectedly: %v", err)
	case <-quit:
		log.Println("graceful shutdown starting...")
	}

	// Two contexts: a longer outer one for the leadership-transfer dance
	// (Broker.Shutdown), then a shorter one for HTTP drain. The total budget
	// is bounded by cfg.ShutdownTimeout (default 10s).
	shutTimeout := cfg.ShutdownTimeout
	if shutTimeout <= 0 {
		shutTimeout = 10 * time.Second
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), shutTimeout)
	defer cancel()

	// Phase-4 graceful shutdown: resign leadership, drain replication, then
	// fall through to existing HTTP shutdown + broker close. Safe to call in
	// standalone mode — it's a no-op there.
	if err := b.Shutdown(shutCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	if err := b.Close(); err != nil {
		log.Printf("broker close error: %v", err)
	}

	log.Println("broker stopped")
}
