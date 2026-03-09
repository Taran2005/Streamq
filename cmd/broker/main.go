package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streamq/streamq/internal/broker"
)

func main() {
	cfg := broker.DefaultConfig()

	flag.IntVar(&cfg.Port, "port", cfg.Port, "broker listen port")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory for persistent storage (empty = in-memory)")
	flag.Parse()

	if cfg.DataDir != "" {
		log.Printf("persistent storage enabled: %s", cfg.DataDir)
	} else {
		log.Println("running in-memory mode (data lost on restart)")
	}

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("failed to start broker: %v", err)
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
		log.Println("shutting down broker...")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	if err := b.Close(); err != nil {
		log.Printf("broker close error: %v", err)
	}

	log.Println("broker stopped")
}
