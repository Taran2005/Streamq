package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streamq/streamq/internal/broker"
)

func main() {
	cfg := broker.DefaultConfig()
	// Create the broker (holds all topics in memory)
	b := broker.New(cfg)

	// Create the HTTP server 
	srv := broker.NewServer(b)

	done := make(chan error, 1)
	go func() {
		done <- srv.Start()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down broker...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("forced shutdown: %v", err)
	}

	<-done
	log.Println("Broker stopped cleanly.")
}
