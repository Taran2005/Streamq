package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streamq/streamq/internal/broker"
)

func main() {
	url := flag.String("url", "", "existing broker URL (if empty, starts an in-process broker)")
	duration := flag.Duration("duration", 10*time.Second, "duration per benchmark")
	msgSize := flag.Int("size", 100, "message value size in bytes")
	flag.Parse()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              StreamQ End-to-End Benchmark                    ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	var brokerURL string
	if *url != "" {
		brokerURL = *url
		fmt.Printf("  Target:   %s (external broker)\n", brokerURL)
	} else {
		brokerURL = startInProcessBroker()
		fmt.Printf("  Target:   %s (in-process broker, real TCP)\n", brokerURL)
	}
	fmt.Printf("  Duration: %s per test\n", *duration)
	fmt.Printf("  Message:  %d byte value\n", *msgSize)
	fmt.Println()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	// ──────────────────────────────────────────────────────
	// Test 1: Single producer throughput (1 producer → 1 partition)
	// ──────────────────────────────────────────────────────
	fmt.Println("  ── Test 1: Single Producer Throughput (1 producer → 1 partition) ──")
	fmt.Println()

	topicName := "bench-single"
	createTopic(client, brokerURL, topicName, 1)

	value := strings.Repeat("x", *msgSize)
	payload, _ := json.Marshal(map[string]string{"key": "k", "value": value})
	msgURL := brokerURL + "/topics/" + topicName + "/messages"

	var singleProducerOps atomic.Int64
	latencies := collectLatencies(client, msgURL, payload, *duration, &singleProducerOps)

	opsPerSec := float64(singleProducerOps.Load()) / duration.Seconds()
	mbPerSec := opsPerSec * float64(*msgSize) / (1024 * 1024)
	fmt.Printf("  Throughput:  %s msg/sec  (%.1f MB/s)\n", fmtNum(opsPerSec), mbPerSec)
	printPercentiles("  Latency:     ", latencies)
	fmt.Println()

	// ──────────────────────────────────────────────────────
	// Test 2: End-to-end latency (produce then immediately consume)
	// ──────────────────────────────────────────────────────
	fmt.Println("  ── Test 2: End-to-End Latency (produce → consume round-trip) ──")
	fmt.Println()

	topicName2 := "bench-e2e"
	createTopic(client, brokerURL, topicName2, 1)
	msgURL2 := brokerURL + "/topics/" + topicName2 + "/messages"

	e2eLatencies := measureE2ELatency(client, msgURL2, payload, *duration)
	printPercentiles("  E2E Latency: ", e2eLatencies)
	fmt.Printf("  Ops:         %d round-trips in %s\n", len(e2eLatencies), *duration)
	fmt.Println()

	// ──────────────────────────────────────────────────────
	// Test 3: Multi-producer throughput (N producers → P partitions)
	// ──────────────────────────────────────────────────────
	for _, cfg := range []struct {
		producers  int
		partitions int
	}{
		{4, 4},
		{8, 8},
		{16, 16},
	} {
		fmt.Printf("  ── Test 3: %d producers → %d partitions ──\n", cfg.producers, cfg.partitions)
		fmt.Println()

		tn := fmt.Sprintf("bench-multi-%d-%d", cfg.producers, cfg.partitions)
		createTopic(client, brokerURL, tn, cfg.partitions)
		mu := brokerURL + "/topics/" + tn + "/messages"

		var totalOps atomic.Int64
		var totalErrors atomic.Int64

		var wg sync.WaitGroup
		deadline := time.Now().Add(*duration)

		for w := 0; w < cfg.producers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					resp, err := client.Post(mu, "application/json", bytes.NewReader(payload))
					if err != nil {
						totalErrors.Add(1)
						continue
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					totalOps.Add(1)
				}
			}()
		}
		wg.Wait()

		ops := float64(totalOps.Load()) / duration.Seconds()
		mb := ops * float64(*msgSize) / (1024 * 1024)
		fmt.Printf("  Throughput:  %s msg/sec  (%.1f MB/s)", fmtNum(ops), mb)
		if totalErrors.Load() > 0 {
			fmt.Printf("  [%d errors]", totalErrors.Load())
		}
		fmt.Println()
		fmt.Println()
	}

	// ──────────────────────────────────────────────────────
	// Test 4: Consumer throughput (batch read from pre-populated topic)
	// ──────────────────────────────────────────────────────
	fmt.Println("  ── Test 4: Consumer Throughput (batch read, 100 msgs/fetch) ──")
	fmt.Println()

	topicName4 := "bench-consume"
	createTopic(client, brokerURL, topicName4, 1)
	msgURL4 := brokerURL + "/topics/" + topicName4 + "/messages"

	// Pre-populate 50K messages
	fmt.Print("  Populating 50,000 messages...")
	for i := 0; i < 50_000; i++ {
		resp, err := client.Post(msgURL4, "application/json", bytes.NewReader(payload))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	fmt.Println(" done")

	var consumeOps atomic.Int64
	var consumeMsgs atomic.Int64
	{
		deadline := time.Now().Add(*duration)
		var offset atomic.Int64
		for time.Now().Before(deadline) {
			o := offset.Load()
			if o >= 49_900 {
				offset.Store(0)
				o = 0
			}
			url := fmt.Sprintf("%s?partition=0&offset=%d&max=100", msgURL4, o)
			resp, err := client.Get(url)
			if err != nil {
				continue
			}
			var result struct {
				Messages []json.RawMessage `json:"messages"`
			}
			json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()
			consumeOps.Add(1)
			consumeMsgs.Add(int64(len(result.Messages)))
			offset.Add(100)
		}
	}

	fetchOps := float64(consumeOps.Load()) / duration.Seconds()
	msgOps := float64(consumeMsgs.Load()) / duration.Seconds()
	fmt.Printf("  Fetch ops:   %s fetches/sec\n", fmtNum(fetchOps))
	fmt.Printf("  Messages:    %s msg/sec delivered\n", fmtNum(msgOps))
	fmt.Println()

	// ──────────────────────────────────────────────────────
	// Summary
	// ──────────────────────────────────────────────────────
	fmt.Println("  ══════════════════════════════════════════════════════════════")
	fmt.Println("  SUMMARY")
	fmt.Println("  ──────────────────────────────────────────────────────────────")
	fmt.Printf("  Setup:               1 broker, 1 node, %d-byte messages\n", *msgSize)
	fmt.Printf("  Single producer:     %s msg/sec → 1 partition\n", fmtNum(float64(singleProducerOps.Load())/duration.Seconds()))
	fmt.Printf("  E2E latency:         p50=%s  p99=%s\n", fmtDuration(percentile(e2eLatencies, 50)), fmtDuration(percentile(e2eLatencies, 99)))
	fmt.Printf("  Consumer throughput: %s msg/sec (batch 100)\n", fmtNum(msgOps))
	fmt.Println("  ══════════════════════════════════════════════════════════════")
	fmt.Println()
}

// startInProcessBroker starts a real broker on a temp directory and returns its URL.
func startInProcessBroker() string {
	dir, err := os.MkdirTemp("", "streamq-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		os.Exit(1)
	}

	cfg := broker.DefaultConfig()
	cfg.DataDir = dir

	b, err := broker.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create broker: %v\n", err)
		os.Exit(1)
	}

	srv := broker.NewServer(b)

	// Listen on random port (":0" lets the OS pick)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	go func() {
		if err := http.Serve(ln, srv.Handler()); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		}
	}()

	brokerURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Wait for broker to be ready
	for i := 0; i < 50; i++ {
		resp, err := http.Get(brokerURL + "/topics")
		if err == nil {
			resp.Body.Close()
			return brokerURL
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "broker failed to start")
	os.Exit(1)
	return ""
}

func createTopic(client *http.Client, brokerURL, name string, partitions int) {
	body, _ := json.Marshal(map[string]any{"name": name, "partitions": partitions})
	resp, err := client.Post(brokerURL+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "create topic %s: %v\n", name, err)
		os.Exit(1)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// collectLatencies sends messages in small batches and records per-op latency.
// Batching avoids Windows timer granularity issues (~1ms resolution) by
// measuring groups of ops that take longer than one clock tick.
func collectLatencies(client *http.Client, url string, payload []byte, duration time.Duration, counter *atomic.Int64) []time.Duration {
	const batchSize = 20 // enough ops per batch to exceed timer tick
	latencies := make([]time.Duration, 0, 50_000)
	deadline := time.Now().Add(duration)

	for time.Now().Before(deadline) {
		start := time.Now()
		for j := 0; j < batchSize; j++ {
			resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
			if err != nil {
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			counter.Add(1)
		}
		elapsed := time.Since(start)
		perOp := elapsed / time.Duration(batchSize)
		for j := 0; j < batchSize; j++ {
			latencies = append(latencies, perOp)
		}
	}
	return latencies
}

// measureE2ELatency produces then immediately consumes messages in batches,
// measuring the full round-trip time per operation.
func measureE2ELatency(client *http.Client, url string, payload []byte, duration time.Duration) []time.Duration {
	const batchSize = 10
	latencies := make([]time.Duration, 0, 50_000)
	deadline := time.Now().Add(duration)
	offset := uint64(0)

	for time.Now().Before(deadline) {
		start := time.Now()
		for j := 0; j < batchSize; j++ {
			// Produce
			resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
			if err != nil {
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			// Consume the message we just produced
			consumeURL := fmt.Sprintf("%s?partition=0&offset=%d&max=1", url, offset)
			resp, err = client.Get(consumeURL)
			if err != nil {
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			offset++
		}
		elapsed := time.Since(start)
		perOp := elapsed / time.Duration(batchSize)
		for j := 0; j < batchSize; j++ {
			latencies = append(latencies, perOp)
		}
	}
	return latencies
}

func printPercentiles(prefix string, latencies []time.Duration) {
	if len(latencies) == 0 {
		fmt.Printf("%s(no data)\n", prefix)
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	avg := time.Duration(0)
	for _, l := range latencies {
		avg += l
	}
	avg /= time.Duration(len(latencies))

	fmt.Printf("%savg=%s  p50=%s  p95=%s  p99=%s  (n=%d)\n",
		prefix,
		fmtDuration(avg),
		fmtDuration(percentile(latencies, 50)),
		fmtDuration(percentile(latencies, 95)),
		fmtDuration(percentile(latencies, 99)),
		len(latencies),
	)
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// fmtDuration formats a duration for display, avoiding "0s" for sub-microsecond values.
func fmtDuration(d time.Duration) string {
	if d == 0 {
		return "<1µs"
	}
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	return d.Round(time.Millisecond).String()
}

func fmtNum(n float64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", n/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", n/1_000)
	}
	return fmt.Sprintf("%.0f", math.Round(n))
}
