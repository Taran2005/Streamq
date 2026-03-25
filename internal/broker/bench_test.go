package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// setupBenchHandler creates a broker and returns the handler directly for
// httptest.NewRequest based benchmarks. This avoids TCP entirely so we
// never hit Windows port exhaustion.
func setupBenchHandler(b *testing.B) http.Handler {
	b.Helper()
	cfg := DefaultConfig()
	br, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	srv := NewServer(br)

	// Pre-create topic
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]any{"name": "bench", "partitions": 16})
	req := httptest.NewRequest("POST", "/topics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	srv.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		b.Fatalf("setup: create topic got %d: %s", w.Code, w.Body.String())
	}

	return srv.httpServer.Handler
}

func doProduce(handler http.Handler, url string, payload []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, req)
	return w
}

func doConsume(handler http.Handler, url string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", url, nil)
	handler.ServeHTTP(w, req)
	return w
}

// --- Produce benchmarks ---

func BenchmarkHTTPProduce_100B(b *testing.B) {
	benchHTTPProduce(b, 100)
}

func BenchmarkHTTPProduce_1KB(b *testing.B) {
	benchHTTPProduce(b, 1024)
}

func BenchmarkHTTPProduce_10KB(b *testing.B) {
	benchHTTPProduce(b, 10*1024)
}

func benchHTTPProduce(b *testing.B, valueSize int) {
	handler := setupBenchHandler(b)
	value := strings.Repeat("x", valueSize)
	payload, _ := json.Marshal(map[string]string{"key": "k", "value": value})

	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := doProduce(handler, "/topics/bench/messages", payload)
		if w.Code != http.StatusOK {
			b.Fatalf("produce: got %d", w.Code)
		}
	}
}

// --- Consume benchmarks ---

func BenchmarkHTTPConsume_100msgs(b *testing.B) {
	handler := setupBenchHandler(b)

	// Pre-populate
	value := strings.Repeat("x", 100)
	payload, _ := json.Marshal(map[string]string{"key": "k", "value": value})
	for i := 0; i < 10_000; i++ {
		doProduce(handler, "/topics/bench/messages", payload)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := (i * 100) % 9900
		url := fmt.Sprintf("/topics/bench/messages?partition=0&offset=%d&max=100", offset)
		w := doConsume(handler, url)
		if w.Code != http.StatusOK {
			b.Fatalf("consume: got %d", w.Code)
		}
	}
}

// --- Concurrent produce ---

func BenchmarkHTTPProduce_Concurrent(b *testing.B) {
	handler := setupBenchHandler(b)
	value := strings.Repeat("x", 100)
	payload, _ := json.Marshal(map[string]string{"key": "k", "value": value})

	b.SetBytes(100)
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := doProduce(handler, "/topics/bench/messages", payload)
			if w.Code != http.StatusOK {
				b.Fatalf("produce: got %d", w.Code)
			}
		}
	})
}

// --- Key-routed produce (all same key = same partition, tests lock contention) ---

func BenchmarkHTTPProduce_SamePartition(b *testing.B) {
	handler := setupBenchHandler(b)
	payload, _ := json.Marshal(map[string]string{"key": "hot-key", "value": strings.Repeat("x", 100)})

	b.SetBytes(100)
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := doProduce(handler, "/topics/bench/messages", payload)
			if w.Code != http.StatusOK {
				b.Fatalf("produce: got %d", w.Code)
			}
		}
	})
}

// --- Mixed produce+consume workload ---

func BenchmarkHTTPMixed_ProduceConsume(b *testing.B) {
	handler := setupBenchHandler(b)
	value := strings.Repeat("x", 100)
	payload, _ := json.Marshal(map[string]string{"value": value})

	// Pre-populate some data for consumers
	for i := 0; i < 1000; i++ {
		doProduce(handler, "/topics/bench/messages", payload)
	}

	var ops atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := ops.Add(1)
			if n%3 == 0 {
				// 1/3 reads
				offset := int(n) % 900
				url := fmt.Sprintf("/topics/bench/messages?partition=0&offset=%d&max=10", offset)
				doConsume(handler, url)
			} else {
				// 2/3 writes
				doProduce(handler, "/topics/bench/messages", payload)
			}
		}
	})
}

// --- E2E with real TCP (run individually if needed: go test -bench=BenchmarkE2E -run=^$) ---

func BenchmarkE2E_ProduceConsume(b *testing.B) {
	cfg := DefaultConfig()
	br, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	srv := NewServer(br)
	ts := httptest.NewServer(srv.httpServer.Handler)
	b.Cleanup(ts.Close)

	client := ts.Client()

	// Create topic
	body, _ := json.Marshal(map[string]any{"name": "bench", "partitions": 4})
	resp, err := client.Post(ts.URL+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		b.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	value := strings.Repeat("x", 100)
	payload, _ := json.Marshal(map[string]string{"key": "k", "value": value})

	b.SetBytes(100)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Produce
		resp, err := client.Post(ts.URL+"/topics/bench/messages", "application/json", bytes.NewReader(payload))
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
