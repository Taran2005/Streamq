package testutil

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestStartBroker_ProduceConsume is the smoke test: spin up an in-process broker,
// create a topic, produce a message, consume it back. If this works the helper is
// wired up correctly.
func TestStartBroker_ProduceConsume(t *testing.T) {
	_, url, cleanup := StartBroker(t)
	defer cleanup()

	// Create a topic.
	body, _ := json.Marshal(map[string]any{"name": "smoke", "partitions": 1})
	resp, err := http.Post(url+"/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: expected 201, got %d", resp.StatusCode)
	}

	// Produce a message.
	body, _ = json.Marshal(map[string]any{"key": "k", "value": "hello"})
	resp, err = http.Post(url+"/topics/smoke/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce: expected 200, got %d", resp.StatusCode)
	}

	// Consume it back.
	resp, err = http.Get(url + "/topics/smoke/messages?partition=0&offset=0")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("consume: expected 200, got %d (%s)", resp.StatusCode, string(raw))
	}
	if !strings.Contains(string(raw), "hello") {
		t.Fatalf("expected response to contain 'hello', got %s", string(raw))
	}
}

// TestStartBroker_RandomPort makes sure the helper hands out a real http URL
// with a port (so two parallel brokers won't collide).
func TestStartBroker_RandomPort(t *testing.T) {
	_, url1, c1 := StartBroker(t)
	defer c1()
	_, url2, c2 := StartBroker(t)
	defer c2()

	if url1 == url2 {
		t.Fatalf("two brokers ended up with the same URL: %s", url1)
	}
	if !strings.HasPrefix(url1, "http://") || !strings.HasPrefix(url2, "http://") {
		t.Fatalf("URLs missing http:// prefix: %s, %s", url1, url2)
	}
}
