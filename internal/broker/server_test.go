package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

)

// helper: create a broker + server for testing.
// httptest.NewServer gives us a real HTTP server on a random port — no need
func setupTestServer(t *testing.T) (*httptest.Server, *Broker) {
	t.Helper()
	cfg := DefaultConfig()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(b)
	ts := httptest.NewServer(srv.httpServer.Handler)
	t.Cleanup(ts.Close)
	return ts, b
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCreateAndListTopics(t *testing.T) {
	ts, _ := setupTestServer(t)

	// Create topic
	resp := postJSON(t, ts.URL+"/topics", map[string]any{
		"name": "orders", "partitions": 3,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	body := decodeJSON(t, resp)
	if body["name"] != "orders" {
		t.Errorf("expected name 'orders', got %v", body["name"])
	}

	// List topics
	resp, err := http.Get(ts.URL + "/topics")
	if err != nil {
		t.Fatal(err)
	}
	body = decodeJSON(t, resp)
	topics := body["topics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
}

func TestDuplicateTopic(t *testing.T) {
	ts, _ := setupTestServer(t)

	postJSON(t, ts.URL+"/topics", map[string]any{"name": "t1", "partitions": 1}).Body.Close()
	resp := postJSON(t, ts.URL+"/topics", map[string]any{"name": "t1", "partitions": 1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate, got %d", resp.StatusCode)
	}
}

func TestProduceAndConsume(t *testing.T) {
	ts, _ := setupTestServer(t)

	// Create topic
	postJSON(t, ts.URL+"/topics", map[string]any{"name": "test", "partitions": 1})

	// Produce
	resp := postJSON(t, ts.URL+"/topics/test/messages", map[string]any{
		"key": "k1", "value": "hello",
	})

	
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce: expected 200, got %d", resp.StatusCode)
	}
	body := decodeJSON(t, resp)
	if body["offset"].(float64) != 0 {
		t.Errorf("expected offset 0, got %v", body["offset"])
	}

	// Produce another
	postJSON(t, ts.URL+"/topics/test/messages", map[string]any{
		"key": "k2", "value": "world",
	})

	// Consume
	resp, err := http.Get(ts.URL + "/topics/test/messages?partition=0&offset=0")
	if err != nil {
		t.Fatal(err)
	}
	body = decodeJSON(t, resp)
	msgs := body["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}

	first := msgs[0].(map[string]any)
	if first["key"] != "k1" || first["value"] != "hello" {
		t.Errorf("message 0 mismatch: %v", first)
	}
}

func TestConsumeFromOffset(t *testing.T) {
	ts, _ := setupTestServer(t)

	postJSON(t, ts.URL+"/topics", map[string]any{"name": "test", "partitions": 1})
	postJSON(t, ts.URL+"/topics/test/messages", map[string]any{"value": "a"})
	postJSON(t, ts.URL+"/topics/test/messages", map[string]any{"value": "b"})
	postJSON(t, ts.URL+"/topics/test/messages", map[string]any{"value": "c"})

	// Read from offset 2 — should only get "c"
	resp, _ := http.Get(ts.URL + "/topics/test/messages?partition=0&offset=2")
	body := decodeJSON(t, resp)
	msgs := body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message from offset 2, got %d", len(msgs))
	}
	if msgs[0].(map[string]any)["value"] != "c" {
		t.Errorf("expected 'c', got %v", msgs[0])
	}
}

func TestKeyBasedPartitioning(t *testing.T) {
	ts, _ := setupTestServer(t)

	postJSON(t, ts.URL+"/topics", map[string]any{"name": "test", "partitions": 10})

	// Same key should always go to same partition
	resp1 := postJSON(t, ts.URL+"/topics/test/messages", map[string]any{
		"key": "user-42", "value": "a",
	})
	body1 := decodeJSON(t, resp1)

	resp2 := postJSON(t, ts.URL+"/topics/test/messages", map[string]any{
		"key": "user-42", "value": "b",
	})
	body2 := decodeJSON(t, resp2)

	if body1["partition"] != body2["partition"] {
		t.Errorf("same key went to different partitions: %v vs %v",
			body1["partition"], body2["partition"])
	}
}

func TestTopicNotFound(t *testing.T) {
	ts, _ := setupTestServer(t)

	resp, _ := http.Get(ts.URL + "/topics/nope/messages?partition=0")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestMessageSizeLimit(t *testing.T) {
	ts, _ := setupTestServer(t)

	postJSON(t, ts.URL+"/topics", map[string]any{"name": "test", "partitions": 1})

	// Create a message that's too big (config default is 1MB)
	bigValue := make([]byte, 2*1024*1024) // 2MB
	for i := range bigValue {
		bigValue[i] = 'x'
	}

	resp := postJSON(t, ts.URL+"/topics/test/messages", map[string]any{
		"value": string(bigValue),
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
}

