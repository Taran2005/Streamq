package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/streamq/streamq/internal/broker"
)

// TestConsumeGroup_HelpersEndToEnd exercises the helper functions used by
// consume-group against a real broker. The full daemon (signal handling,
// background heartbeat) isn't easy to test, so we drive the same operations
// the daemon would and confirm end-to-end behavior.
func TestConsumeGroup_HelpersEndToEnd(t *testing.T) {
	cfg := broker.DefaultConfig()
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	srv := broker.NewServer(b)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Create topic and produce some messages.
	postJSON := func(path string, body any) []byte {
		t.Helper()
		data, _ := json.Marshal(body)
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return raw
	}

	postJSON("/topics", map[string]any{"name": "t", "partitions": 1})
	postJSON("/topics/t/messages", map[string]any{"key": "a", "value": "1"})
	postJSON("/topics/t/messages", map[string]any{"key": "b", "value": "2"})
	postJSON("/topics/t/messages", map[string]any{"key": "c", "value": "3"})

	// Build the client and walk through join → fetch → commit just like the
	// daemon would.
	c := &groupClient{
		broker:    ts.URL,
		group:     "g",
		topics:    []string{"t"},
		strategy:  "range",
		sessionMs: 30_000,
		maxMsgs:   100,
	}
	if err := c.join(); err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(c.assignment()) != 1 || len(c.assignment()[0].Partitions) != 1 {
		t.Fatalf("expected single-partition assignment, got %+v", c.assignment())
	}

	msgs, err := fetchMessages(ts.URL, "t", 0, 0, 100)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Commit the last seen offset (2) — at-least-once means we commit the
	// offset we've finished processing, so a restart resumes at 3.
	err = c.commit([]map[string]any{{"topic": "t", "partition": 0, "offset": uint64(2)}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// On a fresh client, refreshCommittedOffsets should learn the commit.
	c2 := &groupClient{
		broker:   ts.URL,
		group:    "g",
		topics:   []string{"t"},
		strategy: "range",
	}
	if err := c2.join(); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if off, ok := c2.lookupOffset("t", 0); !ok || off != 2 {
		t.Fatalf("expected resume offset 2, got %d ok=%v", off, ok)
	}
}

func TestNumericCoercion(t *testing.T) {
	if asInt(float64(7)) != 7 {
		t.Fatal("float→int")
	}
	if asInt(int(7)) != 7 {
		t.Fatal("int→int")
	}
	if asUint64(float64(99)) != 99 {
		t.Fatal("float→uint64")
	}
	if asUint64(int(-1)) != 0 {
		t.Fatal("negative int→0")
	}
}
