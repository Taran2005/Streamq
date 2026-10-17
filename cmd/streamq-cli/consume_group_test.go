package main

import (
	"bytes"
	"encoding/json"
	"errors"
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

// TestCommit_NoLocalAdvanceOn426 covers the commit-ordering bug: when the
// broker responds with 426 Upgrade Required, the CLI's local committed map
// must NOT advance — the broker still owns the older offset. If we let the
// local map race ahead, the next fetch after re-join skips past messages
// that were never persisted (data loss).
func TestCommit_NoLocalAdvanceOn426(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rejoin", http.StatusUpgradeRequired)
	}))
	defer ts.Close()

	c := &groupClient{
		broker:    ts.URL,
		group:     "g",
		memberID:  "m1",
		generation: 1,
		committed: map[string]map[int]uint64{},
	}

	err := c.commit([]map[string]any{{"topic": "t", "partition": 0, "offset": uint64(99)}})
	if !errors.Is(err, errNeedsRejoin) {
		t.Fatalf("expected errNeedsRejoin, got %v", err)
	}
	// Critical: the local map MUST still be empty. If 99 made it in, the
	// next fetch would skip offsets that were never committed.
	if got, ok := c.lookupOffset("t", 0); ok {
		t.Fatalf("local committed advanced on 426: got %d", got)
	}
}

// TestCommit_LocalAdvanceOnSuccess is the positive side of the same bug:
// when commit returns 2xx, the local map MUST move forward so a quick
// in-process restart doesn't re-deliver.
func TestCommit_LocalAdvanceOnSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	c := &groupClient{
		broker:     ts.URL,
		group:      "g",
		memberID:   "m1",
		generation: 1,
		committed:  map[string]map[int]uint64{},
	}

	if err := c.commit([]map[string]any{{"topic": "t", "partition": 0, "offset": uint64(42)}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, ok := c.lookupOffset("t", 0)
	if !ok || got != 42 {
		t.Fatalf("expected local commit at 42, got %d ok=%v", got, ok)
	}
}

// TestRefreshCommittedOffsets_404 makes sure a missing/erroring endpoint
// returns an error rather than silently zero-ing the local cache. The old
// code decoded the body regardless of status, so a 404 left the consumer
// reading from offset 0 and re-delivering committed messages.
func TestRefreshCommittedOffsets_404(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer ts.Close()

	c := &groupClient{
		broker: ts.URL,
		group:  "g",
		topics: []string{"t"},
		// Pre-seed an offset so we can confirm the call doesn't blow it away.
		committed: map[string]map[int]uint64{
			"t": {0: 55},
		},
	}
	err := c.refreshCommittedOffsets()
	if err == nil {
		t.Fatal("expected error on 404")
	}
	got, ok := c.lookupOffset("t", 0)
	if !ok || got != 55 {
		t.Fatalf("pre-seeded offset clobbered: got %d ok=%v", got, ok)
	}
}

// TestRefreshCommittedOffsets_2xx confirms the happy path still populates
// the local cache as before.
func TestRefreshCommittedOffsets_2xx(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"offsets":[{"topic":"t","partition":0,"committed_offset":11}]}`))
	}))
	defer ts.Close()

	c := &groupClient{
		broker:    ts.URL,
		group:     "g",
		topics:    []string{"t"},
		committed: map[string]map[int]uint64{},
	}
	if err := c.refreshCommittedOffsets(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, ok := c.lookupOffset("t", 0)
	if !ok || got != 11 {
		t.Fatalf("expected refreshed offset 11, got %d ok=%v", got, ok)
	}
}
