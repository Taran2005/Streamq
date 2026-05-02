package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// groupHTTP gives us a tiny test client that abstracts the JSON wrapping.
type groupHTTP struct {
	t  *testing.T
	ts *httptest.Server
}

func newGroupHTTP(t *testing.T) (*groupHTTP, *Broker) {
	t.Helper()
	ts, b := setupTestServer(t)
	return &groupHTTP{t: t, ts: ts}, b
}

func (h *groupHTTP) do(method, path string, body any) (int, []byte) {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		rdr = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, h.ts.URL+path, rdr)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func TestGroup_JoinAssignAndHeartbeat(t *testing.T) {
	h, _ := newGroupHTTP(t)

	// Create a topic with 4 partitions.
	status, _ := h.do("POST", "/topics", map[string]any{"name": "orders", "partitions": 4})
	if status != http.StatusCreated {
		t.Fatalf("create topic: %d", status)
	}

	// Join group g1.
	status, body := h.do("POST", "/groups/g1/join", map[string]any{
		"topics": []string{"orders"},
	})
	if status != http.StatusOK {
		t.Fatalf("join: %d %s", status, body)
	}
	var jr struct {
		MemberID            string `json:"member_id"`
		Generation          int    `json:"generation"`
		Assignment          []struct {
			Topic      string `json:"topic"`
			Partitions []int  `json:"partitions"`
		} `json:"assignment"`
		RebalanceInProgress bool `json:"rebalance_in_progress"`
	}
	if err := json.Unmarshal(body, &jr); err != nil {
		t.Fatalf("decode join: %v body=%s", err, body)
	}
	if jr.MemberID == "" || jr.Generation == 0 {
		t.Fatalf("expected member_id + generation, got %+v", jr)
	}
	if jr.RebalanceInProgress {
		t.Fatal("solo member should finalize immediately")
	}
	if len(jr.Assignment) != 1 || len(jr.Assignment[0].Partitions) != 4 {
		t.Fatalf("solo member should own all 4 partitions, got %+v", jr.Assignment)
	}

	// Heartbeat.
	status, body = h.do("POST", "/groups/g1/heartbeat", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation,
	})
	if status != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", status, body)
	}
}

func TestGroup_HeartbeatWrongGenerationReturns426(t *testing.T) {
	h, _ := newGroupHTTP(t)
	h.do("POST", "/topics", map[string]any{"name": "t", "partitions": 1})

	_, body := h.do("POST", "/groups/g/join", map[string]any{"topics": []string{"t"}})
	var jr struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	_ = json.Unmarshal(body, &jr)

	status, _ := h.do("POST", "/groups/g/heartbeat", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation + 100,
	})
	if status != http.StatusUpgradeRequired {
		t.Fatalf("expected 426, got %d", status)
	}
}

func TestGroup_CommitAndLookup(t *testing.T) {
	h, _ := newGroupHTTP(t)

	h.do("POST", "/topics", map[string]any{"name": "t", "partitions": 2})

	_, body := h.do("POST", "/groups/g/join", map[string]any{
		"topics": []string{"t"},
	})
	var jr struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	_ = json.Unmarshal(body, &jr)

	status, _ := h.do("POST", "/groups/g/commit", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation,
		"commits": []map[string]any{
			{"topic": "t", "partition": 0, "offset": 5},
			{"topic": "t", "partition": 1, "offset": 9},
		},
	})
	if status != http.StatusOK {
		t.Fatalf("commit: %d", status)
	}

	status, body = h.do("GET", "/groups/g/offsets?topic=t", nil)
	if status != http.StatusOK {
		t.Fatalf("offsets: %d %s", status, body)
	}
	var got struct {
		Offsets []struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    uint64 `json:"committed_offset"`
		} `json:"offsets"`
	}
	_ = json.Unmarshal(body, &got)
	if len(got.Offsets) != 2 {
		t.Fatalf("expected 2 offset rows, got %d (%s)", len(got.Offsets), body)
	}
	byPart := make(map[int]uint64)
	for _, r := range got.Offsets {
		byPart[r.Partition] = r.Offset
	}
	if byPart[0] != 5 || byPart[1] != 9 {
		t.Fatalf("unexpected offsets: %+v", byPart)
	}
}

func TestGroup_LeaveAndRebalance(t *testing.T) {
	h, _ := newGroupHTTP(t)
	h.do("POST", "/topics", map[string]any{"name": "t", "partitions": 6})

	// First member joins.
	_, body := h.do("POST", "/groups/g/join", map[string]any{"topics": []string{"t"}})
	var m1 struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	_ = json.Unmarshal(body, &m1)

	// Second member joins → rebalance fires.
	_, body = h.do("POST", "/groups/g/join", map[string]any{"topics": []string{"t"}})
	var m2 struct {
		MemberID            string `json:"member_id"`
		Generation          int    `json:"generation"`
		RebalanceInProgress bool   `json:"rebalance_in_progress"`
	}
	_ = json.Unmarshal(body, &m2)

	// First member re-joins → finalize at new gen, both own 3 partitions.
	_, body = h.do("POST", "/groups/g/join", map[string]any{
		"member_id": m1.MemberID,
		"topics":    []string{"t"},
	})
	var m1b struct {
		Generation int `json:"generation"`
		Assignment []struct {
			Partitions []int `json:"partitions"`
		} `json:"assignment"`
	}
	_ = json.Unmarshal(body, &m1b)
	if len(m1b.Assignment) == 0 || len(m1b.Assignment[0].Partitions) != 3 {
		t.Fatalf("after rebalance m1 should own 3 partitions, got %+v", m1b)
	}

	// m2 leaves.
	status, _ := h.do("POST", "/groups/g/leave", map[string]any{"member_id": m2.MemberID})
	if status != http.StatusOK {
		t.Fatalf("leave: %d", status)
	}

	// m1 rejoins, finalizes.
	_, body = h.do("POST", "/groups/g/join", map[string]any{
		"member_id": m1.MemberID,
		"topics":    []string{"t"},
	})
	var m1c struct {
		RebalanceInProgress bool `json:"rebalance_in_progress"`
		Assignment          []struct {
			Partitions []int `json:"partitions"`
		} `json:"assignment"`
	}
	_ = json.Unmarshal(body, &m1c)
	if m1c.RebalanceInProgress {
		t.Fatalf("expected stable after sole member rejoins")
	}
	if len(m1c.Assignment) != 1 || len(m1c.Assignment[0].Partitions) != 6 {
		t.Fatalf("sole member should get all 6 partitions, got %+v", m1c.Assignment)
	}
}

func TestGroup_DescribeAndList(t *testing.T) {
	h, _ := newGroupHTTP(t)
	h.do("POST", "/topics", map[string]any{"name": "t", "partitions": 1})

	_, body := h.do("POST", "/groups/g1/join", map[string]any{"topics": []string{"t"}})
	var jr struct {
		MemberID string `json:"member_id"`
	}
	_ = json.Unmarshal(body, &jr)

	status, body := h.do("GET", "/groups", nil)
	if status != http.StatusOK {
		t.Fatalf("list: %d", status)
	}
	if !strings.Contains(string(body), "g1") {
		t.Fatalf("expected g1 in list, got %s", body)
	}

	status, body = h.do("GET", "/groups/g1", nil)
	if status != http.StatusOK {
		t.Fatalf("describe: %d", status)
	}
	var desc struct {
		Name    string `json:"name"`
		State   string `json:"state"`
		Members []struct {
			ID string `json:"id"`
		} `json:"members"`
	}
	_ = json.Unmarshal(body, &desc)
	if desc.Name != "g1" || len(desc.Members) != 1 || desc.Members[0].ID != jr.MemberID {
		t.Fatalf("describe wrong: %+v", desc)
	}
}

func TestGroup_DLQRoutesMessage(t *testing.T) {
	h, _ := newGroupHTTP(t)

	// Create source topic and produce one message.
	h.do("POST", "/topics", map[string]any{"name": "orders", "partitions": 2})
	status, body := h.do("POST", "/topics/orders/messages", map[string]any{
		"key":   "bad",
		"value": "garbled",
	})
	if status != http.StatusOK {
		t.Fatalf("produce: %d %s", status, body)
	}
	var prod struct {
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}
	_ = json.Unmarshal(body, &prod)

	// Member must join group g for the DLQ endpoint to be valid in cluster mode.
	// Standalone is permissive — group existence isn't enforced for DLQ, but
	// the route accepts {group} regardless.
	status, body = h.do("POST", "/groups/g/dlq", map[string]any{
		"topic":     "orders",
		"partition": prod.Partition,
		"offset":    prod.Offset,
		"reason":    "deserialize failed",
	})
	if status != http.StatusOK {
		t.Fatalf("dlq: %d %s", status, body)
	}
	var dr struct {
		DLQTopic string `json:"dlq_topic"`
	}
	_ = json.Unmarshal(body, &dr)
	if dr.DLQTopic != "__dlq.orders" {
		t.Fatalf("unexpected dlq topic: %s", dr.DLQTopic)
	}

	// Confirm the DLQ topic now exists (use internal=true so the listing
	// includes it).
	status, body = h.do("GET", "/topics?internal=true", nil)
	if status != http.StatusOK {
		t.Fatalf("list internal topics: %d", status)
	}
	if !strings.Contains(string(body), "__dlq.orders") {
		t.Fatalf("expected __dlq.orders in listing, got %s", body)
	}

	// Read the DLQ message — payload should be JSON envelope with reason.
	url := fmt.Sprintf("/topics/__dlq.orders/messages?partition=%d&offset=0", prod.Partition)
	status, body = h.do("GET", url, nil)
	if status != http.StatusOK {
		t.Fatalf("read dlq: %d %s", status, body)
	}
	if !strings.Contains(string(body), `deserialize failed`) {
		t.Fatalf("expected reason in DLQ payload, got %s", body)
	}
	if !strings.Contains(string(body), `garbled`) {
		t.Fatalf("expected original value in DLQ payload, got %s", body)
	}
}

func TestGroup_RejectReservedTopicCreate(t *testing.T) {
	h, _ := newGroupHTTP(t)
	status, _ := h.do("POST", "/topics", map[string]any{"name": "__consumer_offsets", "partitions": 1})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for reserved name, got %d", status)
	}
}

func TestGroup_InternalTopicHiddenFromListByDefault(t *testing.T) {
	h, _ := newGroupHTTP(t)

	// Trigger creation of __consumer_offsets via a join.
	h.do("POST", "/topics", map[string]any{"name": "t", "partitions": 1})
	h.do("POST", "/groups/g/join", map[string]any{"topics": []string{"t"}})

	// Default list should NOT include __consumer_offsets.
	_, body := h.do("GET", "/topics", nil)
	if strings.Contains(string(body), "__consumer_offsets") {
		t.Fatalf("expected __consumer_offsets hidden by default, got %s", body)
	}

	// ?internal=true should include it.
	_, body = h.do("GET", "/topics?internal=true", nil)
	if !strings.Contains(string(body), "__consumer_offsets") {
		t.Fatalf("expected __consumer_offsets visible with ?internal=true, got %s", body)
	}
}

func TestGroup_RecoverOffsetsOnRestart(t *testing.T) {
	// Use a real data dir so reopening the broker re-reads the offsets log.
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(b)
	ts := httptest.NewServer(srv.httpServer.Handler)

	// Use a small client helper inline so we don't depend on setupTestServer.
	do := func(method, path string, body any) (int, []byte) {
		var rdr io.Reader
		if body != nil {
			data, _ := json.Marshal(body)
			rdr = bytes.NewReader(data)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}

	do("POST", "/topics", map[string]any{"name": "t", "partitions": 1})
	_, body := do("POST", "/groups/g/join", map[string]any{"topics": []string{"t"}})
	var jr struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	_ = json.Unmarshal(body, &jr)

	if status, _ := do("POST", "/groups/g/commit", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation,
		"commits":    []map[string]any{{"topic": "t", "partition": 0, "offset": 99}},
	}); status != http.StatusOK {
		t.Fatalf("commit: %d", status)
	}

	// Shutdown and reopen.
	ts.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()

	rec, ok := b2.groups.store.Lookup("g", "t", 0)
	if !ok {
		t.Fatal("expected committed offset to survive restart")
	}
	if rec.Offset != 99 {
		t.Fatalf("expected offset=99, got %d", rec.Offset)
	}
}
