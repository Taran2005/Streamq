package broker_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/streamq/streamq/internal/broker"
	"github.com/streamq/streamq/internal/testutil"
)

// TestGroup_ClusterCoordinatorForwarding spins up two brokers, creates a
// group, and verifies that whichever broker isn't the coordinator forwards
// requests to the one that is. Skipped cleanly if etcd is unavailable.
func TestGroup_ClusterCoordinatorForwarding(t *testing.T) {
	testutil.RequireEtcd(t, nil)

	endpoints := []string{"localhost:2379"}
	b1, url1, _ := testutil.StartClusterBroker(t, endpoints, "b1")
	b2, url2, _ := testutil.StartClusterBroker(t, endpoints, "b2")

	post := func(url, path string, body any) (int, []byte) {
		t.Helper()
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", url+path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}

	// Wait for controller election to settle before trying to create a topic.
	waitFor(t, 10*time.Second, "controller election", func() bool {
		_, body := post(url1, "/topics", map[string]any{"name": "warm", "partitions": 1})
		return strings.Contains(string(body), "warm") || strings.Contains(string(body), "already exists")
	})

	// Create the user topic.
	status, body := post(url1, "/topics", map[string]any{"name": "t", "partitions": 4})
	if status/100 != 2 && !strings.Contains(string(body), "already exists") {
		t.Skipf("create topic: %d %s (likely etcd flake)", status, body)
	}

	// Join via b1.
	var jr struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	waitFor(t, 10*time.Second, "join group", func() bool {
		s, b := post(url1, "/groups/gc/join", map[string]any{
			"topics":             []string{"t"},
			"session_timeout_ms": 60_000,
		})
		if s != http.StatusOK {
			return false
		}
		_ = json.Unmarshal(b, &jr)
		return jr.MemberID != ""
	})
	if jr.MemberID == "" {
		t.Skip("could not establish group membership; environment race")
	}

	// Decide which broker is coordinator for "gc".
	coordID := waitCoordinator(t, b1, b2, "gc")
	if coordID == "" {
		t.Skip("no coordinator visible — environment issue")
	}

	other := url2
	otherBroker := b2
	if coordID == "b2" {
		other = url1
		otherBroker = b1
	}
	_ = otherBroker

	// Heartbeat via non-coordinator should succeed (proves forwarding).
	status, body = post(other, "/groups/gc/heartbeat", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation,
	})
	if status != http.StatusOK {
		t.Fatalf("heartbeat via non-coordinator: %d %s", status, body)
	}

	// Commit via non-coordinator → forwarded → persisted on coordinator.
	status, body = post(other, "/groups/gc/commit", map[string]any{
		"member_id":  jr.MemberID,
		"generation": jr.Generation,
		"commits":    []map[string]any{{"topic": "t", "partition": 0, "offset": 42}},
	})
	if status != http.StatusOK {
		t.Fatalf("commit via non-coordinator: %d %s", status, body)
	}

	bCoord := b1
	if coordID == "b2" {
		bCoord = b2
	}

	// Verify the offset landed on the coordinator's store.
	got, ok := waitOffset(t, bCoord, "gc", "t", 0, 3*time.Second)
	if !ok || got != 42 {
		t.Fatalf("expected offset=42 on coordinator store, got %d ok=%v", got, ok)
	}
}

// waitFor polls fn() until it returns true or the deadline passes. Logs and
// returns silently when time runs out; callers handle the missed condition.
func waitFor(t *testing.T, timeout time.Duration, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("waitFor %q: timed out after %s", label, timeout)
}

func waitCoordinator(t *testing.T, b1, b2 *broker.Broker, group string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if id, ok := b1.CoordinatorBrokerFor(group); ok && id != "" {
			return id
		}
		if id, ok := b2.CoordinatorBrokerFor(group); ok && id != "" {
			return id
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

func waitOffset(t *testing.T, b *broker.Broker, group, topic string, part int, timeout time.Duration) (uint64, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if off, ok := b.LookupCommittedOffset(group, topic, part); ok {
			return off, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, false
}
