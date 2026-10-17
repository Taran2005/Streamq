package broker

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"github.com/streamq/streamq/internal/cluster"
)

func TestReplicatePush_BodySizeLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMessageSize = 1024 // 1KB message size limit -> 100KB batch limit
	cfg.InternalToken = "test-token"
	
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(b)
	ts := httptest.NewServer(srv.httpServer.Handler)
	t.Cleanup(func() {
		ts.Close()
		b.Close()
		ts.Client().CloseIdleConnections()
	})

	// Create a topic so the partition exists locally.
	// Since we are not in cluster mode for this test, GetTopic will create it locally.
	_, err = b.createLocalTopic("test", 1)
	if err != nil {
		t.Fatal(err)
	}

	// 200KB payload (exceeds 100KB limit)
	payload := make([]byte, 200*1024)
	payload[0] = '{'
	for i := 1; i < len(payload)-1; i++ {
		payload[i] = ' '
	}
	payload[len(payload)-1] = '}'
	
	req, err := http.NewRequest("POST", ts.URL+"/internal/replicate/test/0", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(cluster.InternalHeader, "test-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 413 Payload Too Large, got %d. Body: %s", resp.StatusCode, string(bodyBytes))
	}
}
