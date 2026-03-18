package cluster

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// Forwarder proxies HTTP requests to the correct broker when the
// current broker doesn't own the target partition.
type Forwarder struct {
	client  *http.Client
	cluster *Cluster
}

func NewForwarder(c *Cluster) *Forwarder {
	return &Forwarder{
		// No default timeout — we use the incoming request's context instead.
		// This lets long-poll requests with large wait_ms values work correctly.
		client:  &http.Client{},
		cluster: c,
	}
}

// Forward sends the request to the broker that owns the partition.
// Copies the original method, path, body, and headers.
// Returns the response body and status code from the remote broker.
func (f *Forwarder) Forward(brokerID string, r *http.Request) ([]byte, int, error) {
	info, ok := f.cluster.GetBroker(brokerID)
	if !ok {
		return nil, http.StatusBadGateway, fmt.Errorf("broker %s not found in cluster", brokerID)
	}

	// Read the original body so we can forward it
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("read request body: %w", err)
	}

	// Build the forwarded request
	url := info.Addr + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("create forward request: %w", err)
	}

	// Copy all original headers so auth, tracing, etc. survive forwarding
	for key, vals := range r.Header {
		for _, v := range vals {
			req.Header.Add(key, v)
		}
	}
	// Mark as forwarded so the remote broker doesn't forward again
	req.Header.Set("X-StreamQ-Forwarded", "true")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("forward to %s: %w", info.Addr, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("read forward response: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// IsForwarded checks if a request was already forwarded from another broker.
// Prevents infinite forwarding loops.
func IsForwarded(r *http.Request) bool {
	return r.Header.Get("X-StreamQ-Forwarded") == "true"
}
