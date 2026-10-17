package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/streamq/streamq/internal/topic"
)

// InternalHeader is sent on every leader→follower replication call so the
// follower can authenticate it. The value is the broker's configured
// InternalToken (default "dev", overridable via STREAMQ_INTERNAL_TOKEN).
//
// We deliberately don't reuse X-StreamQ-Forwarded — the forwarder is for
// user-facing produce/consume forwarding and has anti-loop logic; internal
// replication is a separate trust domain.
const InternalHeader = "X-StreamQ-Internal"

// Replicator fans out new appends from the leader to its in-sync followers
// over plain HTTP. It's a thin client — the broker decides WHEN to push (sync
// for acks=all, async for acks=0/1) and which followers are in ISR. Replicator
// just does the network call.
type Replicator struct {
	self      string
	forwarder *Forwarder
	token     string
	client    *http.Client
}

// NewReplicator builds a Replicator. forwarder is used only to look up broker
// addresses by ID; we don't reuse its HTTP client because we want a tighter
// timeout for internal traffic.
func NewReplicator(self string, fwd *Forwarder, token string) *Replicator {
	return &Replicator{
		self:      self,
		forwarder: fwd,
		token:     token,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// ReplicateResponse is what the follower returns from /internal/replicate.
type ReplicateResponse struct {
	HighOffset  uint64 `json:"high_offset"`
	Error       string `json:"error,omitempty"`
	LocalOffset uint64 `json:"local_offset,omitempty"`
}

// ErrFollowerOutOfSync is returned when a follower's local offset doesn't
// match the leader's expected next offset (gap or duplicate). The caller
// should fall back to /internal/follow to backfill.
type ErrFollowerOutOfSync struct {
	BrokerID    string
	LocalOffset uint64
}

func (e *ErrFollowerOutOfSync) Error() string {
	return fmt.Sprintf("follower %s out of sync at offset %d", e.BrokerID, e.LocalOffset)
}

// Push sends a batch of messages to the target follower's /internal/replicate
// endpoint. Returns the follower's reported high offset on success.
//
// If the follower reports 409 Conflict with an out_of_sync error, returns an
// *ErrFollowerOutOfSync so the leader can react (backfill, ISR shrink, etc).
func (r *Replicator) Push(ctx context.Context, target string, topicName string, partition int, epoch uint64, msgs []topic.Message) (uint64, error) {
	if r.forwarder == nil {
		return 0, fmt.Errorf("replicator: no forwarder configured")
	}
	info, ok := r.forwarder.cluster.GetBroker(target)
	if !ok {
		return 0, fmt.Errorf("replicator: unknown broker %s", target)
	}

	body, err := json.Marshal(struct {
		Messages []topic.Message `json:"messages"`
	}{Messages: msgs})
	if err != nil {
		return 0, fmt.Errorf("marshal replication payload: %w", err)
	}

	url := fmt.Sprintf("%s/internal/replicate/%s/%d", info.Addr, topicName, partition)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build replication request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(InternalHeader, r.token)
	req.Header.Set("X-StreamQ-Leader-Epoch", fmt.Sprintf("%d", epoch))

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("push to %s: %w", target, err)
	}
	defer resp.Body.Close()

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read replication response: %w", err)
	}

	var rr ReplicateResponse
	if len(rb) > 0 {
		// Tolerate empty bodies in error paths.
		_ = json.Unmarshal(rb, &rr)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return rr.HighOffset, nil
	case http.StatusConflict:
		return rr.LocalOffset, &ErrFollowerOutOfSync{BrokerID: target, LocalOffset: rr.LocalOffset}
	default:
		if rr.Error != "" {
			return 0, fmt.Errorf("replicate to %s: %s (status %d)", target, rr.Error, resp.StatusCode)
		}
		return 0, fmt.Errorf("replicate to %s: status %d", target, resp.StatusCode)
	}
}

// Fetch hits the leader's /internal/follow endpoint to pull messages starting
// at `from`. Returns the messages and the leader's reported high watermark.
func (r *Replicator) Fetch(ctx context.Context, target string, topicName string, partition int, epoch uint64, from uint64, max int) ([]topic.Message, uint64, error) {
	if r.forwarder == nil {
		return nil, 0, fmt.Errorf("replicator: no forwarder configured")
	}
	info, ok := r.forwarder.cluster.GetBroker(target)
	if !ok {
		return nil, 0, fmt.Errorf("replicator: unknown broker %s", target)
	}

	url := fmt.Sprintf("%s/internal/follow/%s/%d?from=%d&max=%d&follower=%s", info.Addr, topicName, partition, from, max, r.self)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build follow request: %w", err)
	}
	req.Header.Set(InternalHeader, r.token)
	req.Header.Set("X-StreamQ-Leader-Epoch", fmt.Sprintf("%d", epoch))

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch from %s: %w", target, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		rb, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("fetch from %s: status %d (%s)", target, resp.StatusCode, string(rb))
	}

	var body struct {
		Messages   []topic.Message `json:"messages"`
		HighOffset uint64          `json:"high_offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, 0, fmt.Errorf("decode follow response: %w", err)
	}
	return body.Messages, body.HighOffset, nil
}
