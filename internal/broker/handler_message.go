package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/streamq/streamq/internal/cluster"
	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/topic"
)

// parseAcks returns the desired ack mode from the request query. Defaults to
// "1" (leader-write-confirmed). Valid values: "0", "1", "all".
func parseAcks(r *http.Request) string {
	v := r.URL.Query().Get("acks")
	switch v {
	case "0", "1", "all":
		return v
	case "":
		return "1"
	default:
		return "1"
	}
}

func (s *Server) produceMessage(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("name")

	// Reject new produces while shutting down so clients can fail over.
	if s.broker.IsShuttingDown() {
		common.WriteError(w, http.StatusServiceUnavailable, "broker shutting down")
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if len(req.Value) > s.broker.Config.MaxMessageSize {
		common.WriteError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("message too large: %d bytes (max %d)", len(req.Value), s.broker.Config.MaxMessageSize))
		return
	}

	acks := parseAcks(r)

	t, err := s.broker.GetTopic(topicName)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	// Pick partition
	var partitionID int
	if req.Key != "" {
		h := fnv.New64a()
		h.Write([]byte(req.Key))
		partitionID = int(h.Sum64() % uint64(len(t.Partitions)))
	} else {
		p := t.NextPartition()
		partitionID = p.ID
	}

	// In cluster mode, check if we own this partition
	if s.broker.Config.IsClusterMode() && !cluster.IsForwarded(r) {
		if !s.broker.IsPartitionLocal(topicName, partitionID) {
			s.forwardRequest(w, r, topicName, partitionID)
			return
		}
	}

	partition, err := t.GetPartition(partitionID)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	msg, offset, err := partition.AppendAndReturn(req.Key, req.Value)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "failed to store message: "+err.Error())
		return
	}

	// In standalone (single-broker) mode, acks is meaningless — there's no
	// replication. Set HW so consumers see it and return.
	if !s.broker.Config.IsClusterMode() {
		partition.SetHighWatermark(offset + 1)
		common.WriteJSON(w, http.StatusOK, map[string]any{
			"topic":     topicName,
			"partition": partitionID,
			"offset":    offset,
		})
		return
	}

	// Cluster mode: replicate to followers per acks setting.
	switch acks {
	case "all":
		isr := s.broker.leaderStates.inSyncReplicas(topicName, partitionID)
		if err := s.replicateSync(r.Context(), topicName, partitionID, isr, []topic.Message{msg}); err != nil {
			// Roll back HW: we never advanced it, and we surface failure.
			if errors.Is(err, errInsufficientISR) {
				common.WriteError(w, http.StatusServiceUnavailable, "insufficient_isr")
				return
			}
			common.WriteError(w, http.StatusInternalServerError, "replicate failed: "+err.Error())
			return
		}
		// All ISR followers acked — safe to advance HW.
		partition.SetHighWatermark(offset + 1)
	case "0", "1":
		// Leader durability is enough to advance HW (semantics: at-least-once,
		// loses data only if leader dies before any follower catches up).
		partition.SetHighWatermark(offset + 1)
		// Best-effort fan-out in background.
		s.broker.replicateWG.Add(1)
		go func(msg topic.Message) {
			defer s.broker.replicateWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s.replicateBestEffort(ctx, topicName, partitionID, []topic.Message{msg})
		}(msg)
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"topic":     topicName,
		"partition": partitionID,
		"offset":    offset,
	})
}

// errInsufficientISR is returned when acks=all is requested but the leader
// can't reach every in-sync follower.
var errInsufficientISR = errors.New("insufficient_isr")

// replicateSync pushes msgs to every ISR follower and blocks until all succeed
// or the first one fails. Returns errInsufficientISR on any failure so the
// handler can answer 503.
func (s *Server) replicateSync(ctx context.Context, topicName string, partition int, followers []string, msgs []topic.Message) error {
	if s.broker.replicator == nil || len(followers) == 0 {
		return nil // nothing to do
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(followers))
	for _, f := range followers {
		wg.Add(1)
		go func(follower string) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			hw, err := s.broker.replicator.Push(pctx, follower, topicName, partition, msgs)
			cancel()
			if err != nil {
				errs <- err
				return
			}
			// Follower accepted the batch — note its new offset for ISR tracking.
			if s.broker.leaderStates != nil {
				s.broker.leaderStates.noteFollowerSeen(topicName, partition, follower, hw, time.Now())
			}
		}(f)
	}
	wg.Wait()
	close(errs)
	if err, ok := <-errs; ok {
		return fmt.Errorf("%w: %v", errInsufficientISR, err)
	}
	return nil
}

// replicateBestEffort fires replication at every non-self replica without
// waiting on it (used for acks=0 / acks=1). Failures are logged but don't
// affect the producer.
func (s *Server) replicateBestEffort(ctx context.Context, topicName string, partition int, msgs []topic.Message) {
	if s.broker.replicator == nil {
		return
	}
	followers := s.broker.leaderStates.allReplicas(topicName, partition)
	for _, f := range followers {
		hw, err := s.broker.replicator.Push(ctx, f, topicName, partition, msgs)
		if err != nil {
			// Out-of-sync: follower will catch up via /internal/follow loop —
			// expected, not noisy. Other errors get swallowed too because
			// this is best-effort by design.
			var outOfSync *cluster.ErrFollowerOutOfSync
			_ = errors.As(err, &outOfSync)
			continue
		}
		s.broker.leaderStates.noteFollowerSeen(topicName, partition, f, hw, time.Now())
	}
}

// consumeParams holds parsed + validated query params for consume requests.
type consumeParams struct {
	Partition   int
	Offset      uint64
	Timeout     time.Duration
	MaxMessages int
}

func parseConsumeParams(r *http.Request) (consumeParams, error) {
	query := r.URL.Query()

	partitionStr := query.Get("partition")
	if partitionStr == "" {
		return consumeParams{}, fmt.Errorf("missing 'partition' query parameter")
	}
	partitionID, err := strconv.Atoi(partitionStr)
	if err != nil {
		return consumeParams{}, fmt.Errorf("invalid partition: %w", err)
	}

	offset := uint64(0)
	if s := query.Get("offset"); s != "" {
		offset, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return consumeParams{}, fmt.Errorf("invalid offset: %w", err)
		}
	}

	timeout := time.Duration(0)
	if s := query.Get("timeout"); s != "" {
		timeout, err = time.ParseDuration(s)
		if err != nil {
			return consumeParams{}, fmt.Errorf("invalid timeout: %w", err)
		}
		if timeout > 30*time.Second {
			timeout = 30 * time.Second
		}
	}

	maxMessages := 100
	if s := query.Get("max"); s != "" {
		maxMessages, err = strconv.Atoi(s)
		if err != nil || maxMessages <= 0 {
			return consumeParams{}, fmt.Errorf("invalid max: must be a positive integer")
		}
	}

	return consumeParams{
		Partition:   partitionID,
		Offset:      offset,
		Timeout:     timeout,
		MaxMessages: maxMessages,
	}, nil
}

func (s *Server) consumeMessages(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("name")

	params, err := parseConsumeParams(r)
	if err != nil {
		common.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// In cluster mode, check if we own this partition
	if s.broker.Config.IsClusterMode() && !cluster.IsForwarded(r) {
		if !s.broker.IsPartitionLocal(topicName, params.Partition) {
			s.forwardRequest(w, r, topicName, params.Partition)
			return
		}
	}

	t, err := s.broker.GetTopic(topicName)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	p, err := t.GetPartition(params.Partition)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	notifyCh := p.WaitForData()
	// ReadVisible gates reads at the high watermark so consumers never see
	// tail messages that haven't been replicated yet. In standalone mode HW
	// equals LatestOffset (set on every leader append), so this is a no-op.
	messages, err := p.ReadVisible(params.Offset, params.MaxMessages)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "read failed: "+err.Error())
		return
	}

	if len(messages) == 0 && params.Timeout > 0 {
		timer := time.NewTimer(params.Timeout)
		defer timer.Stop()

		select {
		case <-notifyCh:
			messages, err = p.ReadVisible(params.Offset, params.MaxMessages)
			if err != nil {
				common.WriteError(w, http.StatusInternalServerError, "read failed: "+err.Error())
				return
			}
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"messages": messages,
	})
}

// forwardRequest forwards a produce/consume request to the broker that owns the partition.
func (s *Server) forwardRequest(w http.ResponseWriter, r *http.Request, topicName string, partition int) {
	ownerID, ok := s.broker.GetPartitionOwner(topicName, partition)
	if !ok {
		common.WriteError(w, http.StatusInternalServerError, "no owner for partition")
		return
	}

	body, statusCode, err := s.broker.forwarder.Forward(ownerID, r)
	if err != nil {
		common.WriteError(w, http.StatusBadGateway, "forward failed: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-StreamQ-Forwarded-From", s.broker.Config.BrokerID)
	w.WriteHeader(statusCode)
	w.Write(body)
}
