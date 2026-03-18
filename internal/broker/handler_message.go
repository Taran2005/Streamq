package broker

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"time"

	"github.com/streamq/streamq/internal/cluster"
	"github.com/streamq/streamq/internal/common"
)

func (s *Server) produceMessage(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("name")

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

	offset, err := partition.Append(req.Key, req.Value)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "failed to store message: "+err.Error())
		return
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"topic":     topicName,
		"partition": partitionID,
		"offset":    offset,
	})
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
	messages, err := p.Read(params.Offset, params.MaxMessages)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "read failed: "+err.Error())
		return
	}

	if len(messages) == 0 && params.Timeout > 0 {
		timer := time.NewTimer(params.Timeout)
		defer timer.Stop()

		select {
		case <-notifyCh:
			messages, err = p.Read(params.Offset, params.MaxMessages)
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
