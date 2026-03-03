package broker

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"time"

	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/topic"
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

	var partition *topic.Partition
	if req.Key != "" {
		h := fnv.New64a()
		h.Write([]byte(req.Key))
		idx := int(h.Sum64() % uint64(len(t.Partitions)))
		partition = t.Partitions[idx]
	} else {
		partition = t.NextPartition()
	}

	offset := partition.Append(req.Key, req.Value)

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"topic":     topicName,
		"partition": partition.ID,
		"offset":    offset,
	})
}

// consumeParams holds parsed + validated query params for consume requests.
// Keeps the handler clean — parsing is separate from business logic.
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
	params, err := parseConsumeParams(r)
	if err != nil {
		common.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	t, err := s.broker.GetTopic(r.PathValue("name"))
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	p, err := t.GetPartition(params.Partition)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	// Grab the notify channel BEFORE reading. This prevents a race where
	// a producer appends between our Read and WaitForData — if we grabbed
	// the channel after, we'd get the new (unclosed) channel and miss the data.
	notifyCh := p.WaitForData()
	messages := p.Read(params.Offset, params.MaxMessages)

	if len(messages) == 0 && params.Timeout > 0 {
		timer := time.NewTimer(params.Timeout)
		defer timer.Stop()

		select {
		case <-notifyCh:
			messages = p.Read(params.Offset, params.MaxMessages)
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"messages": messages,
	})
}
