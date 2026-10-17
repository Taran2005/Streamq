package broker

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/streamq/streamq/internal/cluster"
	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/topic"
)

// authInternal verifies the X-StreamQ-Internal header against the broker's
// configured token. Returns true if authorized; writes a 401 and returns false
// otherwise.
func (s *Server) authInternal(w http.ResponseWriter, r *http.Request) bool {
	got := r.Header.Get(cluster.InternalHeader)
	want := s.broker.Config.InternalToken
	if want == "" {
		want = "dev"
	}
	if got != want {
		common.WriteError(w, http.StatusUnauthorized, "invalid internal token")
		return false
	}
	return true
}

// replicatePush handles POST /internal/replicate/{topic}/{partition}.
// Leader pushes a batch of pre-stamped messages; follower appends them via
// Partition.AppendReplicated. Out-of-order offsets return 409 so the leader
// can fall back to backfill.
func (s *Server) replicatePush(w http.ResponseWriter, r *http.Request) {
	if !s.authInternal(w, r) {
		return
	}

	topicName := r.PathValue("topic")
	partitionID, perr := strconv.Atoi(r.PathValue("partition"))
	if topicName == "" || perr != nil {
		common.WriteError(w, http.StatusBadRequest, "bad replication path")
		return
	}

	epochStr := r.Header.Get("X-StreamQ-Leader-Epoch")
	if epochStr != "" {
		if epoch, err := strconv.ParseUint(epochStr, 10, 64); err == nil {
			if a, ok := s.broker.assignments.GetAssignment(topicName, partitionID); ok {
				if epoch < a.Epoch {
					common.WriteJSON(w, http.StatusConflict, map[string]any{
						"error": "stale_leader_epoch",
					})
					return
				}
			}
		}
	}

	maxBytes := int64(s.broker.Config.MaxMessageSize) * 100
	if maxBytes == 0 {
		maxBytes = 100 * 1024 * 1024 // 100MB default max batch
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	var req struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			common.WriteError(w, http.StatusRequestEntityTooLarge, "payload too large")
			return
		}
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	t, err := s.broker.GetTopic(topicName)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}
	if !p.IsLocal() {
		common.WriteError(w, http.StatusBadRequest, "partition has no local store")
		return
	}

	for _, m := range req.Messages {
		if _, err := p.AppendReplicated(m); err != nil {
			if strings.Contains(err.Error(), "follower offset gap") {
				common.WriteJSON(w, http.StatusConflict, map[string]any{
					"error":        "out_of_sync",
					"local_offset": p.LatestOffset(),
				})
				return
			}
			common.WriteError(w, http.StatusInternalServerError, "append replicated: "+err.Error())
			return
		}
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"high_offset": p.LatestOffset(),
	})
}

// replicateFollow handles GET /internal/follow/{topic}/{partition}?from=N&max=M.
// Follower pulls messages from the leader to catch up. Also reports the
// leader's current high watermark so the follower can advance its visible
// offset.
func (s *Server) replicateFollow(w http.ResponseWriter, r *http.Request) {
	if !s.authInternal(w, r) {
		return
	}

	topicName := r.PathValue("topic")
	partitionID, perr := strconv.Atoi(r.PathValue("partition"))
	if topicName == "" || perr != nil {
		common.WriteError(w, http.StatusBadRequest, "bad follow path")
		return
	}

	epochStr := r.Header.Get("X-StreamQ-Leader-Epoch")
	if epochStr != "" {
		if epoch, err := strconv.ParseUint(epochStr, 10, 64); err == nil {
			if a, ok := s.broker.assignments.GetAssignment(topicName, partitionID); ok {
				if epoch < a.Epoch {
					common.WriteJSON(w, http.StatusConflict, map[string]any{
						"error": "stale_leader_epoch",
					})
					return
				}
			}
		}
	}

	q := r.URL.Query()
	from := uint64(0)
	if v := q.Get("from"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			common.WriteError(w, http.StatusBadRequest, "invalid from: "+err.Error())
			return
		}
		from = n
	}
	max := 500
	if v := q.Get("max"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			common.WriteError(w, http.StatusBadRequest, "invalid max")
			return
		}
		max = n
	}
	follower := q.Get("follower")

	t, err := s.broker.GetTopic(topicName)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}
	p, err := t.GetPartition(partitionID)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}
	if !p.IsLocal() {
		common.WriteError(w, http.StatusBadRequest, "partition has no local store")
		return
	}

	msgs, err := p.Read(from, max)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "read: "+err.Error())
		return
	}

	// Record follower liveness for ISR tracking. Use the highest offset the
	// follower has now visible after this fetch (from + len(msgs)) since that's
	// what the follower will hold locally after applying the batch.
	if follower != "" && s.broker.leaderStates != nil {
		s.broker.leaderStates.noteFollowerSeen(topicName, partitionID, follower, from+uint64(len(msgs)), time.Now())
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"messages":    msgs,
		"high_offset": p.HighWatermark(),
	})
}

