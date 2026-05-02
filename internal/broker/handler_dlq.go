package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/consumergroup"
	"github.com/streamq/streamq/internal/topic"
)

// dlqRequest is the body of POST /groups/{group}/dlq. The "explicit DLQ"
// model — a consumer explicitly marks a message as poison and we route the
// payload + reason to __dlq.<topic>. We don't try to detect poison server-
// side; experience says that gets noisy and surprising.
type dlqRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Reason    string `json:"reason"`
}

// dlqEnvelope is what we write into the DLQ topic. Includes everything a
// human or downstream tool would need to investigate or reprocess.
type dlqEnvelope struct {
	SourceTopic     string `json:"source_topic"`
	SourcePartition int    `json:"source_partition"`
	SourceOffset    uint64 `json:"source_offset"`
	OriginalKey     string `json:"original_key"`
	OriginalValue   string `json:"original_value"`
	OriginalTime    int64  `json:"original_timestamp"`
	Reason          string `json:"reason"`
	Group           string `json:"group"`
}

// dlqGroup handles POST /groups/{group}/dlq.
//
// Auto-creates __dlq.<source-topic> on first use with the same partition
// count as the source (rf=1 in cluster mode for simplicity — a DLQ losing
// replicas is far less painful than a primary topic doing so).
func (s *Server) dlqGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")

	var req dlqRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Topic == "" {
		common.WriteError(w, http.StatusBadRequest, "topic required")
		return
	}

	// DLQ should land on the coordinator broker so the audit trail is local
	// to where the group lives. Forward there in cluster mode.
	if s.forwardToCoordinator(w, r, groupName) {
		return
	}

	// 1) Locate the source message so we can preserve the original key/value.
	src, err := s.broker.GetTopic(req.Topic)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}
	srcPart, err := src.GetPartition(req.Partition)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	var srcMsg topic.Message
	found := false
	if srcPart.IsLocal() {
		msgs, err := srcPart.Read(req.Offset, 1)
		if err == nil && len(msgs) > 0 && msgs[0].Offset == req.Offset {
			srcMsg = msgs[0]
			found = true
		}
	}
	// If we couldn't read the source (remote replica, or offset compacted),
	// proceed with an empty original payload. The reason + coordinates are
	// what matters; the original is best-effort.

	envelope := dlqEnvelope{
		SourceTopic:     req.Topic,
		SourcePartition: req.Partition,
		SourceOffset:    req.Offset,
		OriginalKey:     srcMsg.Key,
		OriginalValue:   srcMsg.Value,
		OriginalTime:    srcMsg.Timestamp,
		Reason:          req.Reason,
		Group:           groupName,
	}
	if !found {
		// Mark explicitly so consumers of the DLQ can tell the difference
		// between "we don't know the payload" and "the payload was empty".
		envelope.OriginalKey = ""
		envelope.OriginalValue = ""
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "marshal envelope: "+err.Error())
		return
	}

	// 2) Ensure __dlq.<topic> exists with the same partition count as the source.
	dlqTopic := consumergroup.DLQPrefix + req.Topic
	if err := s.broker.ensureDLQTopic(r.Context(), dlqTopic, len(src.Partitions)); err != nil {
		common.WriteError(w, http.StatusInternalServerError, "ensure dlq topic: "+err.Error())
		return
	}

	// 3) Append the envelope. Use the source partition so cross-DLQ replay
	// preserves the original ordering scheme.
	dlqT, err := s.broker.GetTopic(dlqTopic)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "get dlq topic: "+err.Error())
		return
	}
	dlqPartID := req.Partition
	if dlqPartID >= len(dlqT.Partitions) {
		dlqPartID = 0
	}
	dlqPart, err := dlqT.GetPartition(dlqPartID)
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "get dlq partition: "+err.Error())
		return
	}
	if !dlqPart.IsLocal() {
		common.WriteError(w, http.StatusInternalServerError, "dlq partition is remote on this broker")
		return
	}

	_, offset, err := dlqPart.AppendAndReturn(envelope.OriginalKey, string(payload))
	if err != nil {
		common.WriteError(w, http.StatusInternalServerError, "append dlq: "+err.Error())
		return
	}
	if !s.broker.Config.IsClusterMode() {
		dlqPart.SetHighWatermark(offset + 1)
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"dlq_topic":     dlqTopic,
		"dlq_partition": dlqPartID,
		"dlq_offset":    offset,
	})
}

// ensureDLQTopic lazy-creates the __dlq.<src> topic. Standalone mode creates
// locally; cluster mode requires the controller (callers on a non-controller
// node will see "not the controller" — Phase 5 doesn't paper that over).
func (b *Broker) ensureDLQTopic(ctx context.Context, name string, numPartitions int) error {
	if _, err := b.GetTopic(name); err == nil {
		return nil
	}
	if numPartitions <= 0 {
		numPartitions = 1
	}

	if !b.Config.IsClusterMode() {
		if _, err := b.createLocalTopic(name, numPartitions); err != nil {
			return err
		}
		return nil
	}

	// Cluster: only the controller can create. We forward DLQ requests to the
	// coordinator above; if the coordinator isn't the controller, we still
	// try here, and the assignment manager will give a clear "not the
	// controller" error the caller can debug.
	if !b.cluster.IsController() {
		return fmt.Errorf("dlq topic %s does not exist and this broker is not the controller", name)
	}
	brokers := b.cluster.GetBrokers()
	rf := 1 // DLQ is single-replica; durability of poison messages is best-effort.
	if rf > len(brokers) {
		rf = len(brokers)
	}
	assignments, err := b.assignments.AssignPartitions(ctx, name, numPartitions, rf, brokers)
	if err != nil {
		return err
	}
	if _, err := b.createPartitionsFromAssignment(name, numPartitions, assignments); err != nil {
		return err
	}
	return nil
}
