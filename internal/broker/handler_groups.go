package broker

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/consumergroup"
)

// GroupForwardedHeader is set by a coordinator-receiving broker so it doesn't
// forward again (mirrors X-StreamQ-Forwarded for produce/consume routing but
// kept separate so the two flows can be debugged independently).
const GroupForwardedHeader = "X-StreamQ-Group-Forwarded"

// isGroupForwarded reports whether the request has already been forwarded by
// another broker to its coordinator. Stops forwarding loops in their tracks.
func isGroupForwarded(r *http.Request) bool {
	return r.Header.Get(GroupForwardedHeader) == "true"
}

// forwardToCoordinator hops the request to whichever broker is the
// coordinator for `group`. Standalone mode never enters here (we're always
// the coordinator). Caller must NOT have already produced output.
//
// Returns true if the request was forwarded (handler must stop), false if we
// are the coordinator and should serve locally.
func (s *Server) forwardToCoordinator(w http.ResponseWriter, r *http.Request, group string) bool {
	b := s.broker
	if !b.Config.IsClusterMode() || isGroupForwarded(r) {
		return false
	}
	coord, ok := b.coordinatorBrokerFor(group)
	if !ok {
		common.WriteError(w, http.StatusServiceUnavailable, "no coordinator for group (offsets topic not ready)")
		return true
	}
	if coord == b.Config.BrokerID {
		return false
	}

	// Mark + forward. We rely on Forwarder.Forward to copy method, body, and
	// query string. It already sets X-StreamQ-Forwarded; we add our own
	// header so the receiver knows this is a group hop, not a produce hop.
	r.Header.Set(GroupForwardedHeader, "true")
	body, statusCode, err := b.forwarder.Forward(coord, r)
	if err != nil {
		common.WriteError(w, http.StatusBadGateway, "forward to coordinator failed: "+err.Error())
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-StreamQ-Forwarded-From", b.Config.BrokerID)
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
	return true
}

// joinGroup handles POST /groups/{group}/join.
func (s *Server) joinGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")
	if groupName == "" {
		common.WriteError(w, http.StatusBadRequest, "group name required")
		return
	}

	var req struct {
		MemberID         string   `json:"member_id"`
		Topics           []string `json:"topics"`
		SessionTimeoutMs int      `json:"session_timeout_ms"`
		Strategy         string   `json:"strategy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if len(req.Topics) == 0 {
		common.WriteError(w, http.StatusBadRequest, "at least one topic required")
		return
	}

	// Ensure offsets topic exists so the coordinator routing answers.
	if err := s.broker.ensureOffsetsTopic(r.Context()); err != nil {
		common.WriteError(w, http.StatusInternalServerError, "init offsets topic: "+err.Error())
		return
	}

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}

	res, err := s.broker.groups.coordinator.Join(r.Context(), consumergroup.JoinRequest{
		GroupName:        groupName,
		MemberID:         req.MemberID,
		Topics:           req.Topics,
		SessionTimeoutMs: req.SessionTimeoutMs,
		Strategy:         req.Strategy,
	})
	if err != nil {
		common.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"member_id":             res.MemberID,
		"generation":            res.Generation,
		"assignment":            res.Assignment,
		"rebalance_in_progress": res.RebalanceInProgress,
		"strategy":              res.Strategy,
	})
}

// heartbeatGroup handles POST /groups/{group}/heartbeat.
func (s *Server) heartbeatGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")
	var req struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.MemberID == "" {
		common.WriteError(w, http.StatusBadRequest, "member_id required")
		return
	}

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}

	if s.broker.groups == nil {
		common.WriteError(w, http.StatusServiceUnavailable, "consumer groups not initialized")
		return
	}

	rebalance, err := s.broker.groups.coordinator.Heartbeat(groupName, req.MemberID, req.Generation)
	switch {
	case errors.Is(err, consumergroup.ErrGenerationMismatch):
		// 426 Upgrade Required is the canonical "rejoin to bump generation" code.
		common.WriteJSON(w, http.StatusUpgradeRequired, map[string]any{
			"error":              "generation_mismatch",
			"rebalance_required": true,
		})
		return
	case errors.Is(err, consumergroup.ErrUnknownGroup):
		common.WriteError(w, http.StatusNotFound, "unknown group")
		return
	case errors.Is(err, consumergroup.ErrUnknownMember):
		// Treat unknown member as "you need to rejoin" so a consumer that
		// rebooted can recover cleanly.
		common.WriteJSON(w, http.StatusUpgradeRequired, map[string]any{
			"error":              "unknown_member",
			"rebalance_required": true,
		})
		return
	case err != nil:
		common.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	common.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"rebalance_required": rebalance,
	})
}

// leaveGroup handles POST /groups/{group}/leave.
func (s *Server) leaveGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")
	var req struct {
		MemberID string `json:"member_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.MemberID == "" {
		common.WriteError(w, http.StatusBadRequest, "member_id required")
		return
	}

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}
	if s.broker.groups == nil {
		common.WriteError(w, http.StatusServiceUnavailable, "consumer groups not initialized")
		return
	}

	err := s.broker.groups.coordinator.Leave(groupName, req.MemberID)
	switch {
	case errors.Is(err, consumergroup.ErrUnknownGroup):
		common.WriteError(w, http.StatusNotFound, "unknown group")
		return
	case errors.Is(err, consumergroup.ErrUnknownMember):
		// Idempotent leave — treat as success so callers don't have to
		// special-case retries.
		common.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	case err != nil:
		common.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	common.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// commitGroup handles POST /groups/{group}/commit.
func (s *Server) commitGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")
	var req struct {
		MemberID   string `json:"member_id"`
		Generation int    `json:"generation"`
		Commits    []struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    uint64 `json:"offset"`
		} `json:"commits"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.MemberID == "" {
		common.WriteError(w, http.StatusBadRequest, "member_id required")
		return
	}
	if len(req.Commits) == 0 {
		common.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}
	if s.broker.groups == nil {
		common.WriteError(w, http.StatusServiceUnavailable, "consumer groups not initialized")
		return
	}

	commits := make([]consumergroup.CommitInput, 0, len(req.Commits))
	for _, c := range req.Commits {
		commits = append(commits, consumergroup.CommitInput{
			Topic:     c.Topic,
			Partition: c.Partition,
			Offset:    c.Offset,
		})
	}
	err := s.broker.groups.coordinator.CommitOffsets(r.Context(), groupName, req.MemberID, req.Generation, commits)
	switch {
	case errors.Is(err, consumergroup.ErrGenerationMismatch):
		common.WriteJSON(w, http.StatusUpgradeRequired, map[string]any{
			"error":              "generation_mismatch",
			"rebalance_required": true,
		})
		return
	case errors.Is(err, consumergroup.ErrUnknownGroup):
		common.WriteError(w, http.StatusNotFound, "unknown group")
		return
	case errors.Is(err, consumergroup.ErrUnknownMember):
		common.WriteError(w, http.StatusNotFound, "unknown member")
		return
	case err != nil:
		common.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	common.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// listGroupOffsets handles GET /groups/{group}/offsets?topic=<t>.
func (s *Server) listGroupOffsets(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")
	topicFilter := r.URL.Query().Get("topic")

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}
	if s.broker.groups == nil {
		common.WriteError(w, http.StatusServiceUnavailable, "consumer groups not initialized")
		return
	}

	rows := s.broker.groups.coordinator.LookupOffsets(groupName, topicFilter)

	// Flatten to JSON shape: [{topic,partition,committed_offset}, ...].
	type row struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"committed_offset"`
	}
	out := []row{}
	for t, byPart := range rows {
		for p, rec := range byPart {
			out = append(out, row{Topic: t, Partition: p, Offset: rec.Offset})
		}
	}
	common.WriteJSON(w, http.StatusOK, map[string]any{
		"offsets": out,
	})
}

// listGroups handles GET /groups.
//
// In cluster mode this only returns groups whose coordinator is this broker.
// A future improvement could fan out across brokers, but for Phase 5 the
// per-broker view is sufficient — admin CLIs can hit each broker and merge.
func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	if s.broker.groups == nil {
		common.WriteJSON(w, http.StatusOK, map[string]any{"groups": []any{}})
		return
	}
	snaps := s.broker.groups.coordinator.List()

	type entry struct {
		Name       string   `json:"name"`
		Members    int      `json:"members"`
		Topics     []string `json:"topics"`
		Generation int      `json:"generation"`
		State      string   `json:"state"`
	}
	out := make([]entry, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, entry{
			Name:       s.Name,
			Members:    len(s.Members),
			Topics:     s.Topics,
			Generation: s.Generation,
			State:      s.State,
		})
	}
	common.WriteJSON(w, http.StatusOK, map[string]any{"groups": out})
}

// describeGroup handles GET /groups/{group}.
func (s *Server) describeGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("group")

	if s.forwardToCoordinator(w, r, groupName) {
		return
	}
	if s.broker.groups == nil {
		common.WriteError(w, http.StatusNotFound, "unknown group")
		return
	}

	snap, ok := s.broker.groups.coordinator.Describe(groupName)
	if !ok {
		common.WriteError(w, http.StatusNotFound, "unknown group")
		return
	}

	type member struct {
		ID         string                    `json:"id"`
		Topics     []string                  `json:"topics"`
		Assignment []consumergroup.Assignment `json:"assignment"`
	}
	members := make([]member, 0, len(snap.Members))
	for _, m := range snap.Members {
		members = append(members, member{
			ID:         m.ID,
			Topics:     m.Topics,
			Assignment: m.Assignment,
		})
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"name":       snap.Name,
		"state":      snap.State,
		"generation": snap.Generation,
		"strategy":   snap.Strategy,
		"topics":     snap.Topics,
		"members":    members,
	})
}

