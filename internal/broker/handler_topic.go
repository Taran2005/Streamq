package broker

import (
	"encoding/json"
	"net/http"

	"github.com/streamq/streamq/internal/common"
)

func (s *Server) createTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		common.WriteError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Name == "" {
		common.WriteError(w, http.StatusBadRequest, "topic name is required")
		return
	}

	if req.Partitions <= 0 {
		req.Partitions = 1
	}

	t, err := s.broker.CreateTopic(req.Name, req.Partitions)
	if err != nil {
		common.WriteAppError(w, err)
		return
	}

	common.WriteJSON(w, http.StatusCreated, t.Info())
}

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.ListTopics()
	common.WriteJSON(w, http.StatusOK, map[string]any{
		"topics": topics,
	})
}
