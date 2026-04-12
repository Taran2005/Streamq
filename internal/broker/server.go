package broker

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/streamq/streamq/internal/common"
)

type Server struct {
	broker     *Broker
	httpServer *http.Server
}

func NewServer(b *Broker) *Server {
	s := &Server{broker: b}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics", s.createTopic)
	mux.HandleFunc("GET /topics", s.listTopics)
	mux.HandleFunc("POST /topics/{name}/messages", s.produceMessage)
	mux.HandleFunc("GET /topics/{name}/messages", s.consumeMessages)
	mux.HandleFunc("GET /metadata", s.getMetadata)

	// Internal replication endpoints (leader→follower push + follower→leader pull).
	// Auth is by X-StreamQ-Internal header; see handler_replication.go.
	mux.HandleFunc("POST /internal/replicate/{topic}/{partition}", s.replicatePush)
	mux.HandleFunc("GET /internal/follow/{topic}/{partition}", s.replicateFollow)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", b.Config.Port),
		Handler: mux,
	}

	return s
}

func (s *Server) Start() error {
	log.Printf("StreamQ broker listening on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Handler returns the HTTP handler for use with custom listeners (e.g. benchmarks).
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// getMetadata returns cluster topology: which brokers exist and which
// broker owns which partition. Clients use this to route directly.
func (s *Server) getMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.broker.Config.IsClusterMode() {
		common.WriteJSON(w, http.StatusOK, map[string]any{
			"mode":    "standalone",
			"broker":  s.broker.Config.BrokerID,
			"brokers": []any{},
			"topics":  map[string]any{},
		})
		return
	}

	brokers := s.broker.cluster.GetBrokers()
	brokerList := make([]map[string]string, 0, len(brokers))
	for _, b := range brokers {
		brokerList = append(brokerList, map[string]string{
			"id":   b.ID,
			"addr": b.Addr,
		})
	}

	topicMap := make(map[string]any)
	for _, info := range s.broker.ListTopics() {
		assignments := s.broker.assignments.GetTopicAssignments(info.Name)
		partitions := make(map[string]string, len(assignments))
		for pid, brokerID := range assignments {
			partitions[fmt.Sprintf("%d", pid)] = brokerID
		}
		topicMap[info.Name] = partitions
	}

	common.WriteJSON(w, http.StatusOK, map[string]any{
		"mode":       "cluster",
		"broker":     s.broker.Config.BrokerID,
		"controller": s.broker.cluster.IsController(),
		"brokers":    brokerList,
		"topics":     topicMap,
	})
}
