package broker

import (
	"context"
	"fmt"
	"log"
	"net/http"
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
