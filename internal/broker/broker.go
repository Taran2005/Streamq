package broker

import (
	"sync"

	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/topic"
)

// Broker is the central component — it owns all topics and routes requests.
// sync.RWMutex protects the topics map:
//   - Creating a topic: needs exclusive Lock (write)
//   - Reading/listing topics: needs shared RLock (read)
//   - Multiple readers at the same time? No problem.
type Broker struct {
	Config Config
	topics map[string]*topic.Topic
	mu     sync.RWMutex
}

// New creates a broker with the given config.
func New(cfg Config) *Broker {
	return &Broker{
		Config: cfg,
		topics: make(map[string]*topic.Topic),
	}
}

// CreateTopic creates a new topic with the given partition count
// Returns an error if the topic already exists.
func (b *Broker) CreateTopic(name string, partitions int) (*topic.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil, &common.AlreadyExistsError{Resource: "topic", Name: name}
	}

	t, err := topic.NewTopic(name, partitions)
	if err != nil {
		return nil, err
	}

	b.topics[name] = t
	return t, nil
}

// GetTopic returns a topic by name, or an error if it doesn't exist.
func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	if !exists {
		return nil, &common.NotFoundError{Resource: "topic", Name: name}
	}
	return t, nil
}

// ListTopics returns info about all topics.
func (b *Broker) ListTopics() []topic.TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	infos := make([]topic.TopicInfo, 0, len(b.topics))
	for _, t := range b.topics {
		infos = append(infos, t.Info())
	}
	return infos
}
