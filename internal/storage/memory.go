package storage

import (
	"sync"

	"github.com/streamq/streamq/internal/topic"
)

// MemoryStore keeps messages in a slice. Used for tests.
type MemoryStore struct {
	messages []topic.Message
	mu       sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		messages: make([]topic.Message, 0),
	}
}

func (m *MemoryStore) Append(msg topic.Message) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg.Offset = uint64(len(m.messages))
	m.messages = append(m.messages, msg)
	return msg.Offset, nil
}

func (m *MemoryStore) Read(offset uint64, maxMessages int) ([]topic.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if offset >= uint64(len(m.messages)) {
		return []topic.Message{}, nil
	}

	end := uint64(len(m.messages))
	if maxMessages > 0 && offset+uint64(maxMessages) < end {
		end = offset + uint64(maxMessages)
	}

	result := make([]topic.Message, end-offset)
	copy(result, m.messages[offset:end])
	return result, nil
}

func (m *MemoryStore) LatestOffset() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.messages))
}

func (m *MemoryStore) Close() error {
	return nil
}
