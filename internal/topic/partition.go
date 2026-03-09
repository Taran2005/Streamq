package topic

import (
	"sync"
	"time"
)

// Store is the interface partition uses for message storage.
// Defined here to avoid circular imports (storage imports topic for Message).
type Store interface {
	Append(msg Message) (uint64, error)
	Read(offset uint64, maxMessages int) ([]Message, error)
	LatestOffset() uint64
	Close() error
}

// Partition is an ordered, append-only log backed by a Store.
// The Store handles actual data (memory or disk).
// Partition handles the notify channel for long-polling.
type Partition struct {
	ID    int
	store Store
	mu    sync.RWMutex

	notifyCh chan struct{}
}

// NewPartition creates a partition backed by the given store.
func NewPartition(id int, store Store) *Partition {
	return &Partition{
		ID:       id,
		store:    store,
		notifyCh: make(chan struct{}),
	}
}

// Append adds a message to the partition via the store.
func (p *Partition) Append(key, value string) (uint64, error) {
	msg := Message{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Partition: p.ID,
	}

	offset, err := p.store.Append(msg)
	if err != nil {
		return 0, err
	}

	// Wake up long-polling consumers
	p.mu.Lock()
	oldCh := p.notifyCh
	p.notifyCh = make(chan struct{})
	close(oldCh)
	p.mu.Unlock()

	return offset, nil
}

// Read returns messages starting from the given offset.
func (p *Partition) Read(offset uint64, maxMessages int) ([]Message, error) {
	return p.store.Read(offset, maxMessages)
}

// LatestOffset returns the next offset that would be assigned.
func (p *Partition) LatestOffset() uint64 {
	return p.store.LatestOffset()
}

// WaitForData returns a channel that gets closed when new data arrives.
func (p *Partition) WaitForData() <-chan struct{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.notifyCh
}

// Close closes the underlying store.
func (p *Partition) Close() error {
	return p.store.Close()
}
