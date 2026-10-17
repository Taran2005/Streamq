package topic

import (
	"fmt"
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
// Partition handles the notify channel for long-polling, the high
// watermark (max offset visible to consumers), and remote-replica state.
type Partition struct {
	ID    int
	store Store
	mu    sync.RWMutex

	notifyCh chan struct{}

	// highWatermark is the max offset visible to consumers. The leader bumps
	// it once an append is replicated to all ISR members; on followers it's
	// the offset returned by the latest /internal/follow response from the
	// leader. Until set explicitly via SetHighWatermark, reads see everything
	// in the local store (single-broker / pre-replication compat).
	hwSet         bool
	highWatermark uint64
}

// NewPartition creates a partition backed by the given store.
func NewPartition(id int, store Store) *Partition {
	return &Partition{
		ID:       id,
		store:    store,
		notifyCh: make(chan struct{}),
	}
}

// IsLocal returns true if this partition has a local store (not a remote placeholder).
func (p *Partition) IsLocal() bool {
	return p.store != nil
}

// Append adds a message to the partition via the store.
func (p *Partition) Append(key, value string) (uint64, error) {
	_, off, err := p.AppendAndReturn(key, value)
	return off, err
}

// AppendAndReturn appends like Append, but also returns the materialized
// Message (with offset and timestamp set) so the caller can replicate it to
// followers. Added in Phase 4 to support leader→follower push; we keep the
// old Append signature so non-cluster callers don't have to change.
func (p *Partition) AppendAndReturn(key, value string) (Message, uint64, error) {
	if p.store == nil {
		return Message{}, 0, fmt.Errorf("partition %d is remote (no local store)", p.ID)
	}

	msg := Message{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Partition: p.ID,
	}

	offset, err := p.store.Append(msg)
	if err != nil {
		return Message{}, 0, err
	}
	msg.Offset = offset

	p.broadcastNewData()
	return msg, offset, nil
}

// AppendReplicated writes a pre-stamped message from the leader to a follower's
// store. The offset on msg MUST equal the partition's next offset — otherwise
// the follower is out of sync and the caller should refetch via /internal/follow.
func (p *Partition) AppendReplicated(msg Message) (uint64, error) {
	if p.store == nil {
		return 0, fmt.Errorf("partition %d has no local store", p.ID)
	}
	expected := p.store.LatestOffset()
	if msg.Offset != expected {
		return 0, fmt.Errorf("follower offset gap: expected %d, got %d", expected, msg.Offset)
	}
	if msg.Partition == 0 {
		msg.Partition = p.ID
	}
	offset, err := p.store.Append(msg)
	if err != nil {
		return 0, err
	}
	p.broadcastNewData()
	return offset, nil
}

// broadcastNewData wakes up long-polling consumers.
func (p *Partition) broadcastNewData() {
	p.mu.Lock()
	oldCh := p.notifyCh
	p.notifyCh = make(chan struct{})
	close(oldCh)
	p.mu.Unlock()
}

// Read returns messages starting from the given offset.
func (p *Partition) Read(offset uint64, maxMessages int) ([]Message, error) {
	if p.store == nil {
		return nil, fmt.Errorf("partition %d is remote (no local store)", p.ID)
	}
	return p.store.Read(offset, maxMessages)
}

// LatestOffset returns the next offset that would be assigned.
func (p *Partition) LatestOffset() uint64 {
	if p.store == nil {
		return 0
	}
	return p.store.LatestOffset()
}

// HighWatermark returns the highest offset visible to consumers.
// If no HW has been set, returns the local store's latest offset (so
// single-broker setups see everything they wrote).
func (p *Partition) HighWatermark() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.hwSet {
		if p.store == nil {
			return 0
		}
		return p.store.LatestOffset()
	}
	return p.highWatermark
}

// SetHighWatermark updates the consumer-visible offset. Only ever moves
// forward; lower values are silently ignored.
func (p *Partition) SetHighWatermark(hw uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hwSet || hw > p.highWatermark {
		p.highWatermark = hw
		p.hwSet = true
	}
}

// ReadVisible returns messages strictly below the high watermark. Used by
// consumer-facing reads so un-replicated tail messages stay hidden.
func (p *Partition) ReadVisible(offset uint64, maxMessages int) ([]Message, error) {
	if p.store == nil {
		return nil, fmt.Errorf("partition %d is remote (no local store)", p.ID)
	}
	hw := p.HighWatermark()
	if offset >= hw {
		return []Message{}, nil
	}
	msgs, err := p.store.Read(offset, maxMessages)
	if err != nil {
		return nil, err
	}
	// Filter out anything at or beyond HW.
	out := msgs[:0]
	for _, m := range msgs {
		if m.Offset < hw {
			out = append(out, m)
		}
	}
	return out, nil
}

// WaitForData returns a channel that gets closed when new data arrives.
func (p *Partition) WaitForData() <-chan struct{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.notifyCh
}

// Close closes the underlying store.
func (p *Partition) Close() error {
	if p.store == nil {
		return nil
	}
	return p.store.Close()
}
