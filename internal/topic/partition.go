package topic

import (
	"sync"
	"time"
)

// Partition is an ordered, append-only list of messages.
// In Kafka terms: one partition = one ordered log.
//
// Why sync.RWMutex?
//   - Multiple consumers can READ at the same time (RLock)
//   - Only one producer can WRITE at a time (Lock)
//   - This is way faster than a regular Mutex when reads >> writes
type Partition struct {
	ID       int
	messages []Message
	mu       sync.RWMutex

	// notifyCh is used for long-polling.
	// When a new message arrives, we close this channel to wake up all waiting consumers.
	// Then we create a fresh channel for the next wait cycle.
	notifyCh chan struct{}
}

// NewPartition creates an empty partition with the given ID.
func NewPartition(id int) *Partition {
	return &Partition{
		ID:       id,
		messages: make([]Message, 0),
		notifyCh: make(chan struct{}),
	}
}

// Append adds a message to the end of the partition.
// It assigns the next sequential offset and the current timestamp.
// Returns the assigned offset.
func (p *Partition) Append(key, value string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := uint64(len(p.messages))
	msg := Message{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Partition: p.ID,
	}
	p.messages = append(p.messages, msg)

	// Wake up any consumers that are long-polling (waiting for new data).
	// Closing a channel wakes ALL goroutines blocked on it — like a broadcast signal.
	oldCh := p.notifyCh
	p.notifyCh = make(chan struct{})
	close(oldCh)

	return offset
}

// Read returns messages starting from the given offset.
// If offset is beyond the end, returns an empty slice (not an error).
// maxMessages limits how many messages to return (0 = no limit).
func (p *Partition) Read(offset uint64, maxMessages int) []Message {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset >= uint64(len(p.messages)) {
		return []Message{}
	}

	end := uint64(len(p.messages))
	if maxMessages > 0 && offset+uint64(maxMessages) < end {
		end = offset + uint64(maxMessages)
	}

	// Return a COPY so the caller can't mess with our internal slice.
	result := make([]Message, end-offset)
	copy(result, p.messages[offset:end])
	return result
}

// LatestOffset returns the offset that the NEXT message would get.
func (p *Partition) LatestOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return uint64(len(p.messages))
}

// WaitForData returns a channel that gets closed when new data arrives.
// Used for long-polling: consumer blocks on this channel until a producer writes something.
func (p *Partition) WaitForData() <-chan struct{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.notifyCh
}
