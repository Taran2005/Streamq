package topic

import (
	"sync"
	"testing"
	"time"
)

// testStore is a minimal in-memory Store for tests.
// Avoids importing storage package (which imports topic → cycle).
type testStore struct {
	messages []Message
	mu       sync.RWMutex
}

func (s *testStore) Append(msg Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.Offset = uint64(len(s.messages))
	s.messages = append(s.messages, msg)
	return msg.Offset, nil
}

func (s *testStore) Read(offset uint64, max int) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if offset >= uint64(len(s.messages)) {
		return []Message{}, nil
	}
	end := uint64(len(s.messages))
	if max > 0 && offset+uint64(max) < end {
		end = offset + uint64(max)
	}
	result := make([]Message, end-offset)
	copy(result, s.messages[offset:end])
	return result, nil
}

func (s *testStore) LatestOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint64(len(s.messages))
}

func (s *testStore) Close() error { return nil }

func newTestPartition(id int) *Partition {
	return NewPartition(id, &testStore{messages: make([]Message, 0)})
}

func TestPartitionAppendAndRead(t *testing.T) {
	p := newTestPartition(0)

	off0, _ := p.Append("k1", "hello")
	off1, _ := p.Append("k2", "world")
	off2, _ := p.Append("", "no key")

	if off0 != 0 || off1 != 1 || off2 != 2 {
		t.Fatalf("expected offsets 0,1,2 got %d,%d,%d", off0, off1, off2)
	}

	msgs, _ := p.Read(0, 0)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Key != "k1" || msgs[0].Value != "hello" {
		t.Errorf("message 0 mismatch: %+v", msgs[0])
	}
	if msgs[2].Value != "no key" {
		t.Errorf("message 2 mismatch: %+v", msgs[2])
	}

	msgs, _ = p.Read(1, 0)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages from offset 1, got %d", len(msgs))
	}

	msgs, _ = p.Read(0, 1)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message with limit, got %d", len(msgs))
	}

	msgs, _ = p.Read(100, 0)
	if len(msgs) != 0 {
		t.Fatalf("expected empty slice for read past end, got %v", msgs)
	}
}

func TestPartitionLatestOffset(t *testing.T) {
	p := newTestPartition(0)

	if p.LatestOffset() != 0 {
		t.Fatalf("expected 0 for empty partition, got %d", p.LatestOffset())
	}

	p.Append("", "msg")
	if p.LatestOffset() != 1 {
		t.Fatalf("expected 1 after one append, got %d", p.LatestOffset())
	}
}

func TestPartitionConcurrentAppends(t *testing.T) {
	p := newTestPartition(0)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.Append("", "msg")
			}
		}()
	}
	wg.Wait()

	if p.LatestOffset() != 10000 {
		t.Fatalf("expected 10000 messages after concurrent appends, got %d", p.LatestOffset())
	}

	msgs, _ := p.Read(0, 0)
	seen := make(map[uint64]bool)
	for _, m := range msgs {
		if seen[m.Offset] {
			t.Fatalf("duplicate offset: %d", m.Offset)
		}
		seen[m.Offset] = true
	}
}

func TestPartitionLongPolling(t *testing.T) {
	p := newTestPartition(0)

	got := make(chan struct{})
	go func() {
		ch := p.WaitForData()
		<-ch
		close(got)
	}()

	time.Sleep(50 * time.Millisecond)
	p.Append("", "wake up!")

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll waiter was not notified within 2 seconds")
	}
}
