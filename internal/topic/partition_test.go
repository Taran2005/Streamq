package topic

import (
	"sync"
	"testing"
	"time"
)

func TestPartitionAppendAndRead(t *testing.T) {
	p := NewPartition(0)

	// Append 3 messages
	off0 := p.Append("k1", "hello")
	off1 := p.Append("k2", "world")
	off2 := p.Append("", "no key")

	if off0 != 0 || off1 != 1 || off2 != 2 {
		t.Fatalf("expected offsets 0,1,2 got %d,%d,%d", off0, off1, off2)
	}

	// Read all from offset 0
	msgs := p.Read(0, 0)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Key != "k1" || msgs[0].Value != "hello" {
		t.Errorf("message 0 mismatch: %+v", msgs[0])
	}
	if msgs[2].Value != "no key" {
		t.Errorf("message 2 mismatch: %+v", msgs[2])
	}

	// Read from offset 1
	msgs = p.Read(1, 0)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages from offset 1, got %d", len(msgs))
	}

	// Read with limit
	msgs = p.Read(0, 1)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message with limit, got %d", len(msgs))
	}

	// Read past end returns empty slice
	msgs = p.Read(100, 0)
	if len(msgs) != 0 {
		t.Fatalf("expected empty slice for read past end, got %v", msgs)
	}
}

func TestPartitionLatestOffset(t *testing.T) {
	p := NewPartition(0)

	if p.LatestOffset() != 0 {
		t.Fatalf("expected 0 for empty partition, got %d", p.LatestOffset())
	}

	p.Append("", "msg")
	if p.LatestOffset() != 1 {
		t.Fatalf("expected 1 after one append, got %d", p.LatestOffset())
	}
}

func TestPartitionConcurrentAppends(t *testing.T) {
	p := NewPartition(0)
	var wg sync.WaitGroup

	// 100 goroutines each appending 100 messages = 10,000 total
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

	// Verify all offsets are unique and sequential
	msgs := p.Read(0, 0)
	seen := make(map[uint64]bool)
	for _, m := range msgs {
		if seen[m.Offset] {
			t.Fatalf("duplicate offset: %d", m.Offset)
		}
		seen[m.Offset] = true
	}
}

func TestPartitionLongPolling(t *testing.T) {
	p := NewPartition(0)

	// Start a goroutine that waits for data
	got := make(chan struct{})
	go func() {
		ch := p.WaitForData()
		<-ch // blocks until data arrives
		close(got)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Produce a message — should unblock the waiter
	p.Append("", "wake up!")

	select {
	case <-got:
		// success — waiter was notified
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll waiter was not notified within 2 seconds")
	}
}
