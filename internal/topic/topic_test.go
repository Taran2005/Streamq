package topic

import (
	"testing"
)

func TestNewTopicValid(t *testing.T) {
	tp, err := NewTopic("orders", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp.Name != "orders" {
		t.Errorf("expected name 'orders', got %q", tp.Name)
	}
	if len(tp.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(tp.Partitions))
	}
}

func TestNewTopicInvalidNames(t *testing.T) {
	bad := []string{"", "has spaces", "special!", "a/b", ".hidden"}
	for _, name := range bad {
		_, err := NewTopic(name, 1)
		if err == nil {
			t.Errorf("expected error for topic name %q, got nil", name)
		}
	}
}

func TestNewTopicInvalidPartitions(t *testing.T) {
	_, err := NewTopic("test", 0)
	if err == nil {
		t.Error("expected error for 0 partitions")
	}
	_, err = NewTopic("test", -1)
	if err == nil {
		t.Error("expected error for -1 partitions")
	}
	_, err = NewTopic("test", 1025)
	if err == nil {
		t.Error("expected error for 1025 partitions")
	}
}

func TestGetPartition(t *testing.T) {
	tp, _ := NewTopic("test", 3)

	p, err := tp.GetPartition(0)
	if err != nil || p.ID != 0 {
		t.Errorf("expected partition 0, got %v (err: %v)", p, err)
	}

	_, err = tp.GetPartition(5)
	if err == nil {
		t.Error("expected error for out-of-range partition")
	}

	_, err = tp.GetPartition(-1)
	if err == nil {
		t.Error("expected error for negative partition")
	}
}

func TestRoundRobin(t *testing.T) {
	tp, _ := NewTopic("test", 3)

	// Round-robin should cycle through partitions 0, 1, 2, 0, 1, 2...
	ids := make([]int, 6)
	for i := 0; i < 6; i++ {
		ids[i] = tp.NextPartition().ID
	}

	expected := []int{0, 1, 2, 0, 1, 2}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("round-robin step %d: expected partition %d, got %d", i, expected[i], id)
		}
	}
}

func TestTopicInfo(t *testing.T) {
	tp, _ := NewTopic("events", 5)
	info := tp.Info()
	if info.Name != "events" || info.PartitionCount != 5 {
		t.Errorf("unexpected info: %+v", info)
	}
}
