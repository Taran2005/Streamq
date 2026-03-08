package topic

import (
	"testing"
)

func makePartitions(n int) []*Partition {
	parts := make([]*Partition, n)
	for i := 0; i < n; i++ {
		parts[i] = NewPartition(i, &testStore{messages: make([]Message, 0)})
	}
	return parts
}

func TestValidateTopicParams(t *testing.T) {
	if err := ValidateTopicParams("orders", 3); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateTopicInvalidNames(t *testing.T) {
	bad := []string{"", "has spaces", "special!", "a/b", ".hidden"}
	for _, name := range bad {
		if err := ValidateTopicParams(name, 1); err == nil {
			t.Errorf("expected error for topic name %q, got nil", name)
		}
	}
}

func TestValidateTopicInvalidPartitions(t *testing.T) {
	if err := ValidateTopicParams("test", 0); err == nil {
		t.Error("expected error for 0 partitions")
	}
	if err := ValidateTopicParams("test", -1); err == nil {
		t.Error("expected error for -1 partitions")
	}
	if err := ValidateTopicParams("test", 1025); err == nil {
		t.Error("expected error for 1025 partitions")
	}
}

func TestNewTopic(t *testing.T) {
	tp := NewTopic("orders", makePartitions(3))
	if tp.Name != "orders" {
		t.Errorf("expected name 'orders', got %q", tp.Name)
	}
	if len(tp.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(tp.Partitions))
	}
}

func TestGetPartition(t *testing.T) {
	tp := NewTopic("test", makePartitions(3))

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
	tp := NewTopic("test", makePartitions(3))

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
	tp := NewTopic("events", makePartitions(5))
	info := tp.Info()
	if info.Name != "events" || info.PartitionCount != 5 {
		t.Errorf("unexpected info: %+v", info)
	}
}
