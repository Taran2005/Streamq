package consumergroup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/streamq/streamq/internal/topic"
)

func TestStore_ApplyAndLookup(t *testing.T) {
	s := NewStore()
	s.Apply("g1", "orders", 0, CommitRecord{Offset: 42, CommittedAt: 100, MemberID: "m1"})
	s.Apply("g1", "orders", 0, CommitRecord{Offset: 50, CommittedAt: 200, MemberID: "m1"})
	s.Apply("g1", "users", 3, CommitRecord{Offset: 7, CommittedAt: 150, MemberID: "m2"})
	s.Apply("g2", "orders", 0, CommitRecord{Offset: 99, CommittedAt: 300, MemberID: "x"})

	got, ok := s.Lookup("g1", "orders", 0)
	if !ok || got.Offset != 50 || got.MemberID != "m1" {
		t.Fatalf("g1/orders/0: got %+v ok=%v want offset=50", got, ok)
	}
	got, ok = s.Lookup("g1", "users", 3)
	if !ok || got.Offset != 7 {
		t.Fatalf("g1/users/3: got %+v ok=%v", got, ok)
	}
	if _, ok := s.Lookup("g1", "orders", 1); ok {
		t.Fatalf("expected miss for g1/orders/1")
	}
	if _, ok := s.Lookup("missing", "orders", 0); ok {
		t.Fatalf("expected miss for unknown group")
	}
}

func TestStore_LookupAll(t *testing.T) {
	s := NewStore()
	s.Apply("g1", "orders", 0, CommitRecord{Offset: 1})
	s.Apply("g1", "orders", 1, CommitRecord{Offset: 2})
	s.Apply("g1", "users", 0, CommitRecord{Offset: 3})

	all := s.LookupAll("g1")
	if len(all) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(all))
	}
	if all["orders"][0].Offset != 1 || all["orders"][1].Offset != 2 {
		t.Fatalf("unexpected orders: %v", all["orders"])
	}
	if all["users"][0].Offset != 3 {
		t.Fatalf("unexpected users: %v", all["users"])
	}
}

func TestDecodeMessage(t *testing.T) {
	rec := CommitRecord{Offset: 42, CommittedAt: time.Now().UnixNano(), MemberID: "abc"}
	body, _ := json.Marshal(rec)
	msg := topic.Message{
		Key:   CommitKey("group-1", "topic-x", 7),
		Value: string(body),
	}
	g, tn, p, r, err := DecodeMessage(msg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if g != "group-1" || tn != "topic-x" || p != 7 || r.Offset != 42 || r.MemberID != "abc" {
		t.Fatalf("decode: got group=%s topic=%s part=%d rec=%+v", g, tn, p, r)
	}
}

func TestDecodeMessage_MalformedKey(t *testing.T) {
	cases := []string{"", "no-colon", "only:two", "trailing:"}
	for _, k := range cases {
		_, _, _, _, err := DecodeMessage(topic.Message{Key: k, Value: "{}"})
		if err == nil {
			t.Fatalf("expected error for key %q", k)
		}
	}
}

func TestPartitionForGroup_Stable(t *testing.T) {
	// Same input always gives same partition.
	for i := 0; i < 50; i++ {
		if PartitionForGroup("my-group", 8) != PartitionForGroup("my-group", 8) {
			t.Fatalf("non-deterministic hash")
		}
	}
	// Different groups likely land on different partitions (statistical;
	// just confirm the helper returns within bounds).
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		p := PartitionForGroup(name, 8)
		if p < 0 || p >= 8 {
			t.Fatalf("partition for %s out of range: %d", name, p)
		}
	}
}

func TestCommitKey_RoundTrip(t *testing.T) {
	cases := []struct {
		group, topic string
		partition    int
	}{
		{"g", "t", 0},
		{"my-group", "my-topic", 12},
		{"x", "topic-with-dashes", 999},
	}
	for _, c := range cases {
		key := CommitKey(c.group, c.topic, c.partition)
		g, tn, p, err := parseCommitKey(key)
		if err != nil {
			t.Fatalf("parse %q: %v", key, err)
		}
		if g != c.group || tn != c.topic || p != c.partition {
			t.Fatalf("roundtrip %q: got group=%s topic=%s part=%d", key, g, tn, p)
		}
	}
}
