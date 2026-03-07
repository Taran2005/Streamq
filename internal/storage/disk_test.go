package storage

import (
	"os"
	"testing"

	"github.com/streamq/streamq/internal/topic"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "streamq-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestDiskStoreAppendAndRead(t *testing.T) {
	ds, err := NewDiskStore(tempDir(t), 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Append 3 messages
	for i := 0; i < 3; i++ {
		msg := topic.Message{Key: "k", Value: "hello", Timestamp: int64(i)}
		off, err := ds.Append(msg)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, off)
		}
	}

	// Read all
	msgs, err := ds.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Value != "hello" || msgs[2].Offset != 2 {
		t.Errorf("unexpected messages: %+v", msgs)
	}
}

func TestDiskStoreReadFromOffset(t *testing.T) {
	ds, err := NewDiskStore(tempDir(t), 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	for i := 0; i < 5; i++ {
		ds.Append(topic.Message{Key: "", Value: "msg", Timestamp: int64(i)})
	}

	msgs, _ := ds.Read(3, 10)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages from offset 3, got %d", len(msgs))
	}
	if msgs[0].Offset != 3 || msgs[1].Offset != 4 {
		t.Errorf("wrong offsets: %d, %d", msgs[0].Offset, msgs[1].Offset)
	}
}

func TestDiskStoreReadPastEnd(t *testing.T) {
	ds, err := NewDiskStore(tempDir(t), 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	ds.Append(topic.Message{Value: "one"})

	msgs, _ := ds.Read(100, 10)
	if len(msgs) != 0 {
		t.Fatalf("expected empty, got %d", len(msgs))
	}
}

func TestDiskStoreSegmentRolling(t *testing.T) {
	// Tiny segment size to force rolling
	ds, err := NewDiskStore(tempDir(t), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Write enough messages to create multiple segments
	for i := 0; i < 20; i++ {
		_, err := ds.Append(topic.Message{Key: "k", Value: "value-data", Timestamp: int64(i)})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if len(ds.segments) <= 1 {
		t.Fatalf("expected multiple segments, got %d", len(ds.segments))
	}

	// Read all messages across segments
	msgs, err := ds.Read(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 20 {
		t.Fatalf("expected 20 messages across segments, got %d", len(msgs))
	}

	// Verify ordering
	for i, msg := range msgs {
		if msg.Offset != uint64(i) {
			t.Errorf("message %d has offset %d", i, msg.Offset)
		}
	}
}

func TestDiskStoreRecovery(t *testing.T) {
	dir := tempDir(t)

	// Write data and close
	ds, err := NewDiskStore(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		ds.Append(topic.Message{Key: "k", Value: "persistent", Timestamp: int64(i)})
	}
	ds.Close()

	// Reopen — should recover all messages
	ds2, err := NewDiskStore(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()

	if ds2.LatestOffset() != 5 {
		t.Fatalf("expected offset 5 after recovery, got %d", ds2.LatestOffset())
	}

	msgs, _ := ds2.Read(0, 100)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages after recovery, got %d", len(msgs))
	}
	if msgs[0].Value != "persistent" {
		t.Errorf("wrong value after recovery: %s", msgs[0].Value)
	}
}

func TestDiskStoreLatestOffset(t *testing.T) {
	ds, err := NewDiskStore(tempDir(t), 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if ds.LatestOffset() != 0 {
		t.Fatalf("expected 0 for empty store, got %d", ds.LatestOffset())
	}

	ds.Append(topic.Message{Value: "a"})
	ds.Append(topic.Message{Value: "b"})

	if ds.LatestOffset() != 2 {
		t.Fatalf("expected 2, got %d", ds.LatestOffset())
	}
}
