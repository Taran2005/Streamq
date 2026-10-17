package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/streamq/streamq/internal/topic"
)

// TestTornTailRecovery writes N messages, manually truncates a few
// trailing bytes off the last segment, and verifies recovery succeeds
// with the last (torn) message dropped and the rest intact.
func TestTornTailRecovery(t *testing.T) {
	dir := tempDir(t)

	ds, err := NewDiskStore(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	const n = 6
	for i := 0; i < n; i++ {
		if _, err := ds.Append(topic.Message{Key: "k", Value: "v", Timestamp: int64(i)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := ds.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Find the .sqlog file we just wrote.
	logPath := filepath.Join(dir, segmentName(0, ".sqlog"))
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	// Cut 5 bytes off the tail — well inside the last record but not
	// touching anything before it.
	if err := os.Truncate(logPath, info.Size()-5); err != nil {
		t.Fatalf("truncate log: %v", err)
	}
	// Also nuke the saved index so recovery has to replay the log
	// (otherwise loadIndex would mask the torn tail).
	_ = os.Remove(filepath.Join(dir, segmentName(0, ".sqidx")))

	// Reopen — should recover messages 0..n-2 cleanly.
	ds2, err := NewDiskStore(dir, 1024*1024)
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer ds2.Close()

	if got, want := ds2.LatestOffset(), uint64(n-1); got != want {
		t.Fatalf("LatestOffset after torn tail = %d, want %d", got, want)
	}

	msgs, err := ds2.Read(0, 100)
	if err != nil {
		t.Fatalf("read after torn tail: %v", err)
	}
	if len(msgs) != n-1 {
		t.Fatalf("expected %d surviving messages, got %d", n-1, len(msgs))
	}
	for i, m := range msgs {
		if m.Offset != uint64(i) {
			t.Errorf("message %d has offset %d", i, m.Offset)
		}
	}
}

// TestAppendFsyncsBeforeReturn is a soft indirect check that Append
// has actually persisted bytes by the time it returns. We append a
// message and then re-stat the file from a fresh handle to verify
// the bytes are visible. This doesn't strictly prove fsync was
// called (only that the data was at least written), but it catches
// the obvious regression of removing the Sync call.
func TestAppendFsyncsBeforeReturn(t *testing.T) {
	dir := tempDir(t)
	ds, err := NewDiskStore(dir, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if _, err := ds.Append(topic.Message{Key: "k", Value: "hello"}); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, segmentName(0, ".sqlog"))
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("log file empty after Append returned — data not flushed")
	}

	// Sanity: the length prefix in the file should match what Encode wrote.
	f, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	hdr := make([]byte, headerSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	got := binary.BigEndian.Uint32(hdr)
	if got == 0 {
		t.Fatalf("length prefix is zero after Append — write did not land")
	}
}
