package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBroker_CrashRecovery_TornWrite(t *testing.T) {
	tempDir := t.TempDir()

	cfg := DefaultConfig()
	cfg.DataDir = tempDir

	// 1. Start a broker
	b1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Create topic and write a message
	topicName := "torn_write_test"
	if _, err := b1.createLocalTopic(topicName, 1); err != nil {
		t.Fatal(err)
	}
	
	t1, err := b1.GetTopic(topicName)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := t1.GetPartition(0)
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("valid message 1")
	if _, err := p1.Append("", string(msg)); err != nil {
		t.Fatal(err)
	}
	
	msg2 := []byte("valid message 2")
	if _, err := p1.Append("", string(msg2)); err != nil {
		t.Fatal(err)
	}

	// 3. Stop broker cleanly
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Wait a moment for files to sync and close
	time.Sleep(100 * time.Millisecond)

	// 4. Corrupt the log by appending garbage (simulating torn write)
	partitionDir := filepath.Join(tempDir, topicName, "0")
	logPath := filepath.Join(partitionDir, "0000000000.sqlog")

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				t.Logf("found: %s", path)
			}
			return nil
		})
		t.Fatalf("open log file: %v", err)
	}
	
	// Append exactly 12 bytes of garbage (not a full message header, or bad CRC)
	if _, err := f.Write([]byte("garbage tail")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	// 5. Restart the broker
	b2, err := New(cfg)
	if err != nil {
		t.Fatalf("broker failed to recover: %v", err)
	}
	defer b2.Close()

	t2, err := b2.GetTopic(topicName)
	if err != nil {
		t.Fatalf("get topic failed: %v", err)
	}
	p2, err := t2.GetPartition(0)
	if err != nil {
		t.Fatalf("get partition failed: %v", err)
	}

	// 6. Verify valid messages are intact
	msgs, err := p2.Read(0, 1)
	if err != nil || len(msgs) == 0 {
		t.Fatalf("consume offset 0 failed: %v", err)
	}
	if msgs[0].Value != string(msg) {
		t.Fatalf("expected %q, got %q", string(msg), msgs[0].Value)
	}

	msgs2, err := p2.Read(1, 1)
	if err != nil || len(msgs2) == 0 {
		t.Fatalf("consume offset 1 failed: %v", err)
	}
	if msgs2[0].Value != string(msg2) {
		t.Fatalf("expected %q, got %q", string(msg2), msgs2[0].Value)
	}

	// 7. Verify we can append new messages
	msg3 := "valid message 3 after recovery"
	offset, err := p2.Append("", msg3)
	if err != nil {
		t.Fatalf("produce after recovery failed: %v", err)
	}
	if offset != 2 {
		t.Fatalf("expected offset 2, got %d", offset)
	}
}
