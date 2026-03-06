package storage

import (
	"testing"

	"github.com/streamq/streamq/internal/topic"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	msg := topic.Message{
		Offset:    42,
		Key:       "user-123",
		Value:     "hello world",
		Timestamp: 1234567890,
		Partition: 3,
	}

	data := Encode(msg)
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if got.Offset != msg.Offset || got.Key != msg.Key || got.Value != msg.Value || got.Timestamp != msg.Timestamp {
		t.Errorf("roundtrip mismatch:\n  want: %+v\n  got:  %+v", msg, got)
	}
}

func TestEncodeDecodeEmptyKey(t *testing.T) {
	msg := topic.Message{Offset: 0, Key: "", Value: "data", Timestamp: 999}
	data := Encode(msg)
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got.Key != "" || got.Value != "data" {
		t.Errorf("mismatch: %+v", got)
	}
}

func TestDecodeCRCCorruption(t *testing.T) {
	msg := topic.Message{Offset: 1, Key: "k", Value: "v", Timestamp: 100}
	data := Encode(msg)

	// Corrupt one byte in the value
	data[len(data)-5] ^= 0xFF

	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected CRC error for corrupted data")
	}
}

func TestDecodeTooShort(t *testing.T) {
	_, err := Decode([]byte{0, 0, 0})
	if err == nil {
		t.Fatal("expected error for too-short data")
	}
}

func TestRecordSize(t *testing.T) {
	msg := topic.Message{Key: "abc", Value: "hello"}
	data := Encode(msg)
	expected := RecordSize(len(msg.Key), len(msg.Value))
	if len(data) != expected {
		t.Errorf("RecordSize=%d but Encode produced %d bytes", expected, len(data))
	}
}
