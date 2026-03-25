package storage

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/streamq/streamq/internal/topic"
)

func benchDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "streamq-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// --- Append benchmarks (write throughput) ---

func BenchmarkMemoryAppend(b *testing.B) {
	store := NewMemoryStore()
	msg := topic.Message{Key: "k", Value: strings.Repeat("x", 100)}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store.Append(msg)
	}
}

func BenchmarkDiskAppend_100B(b *testing.B) {
	benchDiskAppend(b, 100)
}

func BenchmarkDiskAppend_1KB(b *testing.B) {
	benchDiskAppend(b, 1024)
}

func BenchmarkDiskAppend_10KB(b *testing.B) {
	benchDiskAppend(b, 10*1024)
}

func BenchmarkDiskAppend_100KB(b *testing.B) {
	benchDiskAppend(b, 100*1024)
}

func benchDiskAppend(b *testing.B, valueSize int) {
	ds, err := NewDiskStore(benchDir(b), 64*1024*1024) // 64MB segments
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	msg := topic.Message{Key: "k", Value: strings.Repeat("x", valueSize)}

	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value))))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := ds.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Read benchmarks (read throughput) ---

func BenchmarkMemoryRead(b *testing.B) {
	store := NewMemoryStore()
	for i := 0; i < 100_000; i++ {
		store.Append(topic.Message{Key: "k", Value: strings.Repeat("x", 100)})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % 99_900)
		store.Read(offset, 100)
	}
}

func BenchmarkDiskRead_Sequential(b *testing.B) {
	benchDiskRead(b, 100, false)
}

func BenchmarkDiskRead_Random(b *testing.B) {
	benchDiskRead(b, 100, true)
}

func benchDiskRead(b *testing.B, valueSize int, random bool) {
	ds, err := NewDiskStore(benchDir(b), 64*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	numMessages := 50_000
	msg := topic.Message{Key: "k", Value: strings.Repeat("x", valueSize)}
	for i := 0; i < numMessages; i++ {
		ds.Append(msg)
	}

	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value)) * 100)) // reading 100 msgs
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var offset uint64
		if random {
			offset = uint64((i * 7919) % (numMessages - 100)) // pseudo-random via prime
		} else {
			offset = uint64(i % (numMessages - 100))
		}
		if _, err := ds.Read(offset, 100); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Segment rolling benchmark ---

func BenchmarkDiskAppend_WithRolling(b *testing.B) {
	// Small segments force frequent rolling — measures the overhead
	ds, err := NewDiskStore(benchDir(b), 64*1024) // 64KB segments
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	msg := topic.Message{Key: "k", Value: strings.Repeat("x", 100)}
	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value))))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := ds.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Encode/Decode benchmark ---

func BenchmarkEncode_100B(b *testing.B) {
	msg := topic.Message{Key: "key", Value: strings.Repeat("v", 100), Offset: 42, Timestamp: 1234567890}
	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value))))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Encode(msg)
	}
}

func BenchmarkDecode_100B(b *testing.B) {
	msg := topic.Message{Key: "key", Value: strings.Repeat("v", 100), Offset: 42, Timestamp: 1234567890}
	data := Encode(msg)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Concurrent append benchmark ---

func BenchmarkDiskAppend_Concurrent(b *testing.B) {
	ds, err := NewDiskStore(benchDir(b), 64*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	msg := topic.Message{Key: "k", Value: strings.Repeat("x", 100)}
	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value))))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := ds.Append(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Multi-partition parallel append (simulates real workload) ---

func BenchmarkMultiPartition_4(b *testing.B) {
	benchMultiPartition(b, 4)
}

func BenchmarkMultiPartition_16(b *testing.B) {
	benchMultiPartition(b, 16)
}

func benchMultiPartition(b *testing.B, numPartitions int) {
	stores := make([]*DiskStore, numPartitions)
	dir := benchDir(b)
	for i := range stores {
		ds, err := NewDiskStore(fmt.Sprintf("%s/p%d", dir, i), 64*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		stores[i] = ds
		defer ds.Close()
	}

	msg := topic.Message{Key: "k", Value: strings.Repeat("x", 100)}
	b.SetBytes(int64(RecordSize(len(msg.Key), len(msg.Value))))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			store := stores[i%numPartitions]
			if _, err := store.Append(msg); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
