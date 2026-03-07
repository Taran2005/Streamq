package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/streamq/streamq/internal/topic"
)

// DiskStore manages multiple segments for a single partition.
// Appends go to the active (last) segment. When it's full, a new one is created.
// Reads scan segments to find the right one by offset.
type DiskStore struct {
	dir      string
	maxBytes int64 // max size per segment before rolling

	segments []*Segment // ordered by baseOffset
	mu       sync.RWMutex
}

// NewDiskStore opens or creates a disk-based store at the given directory.
// On startup, it discovers existing segments and recovers their state.
func NewDiskStore(dir string, maxSegmentBytes int64) (*DiskStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	ds := &DiskStore{
		dir:      dir,
		maxBytes: maxSegmentBytes,
		segments: make([]*Segment, 0),
	}

	// Discover existing segments from .sqlog files
	baseOffsets, err := ds.discoverSegments()
	if err != nil {
		return nil, err
	}

	// Open each existing segment (rebuilds index from disk)
	for _, base := range baseOffsets {
		seg, err := NewSegment(dir, base, maxSegmentBytes)
		if err != nil {
			ds.Close()
			return nil, fmt.Errorf("open segment %d: %w", base, err)
		}
		ds.segments = append(ds.segments, seg)
	}

	// If no segments exist, create the first one
	if len(ds.segments) == 0 {
		seg, err := NewSegment(dir, 0, maxSegmentBytes)
		if err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
		ds.segments = append(ds.segments, seg)
	}

	return ds, nil
}

// Append writes a message to the active segment, rolling if needed.
func (ds *DiskStore) Append(msg topic.Message) (uint64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	active := ds.activeSegment()

	// Roll to new segment if the current one is full
	if active.IsFull() {
		newBase := active.NextOffset()
		seg, err := NewSegment(ds.dir, newBase, ds.maxBytes)
		if err != nil {
			return 0, fmt.Errorf("create new segment: %w", err)
		}
		ds.segments = append(ds.segments, seg)
		active = seg
	}

	// Assign offset
	msg.Offset = active.NextOffset()

	if _, err := active.Append(msg); err != nil {
		// Clean up the new segment if we just created it and append failed
		if len(ds.segments) > 1 && active == ds.segments[len(ds.segments)-1] && active.NextOffset() == active.BaseOffset() {
			active.Close()
			ds.segments = ds.segments[:len(ds.segments)-1]
		}
		return 0, err
	}

	return msg.Offset, nil
}

// Read returns messages starting from offset, up to maxMessages.
func (ds *DiskStore) Read(offset uint64, maxMessages int) ([]topic.Message, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if len(ds.segments) == 0 {
		return []topic.Message{}, nil
	}

	// Find which segment contains the starting offset
	segIdx := ds.findSegment(offset)
	if segIdx < 0 {
		return []topic.Message{}, nil
	}

	messages := make([]topic.Message, 0, maxMessages)
	currentOffset := offset

	for i := segIdx; i < len(ds.segments); i++ {
		seg := ds.segments[i]
		for {
			if maxMessages > 0 && len(messages) >= maxMessages {
				return messages, nil
			}

			if !seg.Contains(currentOffset) {
				break
			}

			msg, err := seg.Read(currentOffset)
			if err != nil {
				return messages, fmt.Errorf("read offset %d: %w", currentOffset, err)
			}

			messages = append(messages, msg)
			currentOffset++
		}
	}

	return messages, nil
}

// LatestOffset returns the next offset that would be assigned.
func (ds *DiskStore) LatestOffset() uint64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.activeSegment().NextOffset()
}

// Close flushes and closes all segments.
func (ds *DiskStore) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var firstErr error
	for _, seg := range ds.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Sync flushes all segments to disk.
func (ds *DiskStore) Sync() error {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	for _, seg := range ds.segments {
		if err := seg.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// activeSegment returns the last (newest) segment.
func (ds *DiskStore) activeSegment() *Segment {
	return ds.segments[len(ds.segments)-1]
}

// findSegment returns the index of the segment that should contain the given offset.
// Uses binary search on baseOffsets. Returns -1 if offset is past all segments.
func (ds *DiskStore) findSegment(offset uint64) int {
	idx := sort.Search(len(ds.segments), func(i int) bool {
		return ds.segments[i].BaseOffset() > offset
	})
	idx--

	if idx < 0 {
		return -1
	}

	// Verify the segment actually has data at or after this offset.
	// Without this, a gap between segments could return the wrong one.
	if offset >= ds.activeSegment().NextOffset() {
		return -1
	}

	return idx
}

// discoverSegments scans the directory for .sqlog files and returns their base offsets, sorted.
func (ds *DiskStore) discoverSegments() ([]uint64, error) {
	entries, err := os.ReadDir(ds.dir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}

	var offsets []uint64
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sqlog") {
			continue
		}
		base := strings.TrimSuffix(name, ".sqlog")
		offset, err := strconv.ParseUint(strings.TrimLeft(base, "0"), 10, 64)
		if err != nil {
			// Handle the "0000000000.sqlog" case (all zeros = offset 0)
			if strings.Trim(base, "0") == "" {
				offset = 0
			} else {
				continue
			}
		}
		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets, nil
}

// DataDir returns the directory path for a given topic and partition.
// Used by the broker to construct the right path.
func DataDir(baseDir, topicName string, partitionID int) string {
	return filepath.Join(baseDir, topicName, fmt.Sprintf("%d", partitionID))
}
