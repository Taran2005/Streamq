package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/streamq/streamq/internal/topic"
)

// Segment is a single log file + index for a range of offsets.
//
// When a segment reaches maxBytes, the DiskStore creates a new one.
// Old segments are read-only. Only the "active" segment gets appends.
//
// File naming: base offset zero-padded to 10 digits.
//   00000000000.sqlog    (messages)
//   00000000000.sqidx  (offset → byte position, CRC-protected)
type Segment struct {
	baseOffset uint64
	nextOffset uint64
	size       int64
	maxBytes   int64

	logFile *os.File
	index   map[uint64]int64
	mu      sync.RWMutex
}

func NewSegment(dir string, baseOffset uint64, maxBytes int64) (*Segment, error) {
	logPath := filepath.Join(dir, segmentName(baseOffset, ".sqlog"))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	info, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}

	seg := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       info.Size(),
		maxBytes:   maxBytes,
		logFile:    logFile,
		index:      make(map[uint64]int64),
	}

	// Always rebuild index from log (source of truth).
	// The .sqidx file is just a fast-path cache — if it's corrupt we don't care.
	if info.Size() > 0 {
		if err := seg.rebuildIndex(); err != nil {
			logFile.Close()
			return nil, fmt.Errorf("rebuild index: %w", err)
		}
	}

	return seg, nil
}

func (s *Segment) Append(msg topic.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := Encode(msg)
	position := s.size

	n, err := s.logFile.Write(data)
	if err != nil {
		return 0, fmt.Errorf("write to segment: %w", err)
	}

	s.index[msg.Offset] = position
	s.size += int64(n)
	s.nextOffset = msg.Offset + 1

	return position, nil
}

// Read returns the message at the given offset.
// RLock is held for the entire read sequence (index lookup + file reads)
// so the file can't be closed by another goroutine mid-read.
func (s *Segment) Read(offset uint64) (topic.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	position, ok := s.index[offset]
	if !ok {
		return topic.Message{}, fmt.Errorf("offset %d not in segment (base=%d)", offset, s.baseOffset)
	}

	lenBuf := make([]byte, headerSize)
	if _, err := s.logFile.ReadAt(lenBuf, position); err != nil {
		return topic.Message{}, fmt.Errorf("read length at position %d: %w", position, err)
	}

	payloadSize := int(binary.BigEndian.Uint32(lenBuf))
	totalSize := headerSize + payloadSize
	data := make([]byte, totalSize)
	if _, err := s.logFile.ReadAt(data, position); err != nil {
		return topic.Message{}, fmt.Errorf("read message at position %d: %w", position, err)
	}

	return Decode(data)
}

func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxBytes
}

func (s *Segment) NextOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

func (s *Segment) Contains(offset uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.index[offset]
	return ok
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.logFile.Name())
	idxPath := filepath.Join(dir, segmentName(s.baseOffset, ".sqidx"))
	if err := s.saveIndex(idxPath); err != nil {
		s.logFile.Close()
		return err
	}

	if err := s.logFile.Sync(); err != nil {
		s.logFile.Close()
		return err
	}
	return s.logFile.Close()
}

func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logFile.Sync()
}

func (s *Segment) rebuildIndex() error {
	var position int64

	for {
		lenBuf := make([]byte, headerSize)
		_, err := s.logFile.ReadAt(lenBuf, position)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read at position %d: %w", position, err)
		}

		payloadSize := int(binary.BigEndian.Uint32(lenBuf))
		totalSize := headerSize + payloadSize

		data := make([]byte, totalSize)
		_, err = s.logFile.ReadAt(data, position)
		if err != nil {
			return fmt.Errorf("read message at position %d: %w", position, err)
		}

		msg, err := Decode(data)
		if err != nil {
			return fmt.Errorf("decode at position %d: %w", position, err)
		}

		s.index[msg.Offset] = position
		s.nextOffset = msg.Offset + 1
		position += int64(totalSize)
	}

	return nil
}

// saveIndex writes the index to a temp file, fsyncs, then atomically renames.
// Index format: [sorted entries: 8 bytes offset + 8 bytes position] + [4 bytes CRC32]
// Atomic rename prevents partial/corrupt index files on crash.
func (s *Segment) saveIndex(path string) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// Sort offsets for deterministic output and easier debugging
	offsets := make([]uint64, 0, len(s.index))
	for offset := range s.index {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	// Write all entries and compute CRC over the entire content
	hasher := crc32.NewIEEE()
	buf := make([]byte, 16)
	for _, offset := range offsets {
		position := s.index[offset]
		binary.BigEndian.PutUint64(buf[0:8], offset)
		binary.BigEndian.PutUint64(buf[8:16], uint64(position))
		if _, err := f.Write(buf); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		hasher.Write(buf)
	}

	// Write CRC32 checksum at the end
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hasher.Sum32())
	if _, err := f.Write(crcBuf[:]); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	// Fsync before rename to ensure data is on disk
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	// Atomic rename — either the old file or the new file exists, never partial
	return os.Rename(tmpPath, path)
}

// loadIndex reads a previously saved index file with CRC verification.
// If the file is missing or corrupt, returns error (caller falls back to rebuildIndex).
func (s *Segment) loadIndex(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Need at least 4 bytes for CRC
	if len(data) < 4 {
		return fmt.Errorf("index file too short")
	}

	// Verify CRC
	content := data[:len(data)-4]
	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	computedCRC := crc32.ChecksumIEEE(content)
	if storedCRC != computedCRC {
		return fmt.Errorf("index CRC mismatch (file corrupt)")
	}

	// Parse entries
	for i := 0; i+16 <= len(content); i += 16 {
		offset := binary.BigEndian.Uint64(content[i : i+8])
		position := int64(binary.BigEndian.Uint64(content[i+8 : i+16]))
		s.index[offset] = position
	}
	return nil
}

func segmentName(baseOffset uint64, ext string) string {
	return fmt.Sprintf("%010d%s", baseOffset, ext)
}
