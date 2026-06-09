package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
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

	// We deliberately avoid O_APPEND: Truncate on an O_APPEND handle is
	// denied on Windows, and we already track the write position
	// ourselves via s.size, so explicit WriteAt is both portable and
	// equivalent.
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
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

	if info.Size() > 0 {
		idxPath := filepath.Join(dir, segmentName(baseOffset, ".sqidx"))
		if err := seg.loadIndex(idxPath); err == nil {
			// Index loaded successfully. Determine nextOffset.
			var maxOffset uint64 = baseOffset
			hasEntries := false
			for offset := range seg.index {
				hasEntries = true
				if offset > maxOffset {
					maxOffset = offset
				}
			}
			if hasEntries {
				seg.nextOffset = maxOffset + 1
			}
		} else {
			// Fallback: file missing or corrupt
			if err := seg.rebuildIndex(); err != nil {
				logFile.Close()
				return nil, fmt.Errorf("rebuild index: %w", err)
			}
		}
	}

	return seg, nil
}

func (s *Segment) Append(msg topic.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := Encode(msg)
	position := s.size

	// WriteAt rather than Write because we opened without O_APPEND. We
	// own the write cursor here (s.size) and serialize via s.mu.
	n, err := s.logFile.WriteAt(data, position)
	if err != nil {
		return 0, fmt.Errorf("write to segment: %w", err)
	}

	// fsync on every append; batched fsync deferred (see PLAN.md).
	// Without this an "acknowledged" append can vanish on power loss.
	if err := s.logFile.Sync(); err != nil {
		return 0, fmt.Errorf("fsync segment: %w", err)
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

// rebuildIndex walks the log file and rebuilds the in-memory index.
//
// With fsync-on-append (see Append) every record that has been
// acknowledged is durable. The only legal corruption is a partial
// trailing record from a crash mid-write. When we hit something that
// won't decode we truncate the log back to the start of that record
// and continue cleanly. Anything earlier failing to decode would mean
// real on-disk corruption — we still surface that as an error.
func (s *Segment) rebuildIndex() error {
	var position int64
	fileSize := s.size

	truncate := func(reason string, err error) error {
		extra := fileSize - position
		if err := s.logFile.Truncate(position); err != nil {
			return fmt.Errorf("truncate torn tail at %d: %w", position, err)
		}
		s.size = position
		log.Printf("storage: truncated %d torn bytes from segment %d at offset %d (%s: %v)",
			extra, s.baseOffset, position, reason, err)
		return nil
	}

	for position < fileSize {
		startPos := position

		lenBuf := make([]byte, headerSize)
		n, err := s.logFile.ReadAt(lenBuf, startPos)
		if err == io.EOF || (err == nil && n < headerSize) {
			if startPos < fileSize {
				return truncate("short header", io.ErrUnexpectedEOF)
			}
			break
		}
		if err != nil && err != io.EOF {
			// io.ErrUnexpectedEOF from a partial header is a torn tail.
			if err == io.ErrUnexpectedEOF {
				return truncate("short header", err)
			}
			return fmt.Errorf("read at position %d: %w", startPos, err)
		}

		payloadSize := int(binary.BigEndian.Uint32(lenBuf))
		totalSize := headerSize + payloadSize

		// Bogus length pointing past EOF is a torn tail.
		if totalSize <= headerSize || startPos+int64(totalSize) > fileSize {
			return truncate("declared length past EOF", io.ErrUnexpectedEOF)
		}

		data := make([]byte, totalSize)
		if _, err := s.logFile.ReadAt(data, startPos); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return truncate("short record body", err)
			}
			return fmt.Errorf("read message at position %d: %w", startPos, err)
		}

		msg, err := Decode(data)
		if err != nil {
			// Only treat the very last record as a torn tail. Mid-file
			// decode failures with full bytes available are real corruption.
			if startPos+int64(totalSize) == fileSize {
				return truncate("decode failed on tail record", err)
			}
			return fmt.Errorf("decode at position %d: %w", startPos, err)
		}

		s.index[msg.Offset] = startPos
		s.nextOffset = msg.Offset + 1
		position = startPos + int64(totalSize)
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
