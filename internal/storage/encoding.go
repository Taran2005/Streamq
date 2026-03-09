package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/streamq/streamq/internal/topic"
)

// On-disk format per message:
//   [4 bytes: message length (everything after this field)]
//   [8 bytes: offset]
//   [8 bytes: timestamp]
//   [4 bytes: key length]
//   [N bytes: key]
//   [4 bytes: value length]
//   [N bytes: value]
//   [4 bytes: CRC32 checksum (covers offset through value)]
//
// Total overhead per message: 32 bytes + key + value

const (
	lengthSize    = 4
	offsetSize    = 8
	timestampSize = 8
	keyLenSize    = 4
	valueLenSize  = 4
	crcSize       = 4
	headerSize    = lengthSize // the leading length prefix
)

// Encode serializes a message into its binary on-disk format.
func Encode(msg topic.Message) []byte {
	keyBytes := []byte(msg.Key)
	valueBytes := []byte(msg.Value)

	// Total size of the payload (everything after the 4-byte length prefix)
	payloadSize := offsetSize + timestampSize +
		keyLenSize + len(keyBytes) +
		valueLenSize + len(valueBytes) +
		crcSize

	buf := make([]byte, headerSize+payloadSize)
	pos := 0

	// Message length (excludes this field itself)
	binary.BigEndian.PutUint32(buf[pos:], uint32(payloadSize))
	pos += lengthSize

	// Offset
	binary.BigEndian.PutUint64(buf[pos:], msg.Offset)
	pos += offsetSize

	// Timestamp
	binary.BigEndian.PutUint64(buf[pos:], uint64(msg.Timestamp))
	pos += timestampSize

	// Key
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(keyBytes)))
	pos += keyLenSize
	copy(buf[pos:], keyBytes)
	pos += len(keyBytes)

	// Value
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(valueBytes)))
	pos += valueLenSize
	copy(buf[pos:], valueBytes)
	pos += len(valueBytes)

	// CRC32 over everything between length prefix and CRC field
	checksum := crc32.ChecksumIEEE(buf[headerSize : pos])
	binary.BigEndian.PutUint32(buf[pos:], checksum)

	return buf
}

// Decode deserializes a message from its binary on-disk format.
// The input should be a complete message including the length prefix.
func Decode(data []byte) (topic.Message, error) {
	if len(data) < headerSize+offsetSize+timestampSize+keyLenSize+valueLenSize+crcSize {
		return topic.Message{}, fmt.Errorf("message too short: %d bytes", len(data))
	}

	pos := 0

	// Read and validate length
	payloadSize := int(binary.BigEndian.Uint32(data[pos:]))
	pos += lengthSize

	if len(data) < headerSize+payloadSize {
		return topic.Message{}, fmt.Errorf("incomplete message: expected %d bytes, got %d", headerSize+payloadSize, len(data))
	}

	// Verify CRC before trusting any data
	crcStart := headerSize
	crcEnd := headerSize + payloadSize - crcSize
	storedCRC := binary.BigEndian.Uint32(data[crcEnd:crcEnd+crcSize])
	computedCRC := crc32.ChecksumIEEE(data[crcStart:crcEnd])
	if storedCRC != computedCRC {
		return topic.Message{}, fmt.Errorf("CRC mismatch: stored=%d computed=%d (data corrupted)", storedCRC, computedCRC)
	}

	// Offset
	offset := binary.BigEndian.Uint64(data[pos:])
	pos += offsetSize

	// Timestamp
	timestamp := int64(binary.BigEndian.Uint64(data[pos:]))
	pos += timestampSize

	// Key
	keyLen := int(binary.BigEndian.Uint32(data[pos:]))
	pos += keyLenSize
	if pos+keyLen > len(data) {
		return topic.Message{}, fmt.Errorf("key length %d exceeds message bounds", keyLen)
	}
	key := string(data[pos : pos+keyLen])
	pos += keyLen

	// Value
	valueLen := int(binary.BigEndian.Uint32(data[pos:]))
	pos += valueLenSize
	if pos+valueLen > len(data) {
		return topic.Message{}, fmt.Errorf("value length %d exceeds message bounds", valueLen)
	}
	value := string(data[pos : pos+valueLen])

	return topic.Message{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}, nil
}

// RecordSize returns the total on-disk size for a message with the given key and value lengths.
func RecordSize(keyLen, valueLen int) int {
	return headerSize + offsetSize + timestampSize + keyLenSize + keyLen + valueLenSize + valueLen + crcSize
}
