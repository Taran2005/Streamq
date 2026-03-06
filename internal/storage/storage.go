package storage

import "github.com/streamq/streamq/internal/topic"

// Store is the interface that any storage backend must implement.
// Memory store for tests, disk store for production.
// This is like defining a TypeScript interface — anything that
// has these methods counts as a Store.
type Store interface {
	// Append writes a message and returns its assigned offset.
	Append(msg topic.Message) (uint64, error)

	// Read returns messages starting from offset, up to maxMessages.
	// Returns empty slice if offset is past the end.
	Read(offset uint64, maxMessages int) ([]topic.Message, error)

	// LatestOffset returns the next offset that would be assigned.
	LatestOffset() uint64

	// Close flushes and closes the store.
	Close() error
}
