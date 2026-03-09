package broker

type Config struct {
	Port             int
	MaxMessageSize   int
	DataDir          string // directory for persistent storage ("" = in-memory only)
	MaxSegmentBytes  int64  // max size per segment file before rolling

	// Durability: data is fsync'd on broker shutdown (Close).
	// Unsynced data (in OS page cache) can be lost on power failure.
	// Phase 2 tradeoff: we accept this for write throughput.
	// Phase 7 adds configurable fsync batching (every N ms or N messages).
}

func DefaultConfig() Config {
	return Config{
		Port:            8080,
		MaxMessageSize:  1_048_576,  // 1MB
		DataDir:         "",         // empty = in-memory mode
		MaxSegmentBytes: 10_485_760, // 10MB per segment
	}
}
