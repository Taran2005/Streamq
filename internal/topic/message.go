package topic

// Message is a single record stored in a partition.
// Once written, it's never modified or deleted (append-only).
type Message struct {
	Offset    uint64 `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	Partition int    `json:"partition"`
}
