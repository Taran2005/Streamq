package topic

import (
	"fmt"
	"regexp"
	"sync/atomic"

	"github.com/streamq/streamq/internal/common"
)

// validTopicName enforces: alphanumeric + hyphens, 1-255 chars.
// This prevents garbage topic names from causing file path issues later (Phase 2).
var validTopicName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,254}$`)

// Topic is a named collection of partitions.
// Producers write to it, consumers read from it.
//
// The roundRobin counter distributes messages evenly across partitions
// when the producer doesn't specify a key.
type Topic struct {
	Name       string
	Partitions []*Partition
	roundRobin uint64 // atomic counter for round-robin partition selection
}

// NewTopic creates a topic with the given number of partitions.
// Returns an error if the name is invalid or partition count is out of range.
func NewTopic(name string, numPartitions int) (*Topic, error) {
	if !validTopicName.MatchString(name) {
		return nil, &common.ValidationError{Message: "invalid topic name: must be alphanumeric+hyphens, 1-255 chars"}
	}
	if numPartitions <= 0 || numPartitions > 1024 {
		return nil, &common.ValidationError{Message: "partition count must be between 1 and 1024"}
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = NewPartition(i)
	}

	return &Topic{
		Name:       name,
		Partitions: partitions,
	}, nil
}

// GetPartition returns the partition at the given index.
// Returns an error if the index is out of range.
func (t *Topic) GetPartition(id int) (*Partition, error) {
	if id < 0 || id >= len(t.Partitions) {
		return nil, &common.NotFoundError{Resource: "partition", Name: fmt.Sprintf("%d", id)}
	}
	return t.Partitions[id], nil
}

// NextPartition picks the next partition using round-robin.
// This is used when a producer doesn't specify a partition key.
//
// atomic.AddUint64 is like a thread-safe counter — multiple goroutines
// can increment it simultaneously without locks. Way faster than a mutex
// for a simple counter.
func (t *Topic) NextPartition() *Partition {
	n := atomic.AddUint64(&t.roundRobin, 1)
	idx := int(n-1) % len(t.Partitions)
	return t.Partitions[idx]
}

// Info returns a summary of the topic for API responses.
type TopicInfo struct {
	Name           string `json:"name"`
	PartitionCount int    `json:"partition_count"`
}

func (t *Topic) Info() TopicInfo {
	return TopicInfo{
		Name:           t.Name,
		PartitionCount: len(t.Partitions),
	}
}
