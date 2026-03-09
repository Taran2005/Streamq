package broker

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/storage"
	"github.com/streamq/streamq/internal/topic"
)

type Broker struct {
	Config Config
	topics map[string]*topic.Topic
	mu     sync.RWMutex
}

func New(cfg Config) (*Broker, error) {
	b := &Broker{
		Config: cfg,
		topics: make(map[string]*topic.Topic),
	}

	if cfg.DataDir != "" {
		if err := b.recoverFromDisk(); err != nil {
			return nil, fmt.Errorf("recovery failed: %w", err)
		}
	}

	return b, nil
}

func (b *Broker) CreateTopic(name string, numPartitions int) (*topic.Topic, error) {
	if err := topic.ValidateTopicParams(name, numPartitions); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil, &common.AlreadyExistsError{Resource: "topic", Name: name}
	}

	partitions := make([]*topic.Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		store, err := b.createStore(name, i)
		if err != nil {
			// Clean up any stores we already created
			for j := 0; j < i; j++ {
				if cerr := partitions[j].Close(); cerr != nil {
					log.Printf("cleanup partition %d: %v", j, cerr)
				}
			}
			return nil, fmt.Errorf("create store for partition %d: %w", i, err)
		}
		partitions[i] = topic.NewPartition(i, store)
	}

	t := topic.NewTopic(name, partitions)
	b.topics[name] = t
	return t, nil
}

func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	if !exists {
		return nil, &common.NotFoundError{Resource: "topic", Name: name}
	}
	return t, nil
}

func (b *Broker) ListTopics() []topic.TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	infos := make([]topic.TopicInfo, 0, len(b.topics))
	for _, t := range b.topics {
		infos = append(infos, t.Info())
	}
	return infos
}

// Close shuts down all topics and their stores.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// recoverFromDisk scans the data directory and re-registers topics/partitions.
// Directory layout: {DataDir}/{topicName}/{partitionID}/
func (b *Broker) recoverFromDisk() error {
	entries, err := os.ReadDir(b.Config.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no data dir yet, nothing to recover
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()

		// Discover partition directories (named "0", "1", "2", ...)
		topicPath := b.Config.DataDir + "/" + topicName
		partEntries, err := os.ReadDir(topicPath)
		if err != nil {
			return fmt.Errorf("read topic dir %s: %w", topicName, err)
		}

		// Collect partition IDs
		var partIDs []int
		for _, pe := range partEntries {
			if !pe.IsDir() {
				continue
			}
			id, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue
			}
			partIDs = append(partIDs, id)
		}

		if len(partIDs) == 0 {
			continue
		}

		sort.Ints(partIDs)
		numPartitions := partIDs[len(partIDs)-1] + 1

		partitions := make([]*topic.Partition, numPartitions)
		for _, id := range partIDs {
			dir := storage.DataDir(b.Config.DataDir, topicName, id)
			ds, err := storage.NewDiskStore(dir, b.Config.MaxSegmentBytes)
			if err != nil {
				return fmt.Errorf("recover partition %s/%d: %w", topicName, id, err)
			}
			partitions[id] = topic.NewPartition(id, ds)
		}

		// Fill any gaps with empty disk stores
		for i := 0; i < numPartitions; i++ {
			if partitions[i] == nil {
				dir := storage.DataDir(b.Config.DataDir, topicName, i)
				ds, err := storage.NewDiskStore(dir, b.Config.MaxSegmentBytes)
				if err != nil {
					return fmt.Errorf("create missing partition %s/%d: %w", topicName, i, err)
				}
				partitions[i] = topic.NewPartition(i, ds)
			}
		}

		t := topic.NewTopic(topicName, partitions)
		b.topics[topicName] = t
		log.Printf("recovered topic %q with %d partitions", topicName, numPartitions)
	}

	return nil
}

// createStore builds the right store type based on config.
// Empty DataDir = memory, otherwise disk.
func (b *Broker) createStore(topicName string, partitionID int) (topic.Store, error) {
	if b.Config.DataDir == "" {
		return storage.NewMemoryStore(), nil
	}

	dir := storage.DataDir(b.Config.DataDir, topicName, partitionID)
	return storage.NewDiskStore(dir, b.Config.MaxSegmentBytes)
}
