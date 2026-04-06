package broker

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/streamq/streamq/internal/cluster"
	"github.com/streamq/streamq/internal/common"
	"github.com/streamq/streamq/internal/storage"
	"github.com/streamq/streamq/internal/topic"
)

type Broker struct {
	Config  Config
	topics  map[string]*topic.Topic
	mu      sync.RWMutex

	// Cluster mode (nil in standalone)
	cluster     *cluster.Cluster
	assignments *cluster.AssignmentManager
	forwarder   *cluster.Forwarder
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

// JoinCluster connects to etcd and joins the broker cluster.
// Must be called after New() and before Start().
func (b *Broker) JoinCluster() error {
	if !b.Config.IsClusterMode() {
		return nil
	}

	self := cluster.BrokerInfo{
		ID:   b.Config.BrokerID,
		Addr: b.Config.AdvertiseAddr,
	}

	c, err := cluster.NewCluster(b.Config.EtcdEndpoints, self)
	if err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}
	b.cluster = c

	b.assignments = cluster.NewAssignmentManager(c.Client())
	// Use a background context for the watcher — it needs to run for the broker's lifetime.
	// Only the initial load has a timeout.
	if err := b.assignments.Start(context.Background()); err != nil {
		c.Close()
		return fmt.Errorf("start assignment manager: %w", err)
	}

	b.forwarder = cluster.NewForwarder(c)

	log.Printf("joined cluster as broker %s", b.Config.BrokerID)
	return nil
}

// ErrNotController is returned when a non-controller broker receives a request
// that only the controller can handle. ControllerID contains the current leader
// so the caller can forward the request.
type ErrNotController struct {
	ControllerID string
}

func (e *ErrNotController) Error() string {
	if e.ControllerID != "" {
		return fmt.Sprintf("not the controller — forward to %s", e.ControllerID)
	}
	return "not the controller — no controller elected"
}

// CreateTopic creates a new topic. In cluster mode, the controller assigns
// partitions to brokers. Each broker only creates the partitions it owns locally.
func (b *Broker) CreateTopic(ctx context.Context, name string, numPartitions int) (*topic.Topic, error) {
	if err := topic.ValidateTopicParams(name, numPartitions); err != nil {
		return nil, err
	}

	// Standalone mode: create all partitions locally (same as before)
	if !b.Config.IsClusterMode() {
		return b.createLocalTopic(name, numPartitions)
	}

	// Cluster mode: only the controller can assign partitions
	if !b.cluster.IsController() {
		return nil, &ErrNotController{
			ControllerID: b.cluster.ControllerID(ctx),
		}
	}

	if b.assignments.TopicExists(name) {
		return nil, &common.AlreadyExistsError{Resource: "topic", Name: name}
	}

	// Assign partitions across brokers
	brokers := b.cluster.GetBrokers()
	rf := b.Config.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	assignments, err := b.assignments.AssignPartitions(ctx, name, numPartitions, rf, brokers)
	if err != nil {
		return nil, fmt.Errorf("assign partitions: %w", err)
	}

	// Create only the partitions assigned to us locally
	return b.createPartitionsFromAssignment(name, numPartitions, assignments)
}

// CreateTopicFromAssignment creates local partitions based on existing assignments.
// Used by non-controller brokers when they learn about a new topic from etcd.
//
// With replication, ANY broker that holds a replica (leader or follower) gets
// a real local store. Only brokers that hold no replica at all get a remote
// placeholder.
func (b *Broker) CreateTopicFromAssignment(name string, numPartitions int) (*topic.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return b.topics[name], nil
	}

	assignments := b.assignments.GetTopicFullAssignments(name)
	if assignments == nil {
		return nil, fmt.Errorf("no assignments found for topic %s", name)
	}

	partitions := make([]*topic.Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		a, ok := assignments[i]
		if ok && a.HasReplica(b.Config.BrokerID) {
			store, err := b.createStore(name, i)
			if err != nil {
				return nil, fmt.Errorf("create store for partition %d: %w", i, err)
			}
			partitions[i] = topic.NewPartition(i, store)
		} else {
			// We don't hold a replica of this partition — placeholder.
			partitions[i] = topic.NewPartition(i, nil)
		}
	}

	t := topic.NewTopic(name, partitions)
	b.topics[name] = t
	return t, nil
}

// createLocalTopic creates a topic with all partitions on this broker (standalone mode).
func (b *Broker) createLocalTopic(name string, numPartitions int) (*topic.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil, &common.AlreadyExistsError{Resource: "topic", Name: name}
	}

	partitions := make([]*topic.Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		store, err := b.createStore(name, i)
		if err != nil {
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

// createPartitionsFromAssignment creates the topic. Any partition for which
// we are a replica (leader OR follower) gets a real local store; the rest get
// remote placeholders.
func (b *Broker) createPartitionsFromAssignment(name string, numPartitions int, assignments []cluster.PartitionAssignment) (*topic.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	partitions := make([]*topic.Partition, numPartitions)
	for _, a := range assignments {
		if a.HasReplica(b.Config.BrokerID) {
			store, err := b.createStore(name, a.Partition)
			if err != nil {
				// Clean up already-created partitions
				for _, p := range partitions {
					if p != nil {
						if cerr := p.Close(); cerr != nil {
							log.Printf("cleanup partition: %v", cerr)
						}
					}
				}
				return nil, fmt.Errorf("create store for partition %d: %w", a.Partition, err)
			}
			partitions[a.Partition] = topic.NewPartition(a.Partition, store)
		} else {
			// Placeholder for remote partitions
			partitions[a.Partition] = topic.NewPartition(a.Partition, nil)
		}
	}

	t := topic.NewTopic(name, partitions)
	b.topics[name] = t
	return t, nil
}

// IsPartitionLocal returns true if this broker owns the given partition.
func (b *Broker) IsPartitionLocal(topicName string, partition int) bool {
	if !b.Config.IsClusterMode() {
		return true // standalone = everything is local
	}
	ownerID, ok := b.assignments.GetBrokerForPartition(topicName, partition)
	if !ok {
		return false
	}
	return ownerID == b.Config.BrokerID
}

// GetPartitionOwner returns the broker ID that owns a partition.
func (b *Broker) GetPartitionOwner(topicName string, partition int) (string, bool) {
	if !b.Config.IsClusterMode() {
		return b.Config.BrokerID, true
	}
	return b.assignments.GetBrokerForPartition(topicName, partition)
}

func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	b.mu.RLock()
	t, exists := b.topics[name]
	b.mu.RUnlock()

	if exists {
		return t, nil
	}

	// In cluster mode, the topic may exist in etcd but not yet materialized locally.
	// Lazy-create from assignments if available.
	if b.Config.IsClusterMode() && b.assignments.TopicExists(name) {
		numPartitions := b.assignments.TopicPartitionCount(name)
		return b.CreateTopicFromAssignment(name, numPartitions)
	}

	return nil, &common.NotFoundError{Resource: "topic", Name: name}
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

// Close shuts down all topics, stores, and cluster connection.
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

	if b.cluster != nil {
		if b.assignments != nil {
			b.assignments.Stop()
		}
		if err := b.cluster.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// recoverFromDisk scans the data directory and re-registers topics/partitions.
func (b *Broker) recoverFromDisk() error {
	entries, err := os.ReadDir(b.Config.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()

		topicPath := b.Config.DataDir + "/" + topicName
		partEntries, err := os.ReadDir(topicPath)
		if err != nil {
			return fmt.Errorf("read topic dir %s: %w", topicName, err)
		}

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
func (b *Broker) createStore(topicName string, partitionID int) (topic.Store, error) {
	if b.Config.DataDir == "" {
		return storage.NewMemoryStore(), nil
	}

	dir := storage.DataDir(b.Config.DataDir, topicName, partitionID)
	return storage.NewDiskStore(dir, b.Config.MaxSegmentBytes)
}
