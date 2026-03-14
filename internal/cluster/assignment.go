package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const assignmentPrefix = "/streamq/assignments/"

// PartitionAssignment maps topic+partition to a broker ID.
type PartitionAssignment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	BrokerID  string `json:"broker_id"`
}

// AssignmentManager tracks which broker owns which partitions.
// The controller writes assignments to etcd. All brokers watch and cache them locally.
type AssignmentManager struct {
	client *clientv3.Client

	mu          sync.RWMutex
	assignments map[string]map[int]string // topic → partition → broker ID

	cancel context.CancelFunc
}

func NewAssignmentManager(client *clientv3.Client) *AssignmentManager {
	return &AssignmentManager{
		client:      client,
		assignments: make(map[string]map[int]string),
	}
}

// Start loads existing assignments and watches for changes.
func (am *AssignmentManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	am.cancel = cancel

	if err := am.loadAll(ctx); err != nil {
		cancel()
		return err
	}

	go am.watch(ctx)
	return nil
}

// Stop cancels the watcher.
func (am *AssignmentManager) Stop() {
	if am.cancel != nil {
		am.cancel()
	}
}

// AssignPartitions creates assignments for a new topic using consistent hashing.
// Only the controller should call this.
func (am *AssignmentManager) AssignPartitions(ctx context.Context, topicName string, numPartitions int, brokers []BrokerInfo) ([]PartitionAssignment, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers available")
	}

	// Sort brokers by ID for deterministic assignment
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	assignments := make([]PartitionAssignment, numPartitions)

	for i := 0; i < numPartitions; i++ {
		brokerID := hashPartition(topicName, i, brokers)
		assignments[i] = PartitionAssignment{
			Topic:     topicName,
			Partition: i,
			BrokerID:  brokerID,
		}
	}

	// Build all puts as an atomic etcd transaction — either all partitions
	// are written or none are, preventing half-created topics.
	// The If-condition checks that partition 0 doesn't exist yet (TOCTOU guard).
	firstKey := assignmentKey(topicName, 0)

	ops := make([]clientv3.Op, len(assignments))
	for i, a := range assignments {
		key := assignmentKey(a.Topic, a.Partition)
		data, err := json.Marshal(a)
		if err != nil {
			return nil, fmt.Errorf("marshal assignment %s/%d: %w", a.Topic, a.Partition, err)
		}
		ops[i] = clientv3.OpPut(key, string(data))
	}

	txnResp, err := am.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(firstKey), "=", 0)).
		Then(ops...).
		Commit()
	if err != nil {
		return nil, fmt.Errorf("write assignments for %s: %w", topicName, err)
	}
	if !txnResp.Succeeded {
		return nil, fmt.Errorf("topic %s already exists (concurrent create)", topicName)
	}

	// Update local cache
	am.mu.Lock()
	if am.assignments[topicName] == nil {
		am.assignments[topicName] = make(map[int]string)
	}
	for _, a := range assignments {
		am.assignments[topicName][a.Partition] = a.BrokerID
	}
	am.mu.Unlock()

	log.Printf("assigned %d partitions for topic %q across %d broker(s)", numPartitions, topicName, len(brokers))
	return assignments, nil
}

// GetBrokerForPartition returns which broker owns a given topic+partition.
func (am *AssignmentManager) GetBrokerForPartition(topic string, partition int) (string, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return "", false
	}
	brokerID, ok := parts[partition]
	return brokerID, ok
}

// GetTopicAssignments returns all partition assignments for a topic.
func (am *AssignmentManager) GetTopicAssignments(topic string) map[int]string {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return nil
	}

	// Return a copy
	result := make(map[int]string, len(parts))
	for k, v := range parts {
		result[k] = v
	}
	return result
}

// GetLocalPartitions returns which partitions of a topic this broker owns.
func (am *AssignmentManager) GetLocalPartitions(topic string, brokerID string) []int {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var local []int
	parts := am.assignments[topic]
	for partition, owner := range parts {
		if owner == brokerID {
			local = append(local, partition)
		}
	}
	sort.Ints(local)
	return local
}

// TopicExists checks if any assignments exist for a topic.
func (am *AssignmentManager) TopicExists(topic string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	_, ok := am.assignments[topic]
	return ok
}

// TopicPartitionCount returns the number of partitions for a topic.
func (am *AssignmentManager) TopicPartitionCount(topic string) int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.assignments[topic])
}

// loadAll fetches all assignments from etcd into local cache.
func (am *AssignmentManager) loadAll(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := am.client.Get(ctx, assignmentPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("load assignments: %w", err)
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	for _, kv := range resp.Kvs {
		var a PartitionAssignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			continue
		}
		if am.assignments[a.Topic] == nil {
			am.assignments[a.Topic] = make(map[int]string)
		}
		am.assignments[a.Topic][a.Partition] = a.BrokerID
	}

	return nil
}

// watch monitors etcd for assignment changes and updates local cache.
func (am *AssignmentManager) watch(ctx context.Context) {
	wch := am.client.Watch(ctx, assignmentPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok {
				return
			}
			am.mu.Lock()
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					var a PartitionAssignment
					if err := json.Unmarshal(ev.Kv.Value, &a); err == nil {
						if am.assignments[a.Topic] == nil {
							am.assignments[a.Topic] = make(map[int]string)
						}
						am.assignments[a.Topic][a.Partition] = a.BrokerID
					}
				case clientv3.EventTypeDelete:
					// We don't delete assignments in normal operation,
					// but handle it for completeness
				}
			}
			am.mu.Unlock()
		}
	}
}

// hashPartition picks a broker for a given topic+partition using FNV hash.
// Deterministic: same input always gives same broker.
func hashPartition(topic string, partition int, brokers []BrokerInfo) string {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s/%d", topic, partition)))
	idx := h.Sum64() % uint64(len(brokers))
	return brokers[idx].ID
}

func assignmentKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d", assignmentPrefix, topic, partition)
}
