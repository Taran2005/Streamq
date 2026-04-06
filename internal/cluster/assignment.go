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

// PartitionAssignment maps topic+partition to a leader broker and a replica set.
//
// Replicas is the full set of brokers that hold a copy of the log for this
// partition (sorted, includes the leader). ISR (in-sync replicas) is the
// subset whose offset is within the leader's lag window. Leader is whichever
// replica currently owns the per-partition etcd election lease.
//
// BrokerID stays around as a JSON alias for Leader so older readers (CLI,
// metadata clients written before replication landed) keep working.
type PartitionAssignment struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
	ISR       []string `json:"isr"`

	// BrokerID is a legacy alias for Leader. Kept for backwards compat with
	// pre-Phase-4 callers that only read this field. New code should use Leader.
	BrokerID string `json:"broker_id"`
}

// normalize ensures BrokerID mirrors Leader and replica/ISR lists are sorted.
// Call after constructing or unmarshalling.
func (a *PartitionAssignment) normalize() {
	if a.Leader == "" && a.BrokerID != "" {
		// Migrate old assignment (only BrokerID was set).
		a.Leader = a.BrokerID
	}
	a.BrokerID = a.Leader

	if len(a.Replicas) == 0 && a.Leader != "" {
		a.Replicas = []string{a.Leader}
	}
	sort.Strings(a.Replicas)

	if len(a.ISR) == 0 {
		a.ISR = append([]string(nil), a.Replicas...)
	}
	sort.Strings(a.ISR)
}

// HasReplica reports whether brokerID holds a replica of this partition.
func (a *PartitionAssignment) HasReplica(brokerID string) bool {
	for _, r := range a.Replicas {
		if r == brokerID {
			return true
		}
	}
	return false
}

// InISR reports whether brokerID is currently in the in-sync replica set.
func (a *PartitionAssignment) InISR(brokerID string) bool {
	for _, r := range a.ISR {
		if r == brokerID {
			return true
		}
	}
	return false
}

// AssignmentManager tracks which broker owns which partitions.
// The controller writes assignments to etcd. All brokers watch and cache them locally.
type AssignmentManager struct {
	client *clientv3.Client

	mu          sync.RWMutex
	assignments map[string]map[int]*PartitionAssignment // topic → partition → assignment

	// listeners are notified whenever an assignment changes.
	// Callers register via OnChange. We keep the slice tiny — typically the
	// broker registers one listener for replication wiring.
	listeners []func(PartitionAssignment)

	cancel context.CancelFunc
}

func NewAssignmentManager(client *clientv3.Client) *AssignmentManager {
	return &AssignmentManager{
		client:      client,
		assignments: make(map[string]map[int]*PartitionAssignment),
	}
}

// OnChange registers a callback fired when an assignment is added or updated
// in etcd. The callback runs on the watcher goroutine — keep it short and
// non-blocking. Safe to call before Start.
func (am *AssignmentManager) OnChange(fn func(PartitionAssignment)) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.listeners = append(am.listeners, fn)
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
// rf is the desired replication factor; it is capped at len(brokers) so we
// never write an assignment that asks for more replicas than exist. The first
// broker for each partition (by hash) becomes the leader, the next rf-1 become
// followers. ISR starts equal to the full replica set.
//
// Only the controller should call this.
func (am *AssignmentManager) AssignPartitions(ctx context.Context, topicName string, numPartitions, rf int, brokers []BrokerInfo) ([]PartitionAssignment, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers available")
	}
	if rf <= 0 {
		rf = 1
	}
	if rf > len(brokers) {
		rf = len(brokers)
	}

	// Sort brokers by ID for deterministic assignment
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	assignments := make([]PartitionAssignment, numPartitions)

	for i := 0; i < numPartitions; i++ {
		replicas := pickReplicas(topicName, i, rf, brokers)
		leader := replicas[0]
		// ISR starts equal to the full replica set; followers will fall out
		// once the lag tracker runs (Phase 4.4).
		isr := append([]string(nil), replicas...)
		sort.Strings(replicas)
		sort.Strings(isr)
		assignments[i] = PartitionAssignment{
			Topic:     topicName,
			Partition: i,
			Leader:    leader,
			Replicas:  replicas,
			ISR:       isr,
			BrokerID:  leader, // legacy alias
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
		am.assignments[topicName] = make(map[int]*PartitionAssignment)
	}
	for i := range assignments {
		a := assignments[i]
		am.assignments[topicName][a.Partition] = &a
	}
	listeners := append([]func(PartitionAssignment){}, am.listeners...)
	am.mu.Unlock()

	// Notify after dropping the lock so listeners can read the manager.
	for _, a := range assignments {
		for _, fn := range listeners {
			fn(a)
		}
	}

	log.Printf("assigned %d partitions for topic %q rf=%d across %d broker(s)", numPartitions, topicName, rf, len(brokers))
	return assignments, nil
}

// GetBrokerForPartition returns the current leader for a topic+partition.
// Kept named this way for backwards compat — the returned ID is always the
// partition leader, not an arbitrary replica.
func (am *AssignmentManager) GetBrokerForPartition(topic string, partition int) (string, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return "", false
	}
	a, ok := parts[partition]
	if !ok {
		return "", false
	}
	return a.Leader, true
}

// GetAssignment returns a copy of the full assignment for a topic+partition.
func (am *AssignmentManager) GetAssignment(topic string, partition int) (PartitionAssignment, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return PartitionAssignment{}, false
	}
	a, ok := parts[partition]
	if !ok {
		return PartitionAssignment{}, false
	}
	return cloneAssignment(*a), true
}

// GetTopicAssignments returns the leader for every partition in a topic.
// Kept returning a map[int]string for backwards compat with metadata callers.
func (am *AssignmentManager) GetTopicAssignments(topic string) map[int]string {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return nil
	}

	result := make(map[int]string, len(parts))
	for k, v := range parts {
		result[k] = v.Leader
	}
	return result
}

// GetTopicFullAssignments returns full assignments (leader + replicas + ISR)
// for every partition in a topic. Returns nil if the topic is unknown.
func (am *AssignmentManager) GetTopicFullAssignments(topic string) map[int]PartitionAssignment {
	am.mu.RLock()
	defer am.mu.RUnlock()

	parts, ok := am.assignments[topic]
	if !ok {
		return nil
	}
	result := make(map[int]PartitionAssignment, len(parts))
	for k, v := range parts {
		result[k] = cloneAssignment(*v)
	}
	return result
}

// GetLocalPartitions returns which partitions of a topic this broker owns as
// leader. (Followers don't show up here — callers that want replica-level
// participation use GetLocalReplicas.)
func (am *AssignmentManager) GetLocalPartitions(topic string, brokerID string) []int {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var local []int
	parts := am.assignments[topic]
	for partition, a := range parts {
		if a.Leader == brokerID {
			local = append(local, partition)
		}
	}
	sort.Ints(local)
	return local
}

// GetLocalReplicas returns partitions in a topic where this broker is a
// replica (leader OR follower).
func (am *AssignmentManager) GetLocalReplicas(topic string, brokerID string) []int {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var local []int
	parts := am.assignments[topic]
	for partition, a := range parts {
		if a.HasReplica(brokerID) {
			local = append(local, partition)
		}
	}
	sort.Ints(local)
	return local
}

// UpdateAssignment writes a modified assignment back to etcd. Used when a
// per-partition leader is elected or ISR membership changes. Caller must
// already have determined they're the rightful writer (i.e. they hold the
// election lease or are the controller).
func (am *AssignmentManager) UpdateAssignment(ctx context.Context, a PartitionAssignment) error {
	a.normalize()
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("marshal assignment: %w", err)
	}
	key := assignmentKey(a.Topic, a.Partition)
	if _, err := am.client.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("write assignment: %w", err)
	}
	return nil
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
		a.normalize()
		if am.assignments[a.Topic] == nil {
			am.assignments[a.Topic] = make(map[int]*PartitionAssignment)
		}
		ac := a
		am.assignments[a.Topic][a.Partition] = &ac
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
			var changed []PartitionAssignment
			am.mu.Lock()
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					var a PartitionAssignment
					if err := json.Unmarshal(ev.Kv.Value, &a); err == nil {
						a.normalize()
						if am.assignments[a.Topic] == nil {
							am.assignments[a.Topic] = make(map[int]*PartitionAssignment)
						}
						ac := a
						am.assignments[a.Topic][a.Partition] = &ac
						changed = append(changed, a)
					}
				case clientv3.EventTypeDelete:
					// Assignments aren't normally deleted; ignore.
				}
			}
			listeners := append([]func(PartitionAssignment){}, am.listeners...)
			am.mu.Unlock()

			for _, a := range changed {
				for _, fn := range listeners {
					fn(a)
				}
			}
		}
	}
}

// pickReplicas picks rf distinct broker IDs for a partition. The first one
// (deterministic per topic/partition) becomes leader; the remaining rf-1 walk
// the broker list in a stable order so the same input always picks the same
// replica set.
func pickReplicas(topicName string, partition, rf int, brokers []BrokerInfo) []string {
	// brokers is already sorted by caller.
	leader := hashPartition(topicName, partition, brokers)

	out := make([]string, 0, rf)
	out = append(out, leader)

	// Walk from leader's position so we get a different replica set per
	// partition without depending on a second hash.
	startIdx := 0
	for i, b := range brokers {
		if b.ID == leader {
			startIdx = i
			break
		}
	}

	for i := 1; i < rf; i++ {
		out = append(out, brokers[(startIdx+i)%len(brokers)].ID)
	}
	return out
}

// hashPartition picks a broker for a given topic+partition using FNV hash.
// Deterministic: same input always gives same broker.
func hashPartition(topic string, partition int, brokers []BrokerInfo) string {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s/%d", topic, partition)))
	idx := h.Sum64() % uint64(len(brokers))
	return brokers[idx].ID
}

// cloneAssignment returns a deep copy so callers can't mutate the cache.
func cloneAssignment(a PartitionAssignment) PartitionAssignment {
	cp := a
	cp.Replicas = append([]string(nil), a.Replicas...)
	cp.ISR = append([]string(nil), a.ISR...)
	return cp
}

func assignmentKey(topic string, partition int) string {
	return fmt.Sprintf("%s%s/%d", assignmentPrefix, topic, partition)
}
