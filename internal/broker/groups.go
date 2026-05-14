package broker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/streamq/streamq/internal/consumergroup"
)

// brokerTopicResolver adapts *Broker to consumergroup.TopicResolver. The
// coordinator uses it to learn how many partitions a subscribed topic has so
// it can compute assignments.
type brokerTopicResolver struct {
	broker *Broker
}

func (r *brokerTopicResolver) PartitionCount(topicName string) (int, bool) {
	t, err := r.broker.GetTopic(topicName)
	if err == nil {
		return len(t.Partitions), true
	}
	// Cluster-mode topics may not be materialized locally yet — fall back to
	// the assignment manager which knows about every topic in etcd.
	if r.broker.Config.IsClusterMode() && r.broker.assignments != nil {
		if count := r.broker.assignments.TopicPartitionCount(topicName); count > 0 {
			return count, true
		}
	}
	return 0, false
}

// brokerOffsetWriter adapts *Broker to consumergroup.OffsetWriter. It writes
// each commit as a regular message to __consumer_offsets, routed by fnv-hash
// of the commit key. That keeps a single group's offsets co-located so replay
// is local and cheap.
type brokerOffsetWriter struct {
	broker *Broker
}

func (w *brokerOffsetWriter) WriteCommit(ctx context.Context, key string, value []byte) (uint64, error) {
	b := w.broker
	t, err := b.GetTopic(consumergroup.InternalTopic)
	if err != nil {
		return 0, fmt.Errorf("offsets topic not initialized: %w", err)
	}
	partID := consumergroup.PartitionForKey(key, len(t.Partitions))
	p, err := t.GetPartition(partID)
	if err != nil {
		return 0, fmt.Errorf("get offset partition %d: %w", partID, err)
	}
	if !p.IsLocal() {
		// Not our partition — caller (coordinator forwarding) should have
		// routed this to the right broker before getting here.
		return 0, fmt.Errorf("offset partition %d is remote", partID)
	}
	_, offset, err := p.AppendAndReturn(key, string(value))
	if err != nil {
		return 0, fmt.Errorf("append commit: %w", err)
	}
	// Standalone mode: advance HW immediately so reads can see it on replay.
	if !b.Config.IsClusterMode() {
		p.SetHighWatermark(offset + 1)
	}
	return offset, nil
}

// groupSubsystem bundles everything the broker needs to operate consumer
// groups. Embedded in Broker.
type groupSubsystem struct {
	store       *consumergroup.Store
	coordinator *consumergroup.Coordinator

	once sync.Once // ensures we only bootstrap the internal topic once
}

// InitGroupSubsystem builds the coordinator and store, replays the internal
// topic into the store on startup, and exposes coordinator methods to
// handlers.
//
// Called from Broker.New if cluster mode is off, and after JoinCluster otherwise.
func (b *Broker) initGroupSubsystem() {
	if b.groups != nil {
		return
	}
	gs := &groupSubsystem{
		store: consumergroup.NewStore(),
	}
	gs.coordinator = consumergroup.NewCoordinator(
		&brokerTopicResolver{broker: b},
		&brokerOffsetWriter{broker: b},
		gs.store,
	)
	b.groups = gs
}

// ensureOffsetsTopic creates the __consumer_offsets topic on first need.
// Standalone mode: creates locally with cfg.OffsetTopicPartitions partitions.
// Cluster mode: the controller creates it; non-controller brokers lazy-create
// from etcd assignments (handled by GetTopic).
func (b *Broker) ensureOffsetsTopic(ctx context.Context) error {
	var firstErr error
	b.groups.once.Do(func() {
		if _, err := b.GetTopic(consumergroup.InternalTopic); err == nil {
			return // already exists (recovered from disk or previously created)
		}

		numParts := b.Config.OffsetTopicPartitions
		if numParts <= 0 {
			numParts = 8
		}

		// Standalone: create directly.
		if !b.Config.IsClusterMode() {
			if _, err := b.createLocalTopic(consumergroup.InternalTopic, numParts); err != nil {
				firstErr = fmt.Errorf("bootstrap offsets topic: %w", err)
			}
			return
		}

		// Cluster: only the controller can create. If we're not the controller,
		// hope someone else creates it (the first call to GetTopic will lazy
		// materialize from etcd once assignments show up).
		if !b.cluster.IsController() {
			return
		}
		brokers := b.cluster.GetBrokers()
		rf := b.Config.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		if rf > 3 {
			rf = 3 // cap at 3 even if user asked for more
		}
		if rf > len(brokers) {
			rf = len(brokers)
		}
		assignments, err := b.assignments.AssignPartitions(ctx, consumergroup.InternalTopic, numParts, rf, brokers)
		if err != nil {
			firstErr = fmt.Errorf("assign offsets partitions: %w", err)
			return
		}
		if _, err := b.createPartitionsFromAssignment(consumergroup.InternalTopic, numParts, assignments); err != nil {
			firstErr = fmt.Errorf("create offsets partitions: %w", err)
		}
	})
	if firstErr != nil {
		return firstErr
	}
	// Always (idempotently) ensure the topic is materialized locally now.
	_, _ = b.GetTopic(consumergroup.InternalTopic)
	return nil
}

// replayOffsetsTopic reads every message from every local __consumer_offsets
// partition into the in-memory store. Run after the topic is materialized
// and recovered from disk.
func (b *Broker) replayOffsetsTopic() {
	t, err := b.GetTopic(consumergroup.InternalTopic)
	if err != nil {
		return // topic not bootstrapped yet
	}
	count := 0
	for _, p := range t.Partitions {
		if !p.IsLocal() {
			continue
		}
		offset := uint64(0)
		for {
			msgs, err := p.Read(offset, 1000)
			if err != nil || len(msgs) == 0 {
				break
			}
			for _, m := range msgs {
				group, tn, part, rec, derr := consumergroup.DecodeMessage(m)
				if derr != nil {
					continue
				}
				b.groups.store.Apply(group, tn, part, rec)
				count++
			}
			last := msgs[len(msgs)-1].Offset
			offset = last + 1
		}
	}
	if count > 0 {
		log.Printf("replayed %d offset commits from %s", count, consumergroup.InternalTopic)
	}
}

// coordinatorBrokerFor returns the broker ID that should own (lead) group g.
// In standalone mode that's always us. In cluster mode, it's the leader of
// fnv(group) % offsetTopicPartitions in __consumer_offsets.
//
// Returns ("", false) if the offsets topic doesn't exist or has no leader yet.
func (b *Broker) coordinatorBrokerFor(group string) (string, bool) {
	if !b.Config.IsClusterMode() {
		return b.Config.BrokerID, true
	}
	if b.assignments == nil {
		return "", false
	}
	count := b.assignments.TopicPartitionCount(consumergroup.InternalTopic)
	if count == 0 {
		return "", false
	}
	part := consumergroup.PartitionForGroup(group, count)
	a, ok := b.assignments.GetAssignment(consumergroup.InternalTopic, part)
	if !ok || a.Leader == "" {
		return "", false
	}
	return a.Leader, true
}

// IsCoordinatorFor reports whether this broker is the coordinator for the
// given group. Standalone mode: always true.
func (b *Broker) IsCoordinatorFor(group string) bool {
	id, ok := b.coordinatorBrokerFor(group)
	if !ok {
		return false
	}
	return id == b.Config.BrokerID
}

// CoordinatorBrokerFor exposes the routing decision for the given group.
// Used by external tests and admin tooling.
func (b *Broker) CoordinatorBrokerFor(group string) (string, bool) {
	return b.coordinatorBrokerFor(group)
}

// LookupCommittedOffset returns the last committed offset stored locally for
// the (group, topic, partition) row. Returns (0, false) if no such commit was
// ever applied on this broker. Used by tests and admin tools to verify that a
// coordinator has accepted a commit without poking at internal state.
func (b *Broker) LookupCommittedOffset(group, topicName string, partition int) (uint64, bool) {
	if b.groups == nil || b.groups.store == nil {
		return 0, false
	}
	rec, ok := b.groups.store.Lookup(group, topicName, partition)
	if !ok {
		return 0, false
	}
	return rec.Offset, true
}

