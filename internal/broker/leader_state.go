package broker

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/streamq/streamq/internal/cluster"
)

// followerLag is the leader's view of a single follower for a single partition.
type followerLag struct {
	offset    uint64
	lastSeen  time.Time
}

// partitionLeaderState tracks per-follower offsets for one partition this
// broker leads. ISR membership is recomputed periodically by the broker's
// background loop.
type partitionLeaderState struct {
	mu        sync.Mutex
	followers map[string]*followerLag
}

func newPartitionLeaderState() *partitionLeaderState {
	return &partitionLeaderState{followers: make(map[string]*followerLag)}
}

func (s *partitionLeaderState) note(follower string, offset uint64, when time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lag, ok := s.followers[follower]
	if !ok {
		lag = &followerLag{}
		s.followers[follower] = lag
	}
	if offset > lag.offset {
		lag.offset = offset
	}
	if when.After(lag.lastSeen) {
		lag.lastSeen = when
	}
}

// snapshot returns a copy of follower state for the ISR loop to inspect.
func (s *partitionLeaderState) snapshot() map[string]followerLag {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]followerLag, len(s.followers))
	for k, v := range s.followers {
		out[k] = *v
	}
	return out
}

// LeaderStateRegistry holds one partitionLeaderState per (topic, partition)
// that this broker currently leads. It also runs the background ISR
// recalculation loop.
type LeaderStateRegistry struct {
	mu     sync.RWMutex
	states map[string]map[int]*partitionLeaderState // topic → partition → state

	broker *Broker

	cancel context.CancelFunc
	done   chan struct{}
}

// newLeaderStateRegistry returns an empty registry. Run must be called to
// start the ISR loop.
func newLeaderStateRegistry(b *Broker) *LeaderStateRegistry {
	return &LeaderStateRegistry{
		states: make(map[string]map[int]*partitionLeaderState),
		broker: b,
		done:   make(chan struct{}),
	}
}

// noteFollowerSeen is the entry point used by the /internal/follow and
// /internal/replicate handlers to record follower liveness.
func (r *LeaderStateRegistry) noteFollowerSeen(topic string, partition int, follower string, offset uint64, when time.Time) {
	if follower == "" {
		return
	}
	r.mu.Lock()
	parts, ok := r.states[topic]
	if !ok {
		parts = make(map[int]*partitionLeaderState)
		r.states[topic] = parts
	}
	st, ok := parts[partition]
	if !ok {
		st = newPartitionLeaderState()
		parts[partition] = st
	}
	r.mu.Unlock()
	st.note(follower, offset, when)
}

// stateFor returns the leader state for a partition, creating it if missing.
// Called when this broker becomes leader for a partition.
func (r *LeaderStateRegistry) stateFor(topic string, partition int) *partitionLeaderState {
	r.mu.Lock()
	defer r.mu.Unlock()
	parts, ok := r.states[topic]
	if !ok {
		parts = make(map[int]*partitionLeaderState)
		r.states[topic] = parts
	}
	st, ok := parts[partition]
	if !ok {
		st = newPartitionLeaderState()
		parts[partition] = st
	}
	return st
}

// drop removes state for a partition (called when we lose leadership).
func (r *LeaderStateRegistry) drop(topic string, partition int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if parts, ok := r.states[topic]; ok {
		delete(parts, partition)
	}
}

// Run loops every 5s and recomputes ISR for every partition this broker
// leads. Followers that have fallen too far behind (or stopped responding)
// are removed from ISR in the etcd assignment. Followers that have caught
// back up are re-added.
func (r *LeaderStateRegistry) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer close(r.done)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.recomputeISR(ctx)
		}
	}
}

// Stop cancels the loop and waits for it to exit.
func (r *LeaderStateRegistry) Stop() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
}

// recomputeISR walks every leadership we hold and rewrites the ISR list in
// etcd if it changed. Only the leader for a partition does this — there's a
// natural single-writer per partition because only the elected leader runs
// the loop for it.
func (r *LeaderStateRegistry) recomputeISR(ctx context.Context) {
	b := r.broker
	if b.assignments == nil {
		return
	}

	// Snapshot the partitions we currently lead.
	type entry struct {
		topic     string
		partition int
		state     *partitionLeaderState
	}
	r.mu.RLock()
	var ents []entry
	for tn, parts := range r.states {
		for pid, st := range parts {
			ents = append(ents, entry{tn, pid, st})
		}
	}
	r.mu.RUnlock()

	now := time.Now()
	maxLagMsgs := b.Config.MaxLagMessages
	if maxLagMsgs == 0 {
		maxLagMsgs = 100
	}
	maxLagDur := b.Config.MaxLagDuration
	if maxLagDur == 0 {
		maxLagDur = 10 * time.Second
	}

	for _, e := range ents {
		// Verify we still lead this partition (election may have shifted).
		a, ok := b.assignments.GetAssignment(e.topic, e.partition)
		if !ok || a.Leader != b.Config.BrokerID {
			continue
		}

		// Read leader's own latest offset for the partition.
		t, err := b.GetTopic(e.topic)
		if err != nil {
			continue
		}
		p, err := t.GetPartition(e.partition)
		if err != nil {
			continue
		}
		leaderOffset := p.LatestOffset()

		snap := e.state.snapshot()

		newISR := []string{b.Config.BrokerID} // leader is always in ISR
		for _, replica := range a.Replicas {
			if replica == b.Config.BrokerID {
				continue
			}
			lag, ok := snap[replica]
			if !ok {
				continue // never heard from this follower; drop it for now
			}
			behind := uint64(0)
			if leaderOffset > lag.offset {
				behind = leaderOffset - lag.offset
			}
			if behind <= maxLagMsgs && now.Sub(lag.lastSeen) <= maxLagDur {
				newISR = append(newISR, replica)
			}
		}
		sort.Strings(newISR)

		if !stringSlicesEqual(newISR, a.ISR) {
			updated := a
			updated.ISR = newISR
			if err := b.assignments.UpdateAssignment(ctx, updated); err != nil {
				log.Printf("isr update %s/%d: %v", e.topic, e.partition, err)
				continue
			}
			log.Printf("isr %s/%d: %v → %v (leader_off=%d)", e.topic, e.partition, a.ISR, newISR, leaderOffset)
		}
	}
}

// inSyncReplicas returns the current ISR members for a partition we lead,
// excluding self. Used by acks=all on the produce path.
func (r *LeaderStateRegistry) inSyncReplicas(topic string, partition int) []string {
	b := r.broker
	if b.assignments == nil {
		return nil
	}
	a, ok := b.assignments.GetAssignment(topic, partition)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(a.ISR))
	for _, id := range a.ISR {
		if id != b.Config.BrokerID {
			out = append(out, id)
		}
	}
	return out
}

// allReplicas returns non-self replicas for a partition, regardless of ISR
// status. Used by acks=0/1 background fanout (we still try to push to
// everyone — out-of-sync followers will fall back to /internal/follow).
func (r *LeaderStateRegistry) allReplicas(topic string, partition int) []string {
	b := r.broker
	if b.assignments == nil {
		return nil
	}
	a, ok := b.assignments.GetAssignment(topic, partition)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(a.Replicas))
	for _, id := range a.Replicas {
		if id != b.Config.BrokerID {
			out = append(out, id)
		}
	}
	return out
}

// followerOffsets returns each non-self follower's last-known offset.
// Used by graceful shutdown to pick the most-caught-up successor.
func (r *LeaderStateRegistry) followerOffsets(topic string, partition int) map[string]uint64 {
	r.mu.RLock()
	parts, ok := r.states[topic]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	st, ok := parts[partition]
	if !ok {
		return nil
	}
	snap := st.snapshot()
	out := make(map[string]uint64, len(snap))
	for k, v := range snap {
		out[k] = v.offset
	}
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ensure unused-import warnings don't bite if internalToken stays unused later
var _ = cluster.InternalHeader
