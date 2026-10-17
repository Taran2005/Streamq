package broker

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streamq/streamq/internal/cluster"
)

// partitionRuntime owns the per-partition background machinery for a single
// replica: the leader elector (always running for any replica), and the
// follower-pull loop (running only while we're NOT the leader).
//
// Lifecycle: one is created the first time a broker realizes it's a replica
// for a partition, and stays alive until the broker stops being a replica
// (assignment change) or the broker shuts down.
type partitionRuntime struct {
	broker    *Broker
	topic     string
	partition int

	elector *cluster.PartitionElector

	mu              sync.Mutex
	followCtx       context.Context
	followCancel    context.CancelFunc
	followDone      chan struct{}
}

// ensurePartitionRuntime creates the runtime for a (topic, partition) if it
// doesn't exist yet. Safe to call repeatedly.
func (b *Broker) ensurePartitionRuntime(topicName string, partition int) {
	b.pmMu.Lock()
	defer b.pmMu.Unlock()

	parts, ok := b.partitionMgrs[topicName]
	if !ok {
		parts = make(map[int]*partitionRuntime)
		b.partitionMgrs[topicName] = parts
	}
	if _, ok := parts[partition]; ok {
		return // already running
	}

	pr := &partitionRuntime{
		broker:    b,
		topic:     topicName,
		partition: partition,
	}
	parts[partition] = pr

	// Start elector. The leader callback starts ISR tracking; the deposed
	// callback starts the follower pull loop.
	pr.elector = cluster.NewPartitionElector(b.cluster.Client(), b.Config.BrokerID, topicName, partition)
	pr.elector.OnElected(func(ctx context.Context) {
		pr.onElected(ctx)
	})
	pr.elector.OnDeposed(func() {
		pr.onDeposed()
	})
	go pr.elector.Run(context.Background())

	// Until we win the election, we're a follower — start pulling immediately.
	pr.startFollowLoop()
}

// stopPartitionRuntime tears down a single partition's runtime. Called when
// the assignment shows we no longer hold a replica.
func (b *Broker) stopPartitionRuntime(topicName string, partition int) {
	b.pmMu.Lock()
	pr, ok := b.partitionMgrs[topicName][partition]
	if ok {
		delete(b.partitionMgrs[topicName], partition)
	}
	b.pmMu.Unlock()

	if pr != nil {
		pr.stop()
	}
}

// stopAllPartitionRuntimes tears down every runtime. Used on broker close.
func (b *Broker) stopAllPartitionRuntimes() {
	b.pmMu.Lock()
	all := b.partitionMgrs
	b.partitionMgrs = make(map[string]map[int]*partitionRuntime)
	b.pmMu.Unlock()

	for _, parts := range all {
		for _, pr := range parts {
			pr.stop()
		}
	}
}

// stop cancels the follower loop, resigns + stops the elector.
func (pr *partitionRuntime) stop() {
	pr.stopFollowLoop()
	if pr.elector != nil {
		pr.elector.Stop()
	}
	if pr.broker.leaderStates != nil {
		pr.broker.leaderStates.drop(pr.topic, pr.partition)
	}
}

// onElected is called when this broker wins the partition election. Stops
// any follower loop, makes sure the assignment in etcd reflects us as
// leader, and primes the leader state.
func (pr *partitionRuntime) onElected(ctx context.Context) {
	log.Printf("partition-runtime: %s/%d elected leader", pr.topic, pr.partition)
	pr.stopFollowLoop()

	// Mirror leader in the assignment so other brokers and clients see it.
	if pr.broker.assignments != nil {
		a, ok := pr.broker.assignments.GetAssignment(pr.topic, pr.partition)
		if ok && a.Leader != pr.broker.Config.BrokerID {
			a.Leader = pr.broker.Config.BrokerID
			a.BrokerID = pr.broker.Config.BrokerID
			a.Epoch++
			// On takeover, conservatively shrink ISR to just us. The ISR
			// loop will re-add caught-up followers within a few seconds.
			a.ISR = []string{pr.broker.Config.BrokerID}
			if err := pr.broker.assignments.UpdateAssignment(ctx, a); err != nil {
				log.Printf("partition-runtime: update assignment after election %s/%d: %v", pr.topic, pr.partition, err)
			}
		}
	}

	// Prime leader state so the ISR loop knows about us.
	if pr.broker.leaderStates != nil {
		pr.broker.leaderStates.stateFor(pr.topic, pr.partition)
	}
}

// onDeposed is called when we lose leadership. Restart the follower pull loop.
func (pr *partitionRuntime) onDeposed() {
	log.Printf("partition-runtime: %s/%d lost leadership", pr.topic, pr.partition)
	if pr.broker.leaderStates != nil {
		pr.broker.leaderStates.drop(pr.topic, pr.partition)
	}
	pr.startFollowLoop()
}

// startFollowLoop spawns the goroutine that pulls from the current leader.
// No-op if already running.
func (pr *partitionRuntime) startFollowLoop() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.followCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	pr.followCtx = ctx
	pr.followCancel = cancel
	pr.followDone = make(chan struct{})
	go pr.followLoop(ctx, pr.followDone)
}

// stopFollowLoop cancels the follower loop and waits for it to exit.
func (pr *partitionRuntime) stopFollowLoop() {
	pr.mu.Lock()
	cancel := pr.followCancel
	done := pr.followDone
	pr.followCancel = nil
	pr.followCtx = nil
	pr.followDone = nil
	pr.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// followLoop pulls messages from the current leader every 2s.
func (pr *partitionRuntime) followLoop(ctx context.Context, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Fire once immediately so initial catch-up isn't gated on 2s.
	pr.followOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pr.followOnce(ctx)
		}
	}
}

// followOnce performs a single /internal/follow call against the current
// leader. Updates the local store and high watermark on success.
func (pr *partitionRuntime) followOnce(ctx context.Context) {
	b := pr.broker
	if b.assignments == nil || b.replicator == nil {
		return
	}
	a, ok := b.assignments.GetAssignment(pr.topic, pr.partition)
	if !ok {
		return
	}
	if a.Leader == "" || a.Leader == b.Config.BrokerID {
		return // no leader, or we ARE the leader
	}

	t, err := b.GetTopic(pr.topic)
	if err != nil {
		return
	}
	p, err := t.GetPartition(pr.partition)
	if err != nil || !p.IsLocal() {
		return
	}

	from := p.LatestOffset()
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msgs, leaderHW, err := b.replicator.Fetch(fctx, a.Leader, pr.topic, pr.partition, a.Epoch, from, 500)
	if err != nil {
		// Many transient errors (election just happened, leader rebooting).
		// Don't spam — log only at debug level conceptually.
		if !errors.Is(err, context.Canceled) {
			log.Printf("follow %s/%d from %s: %v", pr.topic, pr.partition, a.Leader, err)
		}
		return
	}

	for _, m := range msgs {
		if _, err := p.AppendReplicated(m); err != nil {
			log.Printf("follow apply %s/%d: %v", pr.topic, pr.partition, err)
			break
		}
	}
	// Advance high watermark to leader's view, but cap at our local LatestOffset
	// so we never claim visibility for data we don't yet have.
	local := p.LatestOffset()
	visible := leaderHW
	if visible > local {
		visible = local
	}
	p.SetHighWatermark(visible)
}
