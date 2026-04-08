package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// partitionElectionPrefix is the etcd prefix for per-partition leader election.
// Each partition we hold a replica for gets its own concurrency.Election under
// /streamq/partition-leaders/{topic}/{partition}/. When our session expires
// (broker dies / network partition / clean shutdown), the lease is revoked and
// the next replica wins the campaign.
const partitionElectionPrefix = "/streamq/partition-leaders"

// PartitionElector runs the leader election for a single (topic, partition)
// that this broker holds a replica of. It runs in its own goroutine, exposing
// an IsLeader() check and an OnElected/OnDeposed callback pair so the broker
// can wire start/stop of replication when leadership changes.
type PartitionElector struct {
	client    *clientv3.Client
	brokerID  string
	topic     string
	partition int

	isLeader atomic.Bool

	// onElected is fired (in a fresh goroutine) when this broker wins the
	// campaign. onDeposed fires when leadership is lost (session expired,
	// resigned, etc). Either may be nil.
	onElected func(ctx context.Context)
	onDeposed func()

	mu       sync.Mutex
	session  *concurrency.Session
	election *concurrency.Election

	cancel context.CancelFunc
	done   chan struct{}
}

// NewPartitionElector constructs an elector. Run must be called to start it.
func NewPartitionElector(client *clientv3.Client, brokerID, topic string, partition int) *PartitionElector {
	return &PartitionElector{
		client:    client,
		brokerID:  brokerID,
		topic:     topic,
		partition: partition,
		done:      make(chan struct{}),
	}
}

// OnElected registers a callback fired when this broker becomes leader for the
// partition. The provided context is cancelled when leadership is lost. Safe
// to call before Run.
func (pe *PartitionElector) OnElected(fn func(ctx context.Context)) {
	pe.onElected = fn
}

// OnDeposed registers a callback fired when this broker loses leadership.
func (pe *PartitionElector) OnDeposed(fn func()) {
	pe.onDeposed = fn
}

// Run blocks until ctx is cancelled. It campaigns for leadership, and when
// the underlying etcd session expires, it loops back and re-campaigns.
func (pe *PartitionElector) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	pe.mu.Lock()
	pe.cancel = cancel
	pe.mu.Unlock()

	defer close(pe.done)

	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		session, err := concurrency.NewSession(pe.client, concurrency.WithTTL(10))
		if err != nil {
			log.Printf("partition-elect %s/%d: session: %v", pe.topic, pe.partition, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		election := concurrency.NewElection(session, pe.electionKey())

		pe.mu.Lock()
		pe.session = session
		pe.election = election
		pe.mu.Unlock()

		if err := election.Campaign(ctx, pe.brokerID); err != nil {
			session.Close()
			if ctx.Err() != nil {
				return
			}
			log.Printf("partition-elect %s/%d: campaign: %v", pe.topic, pe.partition, err)
			continue
		}

		pe.isLeader.Store(true)
		log.Printf("partition-elect: broker %s elected leader for %s/%d", pe.brokerID, pe.topic, pe.partition)

		// Run the on-elected callback in its own context that we cancel
		// when leadership is lost.
		leaderCtx, leaderCancel := context.WithCancel(ctx)
		if pe.onElected != nil {
			go pe.onElected(leaderCtx)
		}

		select {
		case <-ctx.Done():
			pe.isLeader.Store(false)
			leaderCancel()
			resignCtx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = election.Resign(resignCtx)
			rcancel()
			session.Close()
			if pe.onDeposed != nil {
				pe.onDeposed()
			}
			return
		case <-session.Done():
			pe.isLeader.Store(false)
			leaderCancel()
			log.Printf("partition-elect: broker %s lost leadership for %s/%d (session expired)", pe.brokerID, pe.topic, pe.partition)
			if pe.onDeposed != nil {
				pe.onDeposed()
			}
			// Loop back and re-campaign.
		}
	}
}

// IsLeader reports whether this broker currently holds leadership for the
// partition.
func (pe *PartitionElector) IsLeader() bool {
	return pe.isLeader.Load()
}

// Resign voluntarily gives up leadership and waits for the etcd write to
// complete. Returns the assignment update that should be written to etcd —
// caller may want to ensure the next leader has materialized before exiting.
func (pe *PartitionElector) Resign(ctx context.Context) error {
	pe.mu.Lock()
	el := pe.election
	pe.mu.Unlock()
	if el == nil {
		return nil
	}
	if err := el.Resign(ctx); err != nil {
		return fmt.Errorf("resign %s/%d: %w", pe.topic, pe.partition, err)
	}
	pe.isLeader.Store(false)
	return nil
}

// CurrentLeader returns the broker ID currently holding leadership in etcd,
// or "" if there is no leader / lookup fails.
func (pe *PartitionElector) CurrentLeader(ctx context.Context) string {
	pe.mu.Lock()
	el := pe.election
	pe.mu.Unlock()
	if el == nil {
		return ""
	}
	resp, err := el.Leader(ctx)
	if err != nil {
		return ""
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}

// Stop cancels the election loop. Blocks until the Run goroutine exits.
func (pe *PartitionElector) Stop() {
	pe.mu.Lock()
	cancel := pe.cancel
	pe.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	<-pe.done
}

// electionKey returns the etcd prefix passed to concurrency.NewElection.
func (pe *PartitionElector) electionKey() string {
	return fmt.Sprintf("%s/%s/%d", partitionElectionPrefix, pe.topic, pe.partition)
}
