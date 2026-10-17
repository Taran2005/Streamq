package cluster

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const electionPrefix = "/streamq/controller-election"

// Controller manages leader election via etcd.
// The controller broker owns the partition assignment map — it decides
// which broker gets which partitions when topics are created.
// If the controller dies, etcd elects a new one from remaining brokers.
type Controller struct {
	client   *clientv3.Client
	brokerID string
	isLeader atomic.Bool

	mu       sync.Mutex // protects session, election, cancel
	session  *concurrency.Session
	election *concurrency.Election
	cancel   context.CancelFunc
}

func NewController(client *clientv3.Client, brokerID string, cancel context.CancelFunc) *Controller {
	return &Controller{
		client:   client,
		brokerID: brokerID,
		cancel:   cancel,
	}
}

// Run starts the election campaign. Blocks until elected or context cancelled.
// When elected, sets isLeader=true. If leadership is lost, sets it back to false.
func (c *Controller) Run(ctx context.Context) {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Create a session with etcd — this is a lease that auto-renews.
		// If we crash, the session expires and we lose leadership.
		session, err := concurrency.NewSession(c.client, concurrency.WithTTL(10))
		if err != nil {
			log.Printf("controller: create session: %v", err)
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
		backoff = time.Second // reset on success

		election := concurrency.NewElection(session, electionPrefix)

		c.mu.Lock()
		c.session = session
		c.election = election
		c.mu.Unlock()

		log.Printf("controller: broker %s campaigning for leadership", c.brokerID)

		// Campaign blocks until we become leader or context is cancelled
		if err := election.Campaign(ctx, c.brokerID); err != nil {
			session.Close()
			if ctx.Err() != nil {
				return
			}
			log.Printf("controller: campaign failed: %v", err)
			continue
		}

		c.isLeader.Store(true)
		log.Printf("controller: broker %s elected as controller", c.brokerID)

		// Watch for session expiry (means we lost leadership)
		select {
		case <-ctx.Done():
			c.isLeader.Store(false)
			resignCtx, resignCancel := context.WithTimeout(context.Background(), 3*time.Second)
			c.resign(resignCtx)
			resignCancel()
			session.Close()
			return
		case <-session.Done():
			c.isLeader.Store(false)
			log.Printf("controller: broker %s lost leadership (session expired)", c.brokerID)
			// Loop back and re-campaign
		}
	}
}

// IsLeader returns whether this broker is the current controller.
func (c *Controller) IsLeader() bool {
	return c.isLeader.Load()
}

// LeaderID returns the current controller's broker ID by reading from etcd.
// Returns "" if no leader is elected or etcd is unreachable.
func (c *Controller) LeaderID(ctx context.Context) string {
	c.mu.Lock()
	el := c.election
	c.mu.Unlock()

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

// resign voluntarily gives up leadership.
func (c *Controller) resign(ctx context.Context) {
	c.mu.Lock()
	el := c.election
	c.mu.Unlock()

	if el != nil {
		if err := el.Resign(ctx); err != nil {
			log.Printf("controller: resign failed: %v", err)
		}
	}
}

// Stop cancels the election loop and closes the session.
func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
}
