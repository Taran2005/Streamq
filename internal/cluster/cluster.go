package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	brokerPrefix = "/streamq/brokers/"
	leaseTTL     = 10 // seconds — if a broker dies, its key expires after this
)

// BrokerInfo is what each broker registers in etcd.
type BrokerInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // HTTP address for forwarding (e.g. "http://localhost:8081")
}

// Cluster handles broker registration, discovery, and liveness.
// Each broker registers itself in etcd with a lease. If it dies, the lease
// expires and other brokers stop routing to it.
type Cluster struct {
	client  *clientv3.Client
	self    BrokerInfo
	leaseID clientv3.LeaseID

	mu      sync.RWMutex
	brokers map[string]BrokerInfo // id → info (cached, updated by watcher)

	controller *Controller
	cancel     context.CancelFunc
}

// NewCluster connects to etcd, registers this broker, and starts watching for peers.
func NewCluster(etcdEndpoints []string, self BrokerInfo) (*Cluster, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to etcd: %w", err)
	}

	c := &Cluster{
		client:  client,
		self:    self,
		brokers: make(map[string]BrokerInfo),
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	// Register ourselves in etcd with a lease
	if err := c.register(ctx); err != nil {
		client.Close()
		cancel()
		return nil, fmt.Errorf("register broker: %w", err)
	}

	// Load existing brokers and start watching for changes
	if err := c.loadBrokers(ctx); err != nil {
		client.Close()
		cancel()
		return nil, fmt.Errorf("load brokers: %w", err)
	}

	go c.watchBrokers(ctx)
	go c.keepAlive(ctx)

	// Start controller election with its own child context
	ctrlCtx, ctrlCancel := context.WithCancel(ctx)
	c.controller = NewController(client, self.ID, ctrlCancel)
	go c.controller.Run(ctrlCtx)

	return c, nil
}

// register creates a lease and puts this broker's info in etcd.
func (c *Cluster) register(ctx context.Context) error {
	resp, err := c.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("create lease: %w", err)
	}
	c.leaseID = resp.ID

	data, err := json.Marshal(c.self)
	if err != nil {
		return fmt.Errorf("marshal broker info: %w", err)
	}
	key := brokerPrefix + c.self.ID

	_, err = c.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("put broker key: %w", err)
	}

	log.Printf("registered broker %s at %s (lease TTL=%ds)", c.self.ID, c.self.Addr, leaseTTL)
	return nil
}

// keepAlive renews the lease periodically so etcd knows we're alive.
func (c *Cluster) keepAlive(ctx context.Context) {
	ch, err := c.client.KeepAlive(ctx, c.leaseID)
	if err != nil {
		log.Printf("keepalive failed: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				log.Println("keepalive channel closed")
				return
			}
			_ = resp // just drain; the library handles renewal
		}
	}
}

// loadBrokers fetches all currently registered brokers from etcd.
func (c *Cluster) loadBrokers(ctx context.Context) error {
	resp, err := c.client.Get(ctx, brokerPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, kv := range resp.Kvs {
		var info BrokerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			continue
		}
		c.brokers[info.ID] = info
	}

	log.Printf("discovered %d broker(s) in cluster", len(c.brokers))
	return nil
}

// watchBrokers watches etcd for broker registrations and departures.
func (c *Cluster) watchBrokers(ctx context.Context) {
	wch := c.client.Watch(ctx, brokerPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok {
				return
			}
			c.mu.Lock()
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					var info BrokerInfo
					if err := json.Unmarshal(ev.Kv.Value, &info); err == nil {
						c.brokers[info.ID] = info
						log.Printf("broker joined: %s at %s", info.ID, info.Addr)
					}
				case clientv3.EventTypeDelete:
					// Key is "/streamq/brokers/{id}" — extract ID
					id := string(ev.Kv.Key[len(brokerPrefix):])
					delete(c.brokers, id)
					log.Printf("broker left: %s", id)
				}
			}
			c.mu.Unlock()
		}
	}
}

// GetBroker returns the info for a broker by ID.
func (c *Cluster) GetBroker(id string) (BrokerInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.brokers[id]
	return info, ok
}

// GetBrokers returns all known brokers.
func (c *Cluster) GetBrokers() []BrokerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	brokers := make([]BrokerInfo, 0, len(c.brokers))
	for _, b := range c.brokers {
		brokers = append(brokers, b)
	}
	return brokers
}

// Self returns this broker's info.
func (c *Cluster) Self() BrokerInfo {
	return c.self
}

// IsController returns true if this broker is the current controller.
func (c *Cluster) IsController() bool {
	return c.controller.IsLeader()
}

// ControllerID returns the current controller's broker ID from etcd.
func (c *Cluster) ControllerID(ctx context.Context) string {
	return c.controller.LeaderID(ctx)
}

// Controller returns the controller instance.
func (c *Cluster) Controller() *Controller {
	return c.controller
}

// Client returns the etcd client (used by assignment manager).
func (c *Cluster) Client() *clientv3.Client {
	return c.client
}

// Close deregisters from etcd and shuts down.
func (c *Cluster) Close() error {
	// Stop controller first — this cancels its context and closes its session
	c.controller.Stop()

	// Revoke lease — immediately removes our broker key
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := c.client.Revoke(ctx, c.leaseID); err != nil {
		log.Printf("revoke lease: %v", err)
	}

	c.cancel()
	return c.client.Close()
}
