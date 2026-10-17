package cluster

import (
	"testing"
	"time"
)

func TestWatcherReconnection(t *testing.T) {
	requireEtcd(t)

	// Create a cluster client
	c1, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b1", Addr: "http://localhost:8081"})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	defer c1.Close()

	// Wait for initial sync
	time.Sleep(500 * time.Millisecond)

	// Trigger a connection flap by setting a bad endpoint
	c1.client.SetEndpoints("localhost:2380") // unreachable port
	
	// Wait a bit so the current watch drops and the reconnect loop starts failing
	time.Sleep(1 * time.Second)

	// Restore the valid endpoint
	c1.client.SetEndpoints("localhost:2379")

	// Wait for the reconnect loop to succeed and establish a new watch
	time.Sleep(1 * time.Second)

	// Now register a new broker and see if c1 discovers it
	c2, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b2", Addr: "http://localhost:8082"})
	if err != nil {
		t.Fatalf("create cluster 2: %v", err)
	}
	defer c2.Close()

	// Give c1 time to receive the watch event for b2
	time.Sleep(1 * time.Second)

	brokers := c1.GetBrokers()
	found := false
	for _, b := range brokers {
		if b.ID == "b2" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("c1 did not discover b2 after watcher reconnection")
	}
}
