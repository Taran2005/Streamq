package cluster

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// requireEtcd connects to etcd or skips the test.
func requireEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Status(ctx, "localhost:2379")
	if err != nil {
		client.Close()
		t.Skipf("etcd not reachable: %v", err)
	}

	// Clean up test keys before each test
	client.Delete(context.Background(), "/streamq/", clientv3.WithPrefix())

	t.Cleanup(func() {
		client.Delete(context.Background(), "/streamq/", clientv3.WithPrefix())
		client.Close()
	})

	return client
}

func TestBrokerRegistration(t *testing.T) {
	requireEtcd(t)

	c1, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b1", Addr: "http://localhost:8081"})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	defer c1.Close()

	c2, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b2", Addr: "http://localhost:8082"})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	defer c2.Close()

	// Give watchers time to sync
	time.Sleep(500 * time.Millisecond)

	// Each broker should see the other
	brokers := c1.GetBrokers()
	if len(brokers) != 2 {
		t.Fatalf("broker 1 expected 2 brokers, got %d", len(brokers))
	}

	brokers = c2.GetBrokers()
	if len(brokers) != 2 {
		t.Fatalf("broker 2 expected 2 brokers, got %d", len(brokers))
	}
}

func TestControllerElection(t *testing.T) {
	requireEtcd(t)

	c1, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b1", Addr: "http://localhost:8081"})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	defer c1.Close()

	c2, err := NewCluster([]string{"localhost:2379"}, BrokerInfo{ID: "b2", Addr: "http://localhost:8082"})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	defer c2.Close()

	// Wait for election to settle
	time.Sleep(2 * time.Second)

	// Exactly one should be controller
	leaders := 0
	if c1.IsController() {
		leaders++
	}
	if c2.IsController() {
		leaders++
	}

	if leaders != 1 {
		t.Fatalf("expected exactly 1 controller, got %d", leaders)
	}
}

func TestPartitionAssignment(t *testing.T) {
	client := requireEtcd(t)

	am := NewAssignmentManager(client)
	ctx := context.Background()
	if err := am.Start(ctx); err != nil {
		t.Fatalf("start assignment manager: %v", err)
	}
	defer am.Stop()

	brokers := []BrokerInfo{
		{ID: "b1", Addr: "http://localhost:8081"},
		{ID: "b2", Addr: "http://localhost:8082"},
		{ID: "b3", Addr: "http://localhost:8083"},
	}

	assignments, err := am.AssignPartitions(ctx, "orders", 6, brokers)
	if err != nil {
		t.Fatalf("assign partitions: %v", err)
	}

	if len(assignments) != 6 {
		t.Fatalf("expected 6 assignments, got %d", len(assignments))
	}

	// Every partition should be assigned to some broker
	for _, a := range assignments {
		if a.BrokerID == "" {
			t.Errorf("partition %d has no broker", a.Partition)
		}
	}

	// Verify lookup works
	ownerID, ok := am.GetBrokerForPartition("orders", 0)
	if !ok {
		t.Fatal("partition 0 not found")
	}
	if ownerID != assignments[0].BrokerID {
		t.Errorf("partition 0 owner mismatch: %s vs %s", ownerID, assignments[0].BrokerID)
	}

	// Verify assignments spread across brokers (with 6 partitions and 3 brokers)
	counts := make(map[string]int)
	for _, a := range assignments {
		counts[a.BrokerID]++
	}
	if len(counts) < 2 {
		t.Errorf("assignments not spread well: %v", counts)
	}
}

func TestHashPartitionDeterministic(t *testing.T) {
	brokers := []BrokerInfo{
		{ID: "b1"}, {ID: "b2"}, {ID: "b3"},
	}

	// Same input should always produce same output
	result1 := hashPartition("orders", 0, brokers)
	result2 := hashPartition("orders", 0, brokers)
	if result1 != result2 {
		t.Errorf("hash not deterministic: %s vs %s", result1, result2)
	}

	// Different partitions can map to different brokers
	results := make(map[string]bool)
	for i := 0; i < 10; i++ {
		results[hashPartition("orders", i, brokers)] = true
	}
	// With 10 partitions and 3 brokers, we should hit at least 2 brokers
	if len(results) < 2 {
		t.Errorf("expected at least 2 different brokers, got %d", len(results))
	}
}
