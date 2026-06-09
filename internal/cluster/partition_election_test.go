package cluster

import (
	"context"
	"testing"
	"time"
)

func TestPartitionElection_Basic(t *testing.T) {
	client := requireEtcd(t)

	pe := NewPartitionElector(client, "broker1", "topic1", 0)
	
	electedCh := make(chan struct{})
	deposedCh := make(chan struct{})

	pe.OnElected(func(ctx context.Context) {
		close(electedCh)
	})
	pe.OnDeposed(func() {
		close(deposedCh)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go pe.Run(ctx)

	// Wait to become leader
	select {
	case <-electedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	if !pe.IsLeader() {
		t.Fatal("expected IsLeader to be true")
	}

	// Stop
	cancel()

	// Wait to be deposed
	select {
	case <-deposedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deposed callback")
	}

	if pe.IsLeader() {
		t.Fatal("expected IsLeader to be false after stop")
	}
}

func TestPartitionElection_Resign(t *testing.T) {
	client := requireEtcd(t)

	pe := NewPartitionElector(client, "broker1", "topic2", 0)
	
	electedCh := make(chan struct{})
	deposedCh := make(chan struct{})

	pe.OnElected(func(ctx context.Context) {
		close(electedCh)
	})
	pe.OnDeposed(func() {
		close(deposedCh)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pe.Run(ctx)

	// Wait to become leader
	select {
	case <-electedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	pe.Resign(context.Background())

	// Wait to be deposed
	select {
	case <-deposedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deposed callback")
	}

	if pe.IsLeader() {
		t.Fatal("expected IsLeader to be false after resign")
	}

	// Make sure it doesn't re-campaign
	time.Sleep(2 * time.Second)
	if pe.IsLeader() {
		t.Fatal("expected elector to not re-campaign after resign")
	}
}

func TestPartitionElection_SessionExpiry(t *testing.T) {
	client1 := requireEtcd(t)

	pe1 := NewPartitionElector(client1, "broker1", "topic3", 0)
	
	electedCh1 := make(chan struct{})
	deposedCh1 := make(chan struct{})

	pe1.OnElected(func(ctx context.Context) {
		close(electedCh1)
	})
	pe1.OnDeposed(func() {
		close(deposedCh1)
	})

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go pe1.Run(ctx1)

	// Wait for pe1 to become leader
	select {
	case <-electedCh1:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for election 1")
	}

	// Close the etcd client to simulate a network partition or crash
	// This will cause the session to expire eventually.
	client1.Close()

	// Wait for pe1 to be deposed
	select {
	case <-deposedCh1:
	case <-time.After(15 * time.Second): // Session TTL is 10s
		t.Fatal("timed out waiting for deposed callback after session expiry")
	}

	if pe1.IsLeader() {
		t.Fatal("expected IsLeader to be false after session expiry")
	}
}
