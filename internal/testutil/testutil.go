// Package testutil provides helpers for spinning up in-process brokers
// from tests. It exists so Phase 4/5 integration tests don't each have to
// reinvent the same "give me a broker on a random port" boilerplate.
package testutil

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/streamq/streamq/internal/broker"
)

// StartBroker spawns an in-process broker on a random port using
// httptest.NewServer. Standalone mode (no cluster, no etcd). Data is written
// under t.TempDir() so each test gets a clean slate.
//
// Returns the broker, the listening URL (e.g. "http://127.0.0.1:54321"), and
// a cleanup func. The cleanup func is also registered with t.Cleanup, so
// callers can ignore it — but returning it matches the spec and lets callers
// tear down early if they want.
func StartBroker(t *testing.T) (*broker.Broker, string, func()) {
	t.Helper()

	cfg := broker.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.BrokerID = "test-" + t.Name()

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("testutil: broker.New: %v", err)
	}

	srv := broker.NewServer(b)
	ts := httptest.NewServer(srv.Handler())

	cleanup := func() {
		ts.Close()
		if err := b.Close(); err != nil {
			t.Logf("testutil: broker.Close: %v", err)
		}
	}
	t.Cleanup(cleanup)

	return b, ts.URL, cleanup
}

// StartClusterBroker spawns one broker in cluster mode joined to the given etcd
// endpoints. Caller should call RequireEtcd first to skip cleanly when etcd is
// unavailable — this helper assumes etcd is reachable and will fail the test
// otherwise.
func StartClusterBroker(t *testing.T, etcdEndpoints []string, id string) (*broker.Broker, string, func()) {
	t.Helper()

	// We need to know the URL before we can set AdvertiseAddr, but we also
	// can't start the broker until we know its URL. Trick: spin up httptest
	// first against a placeholder handler, capture the URL, then build the
	// broker pointing AdvertiseAddr at that URL and swap the handler in.
	//
	// httptest.Server is just a net.Listener + http.Server, and Config.Handler
	// is mutable before the first request. So set the handler after we build
	// the broker.
	ts := httptest.NewUnstartedServer(nil)
	ts.Start()

	cfg := broker.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.BrokerID = id
	cfg.AdvertiseAddr = ts.URL
	cfg.EtcdEndpoints = etcdEndpoints

	b, err := broker.New(cfg)
	if err != nil {
		ts.Close()
		t.Fatalf("testutil: broker.New: %v", err)
	}
	if err := b.JoinCluster(); err != nil {
		ts.Close()
		_ = b.Close()
		t.Fatalf("testutil: JoinCluster: %v", err)
	}

	srv := broker.NewServer(b)
	ts.Config.Handler = srv.Handler()

	cleanup := func() {
		ts.Close()
		if err := b.Close(); err != nil {
			t.Logf("testutil: broker.Close: %v", err)
		}
	}
	t.Cleanup(cleanup)

	return b, ts.URL, cleanup
}

// RequireEtcd dials etcd and skips the test if unreachable. Returns the
// connected client. Also wipes /streamq/ keys before the test and registers
// a cleanup that wipes them again — so consecutive runs don't leak state.
func RequireEtcd(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()

	if len(endpoints) == 0 {
		endpoints = []string{"localhost:2379"}
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("etcd unreachable (dial): %v", err)
	}

	// Confirm we can actually talk to etcd — clientv3.New won't fail
	// just because the endpoint is unreachable; the real check is the
	// first request.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, endpoints[0]); err != nil {
		client.Close()
		t.Skipf("etcd unreachable (status): %v", err)
	}

	wipe := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = client.Delete(ctx, "/streamq/", clientv3.WithPrefix())
	}
	wipe()
	t.Cleanup(func() {
		wipe()
		client.Close()
	})

	return client
}
