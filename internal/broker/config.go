package broker

import "time"

type Config struct {
	Port             int
	MaxMessageSize   int
	DataDir          string // directory for persistent storage ("" = in-memory only)
	MaxSegmentBytes  int64  // max size per segment file before rolling

	// Cluster mode (all empty = standalone mode)
	BrokerID      string   // unique ID for this broker in the cluster
	AdvertiseAddr string   // address other brokers use to reach us (e.g. "http://localhost:8081")
	EtcdEndpoints []string // etcd cluster endpoints (e.g. ["localhost:2379"])

	// Replication
	ReplicationFactor int           // default replicas per partition (capped at #brokers)
	InternalToken     string        // shared secret for /internal/* endpoints
	MaxLagMessages    uint64        // followers more than N messages behind drop out of ISR
	MaxLagDuration    time.Duration // followers not seen in this long drop out of ISR
	ShutdownTimeout   time.Duration // hard cap on graceful shutdown
}

func DefaultConfig() Config {
	return Config{
		Port:              8080,
		MaxMessageSize:    1_048_576,  // 1MB
		DataDir:           "",         // empty = in-memory mode
		MaxSegmentBytes:   10_485_760, // 10MB per segment
		ReplicationFactor: 3,
		InternalToken:     "dev", // override via STREAMQ_INTERNAL_TOKEN
		MaxLagMessages:    100,
		MaxLagDuration:    10 * time.Second,
		ShutdownTimeout:   10 * time.Second,
	}
}

// IsClusterMode returns true if etcd endpoints are configured.
func (c Config) IsClusterMode() bool {
	return len(c.EtcdEndpoints) > 0
}
