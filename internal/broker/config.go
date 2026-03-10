package broker

type Config struct {
	Port             int
	MaxMessageSize   int
	DataDir          string // directory for persistent storage ("" = in-memory only)
	MaxSegmentBytes  int64  // max size per segment file before rolling

	// Cluster mode (all empty = standalone mode)
	BrokerID      string   // unique ID for this broker in the cluster
	AdvertiseAddr string   // address other brokers use to reach us (e.g. "http://localhost:8081")
	EtcdEndpoints []string // etcd cluster endpoints (e.g. ["localhost:2379"])
}

func DefaultConfig() Config {
	return Config{
		Port:            8080,
		MaxMessageSize:  1_048_576,  // 1MB
		DataDir:         "",         // empty = in-memory mode
		MaxSegmentBytes: 10_485_760, // 10MB per segment
	}
}

// IsClusterMode returns true if etcd endpoints are configured.
func (c Config) IsClusterMode() bool {
	return len(c.EtcdEndpoints) > 0
}
