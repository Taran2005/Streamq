# StreamQ

A Kafka-style distributed message broker built from scratch in Go, as a learning project.

## What it does

- Topic + partition model. Producers append, consumers fetch by offset.
- **Persistent storage**: custom append-only log with segment rolling, CRC32 integrity, crash recovery.
- **Multi-broker cluster** coordinated via etcd. Controller pattern for partition assignment, request forwarding between brokers.
- **Replication** with per-partition leader election, ISR tracking, and configurable acks (`0`, `1`, `all`).
- **Graceful shutdown** with leadership transfer — zero-downtime restarts.
- **Consumer groups** with range and round-robin partition assignment, rebalancing on membership change, offset commits dogfooded into an internal `__consumer_offsets` topic.
- **Dead-letter queue** for poison messages (consumer-driven).

## Build & run

Requires Go 1.25+ and Docker.

```bash
# Standalone broker
go run ./cmd/broker --port 8080 --data-dir ./data

# 3-broker cluster + etcd
docker-compose up
```

## Admin CLI

```bash
go build -o streamq-cli ./cmd/streamq-cli

./streamq-cli topic create --broker http://localhost:8081 --name orders --partitions 6 --replication-factor 3
./streamq-cli topic list --broker http://localhost:8081
./streamq-cli topic describe --broker http://localhost:8081 --name orders

./streamq-cli produce --broker http://localhost:8081 --topic orders --key user-42 --value "hello"
./streamq-cli consume --broker http://localhost:8081 --topic orders --partition 0 --offset 0

./streamq-cli partitions list --etcd localhost:2379
./streamq-cli partitions reassign --etcd localhost:2379 --topic orders --partition 0 --to broker-2 --throttle 50

./streamq-cli consume-group --broker http://localhost:8081 --group order-processor --topic orders
./streamq-cli groups list --broker http://localhost:8081
```

## HTTP API

| Endpoint | Purpose |
|----------|---------|
| `POST /topics` | Create topic. Body `{name, partitions, replication_factor?}` |
| `GET /topics` | List user topics. `?internal=true` to include `__*` topics |
| `POST /topics/{name}/messages?acks=0|1|all` | Produce. Body `{key, value}` |
| `GET /topics/{name}/messages?partition=N&offset=N&timeout=Ns&max=N` | Consume (long-poll, HW-gated) |
| `GET /metadata` | Cluster topology + partition→broker map |
| `POST /groups/{group}/join` | Consumer group join |
| `POST /groups/{group}/heartbeat` | Heartbeat |
| `POST /groups/{group}/leave` | Leave |
| `POST /groups/{group}/commit` | Commit offsets |
| `GET /groups/{group}/offsets?topic=<t>` | Read committed offsets |
| `POST /groups/{group}/dlq` | Mark message as poison |
| `GET /groups` | List groups served by this broker |

Internal endpoints (require `X-StreamQ-Internal` header):
- `POST /internal/replicate/{topic}/{partition}` — leader pushes to followers
- `GET /internal/follow/{topic}/{partition}?from=<n>` — follower backfill

## Configuration

| Flag | Default | Purpose |
|------|---------|---------|
| `-port` | 8080 | HTTP listen port |
| `-data-dir` | "" (in-memory) | Persistent storage root |
| `-id` | "" (required in cluster mode) | Broker ID |
| `-advertise-addr` | derived | URL other brokers use to reach us |
| `-etcd` | "" | Comma-separated etcd endpoints (enables cluster mode) |

## Architecture

```
cmd/
  broker/                  broker entry point
  bench/                   produce/consume throughput benchmark
  streamq-cli/             admin CLI

internal/
  broker/                  HTTP server, handlers, replication runtime
  cluster/                 etcd registration, controller + per-partition election, forwarding
  consumergroup/           group coordinator, strategies, offset store
  storage/                 append-only log with segments, index, CRC32
  topic/                   topic + partition + message types
  common/                  typed errors, JSON response helpers
  testutil/                in-process broker test helpers
```

## Storage format

Each partition is a directory of segment files (`.sqlog`) with matching index files (`.sqidx`). Segments roll at 10MB.

Each message on disk:
```
[4 bytes: length]
[8 bytes: offset]
[8 bytes: timestamp]
[4 bytes: key length]   [N: key]
[4 bytes: value length] [N: value]
[4 bytes: CRC32]
```

Index files are written atomically (write-to-temp + fsync + rename) and CRC-protected so half-written files are detected at startup.

## Testing

```bash
go test -race -count=1 ./...
```

Cluster-mode tests require etcd at `localhost:2379` and skip cleanly if unavailable. `docker-compose up etcd` for local etcd.

## What's intentionally NOT here

These are documented tradeoffs, not oversights:

- **gRPC transport** — HTTP throughout. gRPC + zero-copy `sendfile` is designed-in via wire-compatible storage format, deferred.
- **fsync batching** — every append fsyncs. Trade for ~10-50x throughput available with a small batching change.
- **Backpressure** — no memory-bounded receive buffer. Under sustained overload, broker would OOM rather than return `RESOURCE_EXHAUSTED`.
- **Producer client library** — only a CLI exists.
- **Exactly-once semantics** — at-least-once only. No idempotent producer sequence numbers.
- **Log compaction / TTL** — old messages stay forever.
- **mTLS / token auth** — internal endpoints use a static dev shared-secret.
- **Tiered storage (S3/GCS)** — local disk only.

## License

See LICENSE.
