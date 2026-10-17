# StreamQ

A Kafka-style message broker written in Go. I built it to understand how log-based brokers actually work: storage, replication, leader election, consumer groups. It runs standalone or as a cluster coordinated through etcd.

It's a learning project, not a Kafka replacement. See [Limitations](#limitations) for what's missing.

## Features

- Topics split into partitions. Producers append, consumers read by offset.
- Append-only log on disk: segment files, CRC32 per message, recovery after a crash or torn write.
- Multi-broker clusters. etcd handles membership, a controller assigns partitions, and any broker forwards requests to the right leader.
- Per-partition leader election, ISR tracking, and `acks=0|1|all`.
- Graceful shutdown that hands off leadership before exiting.
- Consumer groups with range and round-robin assignment, rebalancing, and committed offsets stored in an internal `__consumer_offsets` topic.
- Dead-letter topics for messages a consumer marks as poison.

## Quick start

Needs Go 1.25+. Docker is only needed for the cluster.

Standalone broker:

```bash
go run ./cmd/broker -port 8080 -data-dir ./data
```

Leave out `-data-dir` to keep everything in memory.

Three brokers plus etcd:

```bash
docker-compose up
```

Brokers listen on ports 8081–8083.

Create a topic, write to it, read it back:

```bash
curl -X POST localhost:8080/topics -d '{"name":"orders","partitions":3}'

curl -X POST 'localhost:8080/topics/orders/messages?acks=1' \
     -d '{"key":"user-42","value":"hello"}'

curl 'localhost:8080/topics/orders/messages?partition=0&offset=0'
```

## CLI

```bash
go build -o streamq-cli ./cmd/streamq-cli
```

```bash
B=http://localhost:8081

./streamq-cli topic create --broker $B --name orders --partitions 6 --replication-factor 3
./streamq-cli topic list --broker $B
./streamq-cli topic describe --broker $B --name orders

./streamq-cli produce --broker $B --topic orders --key user-42 --value "hello"
./streamq-cli consume --broker $B --topic orders --partition 0 --offset 0

./streamq-cli consume-group --broker $B --group order-processor --topic orders
./streamq-cli groups list --broker $B

# these two talk to etcd directly, not to a broker
./streamq-cli partitions list --etcd localhost:2379
./streamq-cli partitions reassign --etcd localhost:2379 --topic orders --partition 0 --to broker-2
```

`streamq-cli <command> --help` lists the flags for each command. `groups` also has `describe`, `offsets` and `dlq`.

Heads up: `partitions reassign` only rewrites the assignment in etcd. It doesn't copy data yet, and `--throttle` is a placeholder.

## Broker flags

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8080` | HTTP listen port |
| `-data-dir` | empty | Where to store logs. Empty means in-memory. |
| `-id` | empty | Broker ID. Required when `-etcd` is set. |
| `-advertise-addr` | `http://localhost:<port>` | URL other brokers use to reach this one |
| `-etcd` | empty | Comma-separated etcd endpoints. Setting this turns on cluster mode. |

## HTTP API

| Endpoint | Purpose |
|----------|---------|
| `POST /topics` | Create a topic. Body: `{name, partitions, replication_factor?}` |
| `GET /topics` | List topics. Add `?internal=true` to include `__*` topics. |
| `POST /topics/{name}/messages?acks=0\|1\|all` | Produce. Body: `{key, value}` |
| `GET /topics/{name}/messages?partition=N&offset=N&timeout=Ns&max=N` | Consume. Long-polls, and only returns messages below the high watermark. |
| `GET /metadata` | Cluster topology and the partition → broker map |
| `POST /groups/{group}/join` | Join a consumer group |
| `POST /groups/{group}/heartbeat` | Heartbeat |
| `POST /groups/{group}/leave` | Leave |
| `POST /groups/{group}/commit` | Commit offsets |
| `GET /groups/{group}/offsets?topic=<t>` | Read committed offsets |
| `POST /groups/{group}/dlq` | Send a message to the dead-letter topic |
| `GET /groups` | List groups on this broker |
| `GET /groups/{group}` | Describe one group |

Brokers also talk to each other over `/internal/replicate/{topic}/{partition}` (leader pushes to followers) and `/internal/follow/{topic}/{partition}?from=<n>` (follower catch-up). Both require an `X-StreamQ-Internal` header.

## How it works

### Cluster and controller

Every broker registers itself in etcd and watches the `/streamq/brokers/` prefix. One broker wins a lease-based election and becomes the controller. When a topic is created or a broker joins or leaves, the controller computes a consistent-hash replica assignment and writes it under `/streamq/assignments/`. All brokers watch that prefix, and a broker starts managing a partition when it sees itself assigned.

### Replication

Each partition's replicas hold their own election under `/streamq/partition-leaders/{topic}/{partition}/`. The leader assigns offsets, appends locally, then pushes the stamped messages to followers. A follower that falls behind is dropped from the ISR and catches up by polling the leader. Requests carry a leader epoch so writes from a stale leader are rejected.

### Layout

```text
cmd/
  broker/          broker entry point
  streamq-cli/     admin CLI
  bench/           produce/consume throughput benchmark

internal/
  broker/          HTTP server, handlers, replication runtime
  cluster/         etcd registration, elections, forwarding
  consumergroup/   coordinator, assignment strategies, offset store
  storage/         segmented log, index, CRC
  topic/           topic, partition and message types
  common/          typed errors, JSON helpers
  testutil/        in-process broker helpers for tests
```

### Storage format

A partition is a directory of `.sqlog` segment files, each with a matching `.sqidx` index. Segments roll at 10 MB. Every append is fsynced.

One message on disk:

```text
[4 bytes  length]
[8 bytes  offset]
[8 bytes  timestamp]
[4 bytes  key length]    [key]
[4 bytes  value length]  [value]
[4 bytes  CRC32]
```

Index files are written to a temp file, fsynced, then renamed into place, and they carry their own CRC. A half-written index is detected on startup.

## Tests

```bash
go test -race -count=1 ./...
```

Cluster tests need etcd on `localhost:2379` and skip themselves if it isn't there. `docker-compose up etcd` is enough.

There's also a throughput benchmark: `go run ./cmd/bench` (use `-url` to point it at a running broker).

## Limitations

Known gaps, mostly deliberate:

- **HTTP only.** No gRPC, and no zero-copy `sendfile` on the read path.
- **fsync on every append.** Safe but slow. Batching would help a lot.
- **No backpressure.** Under sustained overload the broker will run out of memory instead of rejecting requests.
- **No producer library.** Only the CLI.
- **At-least-once delivery.** No idempotent producer or transactions.
- **No compaction or retention.** Old messages are never deleted.
- **No real auth.** Internal endpoints use a shared token that defaults to `dev`.
- **Local disk only.** No tiered storage.

## License

See [LICENSE](LICENSE).
