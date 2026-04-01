package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Etcd key shape (mirrors internal/cluster):
//   /streamq/brokers/{id}                  -> BrokerInfo JSON
//   /streamq/assignments/{topic}/{part}    -> PartitionAssignment JSON
const (
	brokerPrefix     = "/streamq/brokers/"
	assignmentPrefix = "/streamq/assignments/"
)

// partitionAssignment matches cluster.PartitionAssignment. Re-declared here
// so the CLI doesn't depend on the internal cluster package (keeps the
// dependency surface small).
type partitionAssignment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	BrokerID  string `json:"broker_id"`
}

type brokerInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

func runPartitions(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: streamq-cli partitions <list|reassign> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		partitionsList(args[1:])
	case "reassign":
		partitionsReassign(args[1:])
	case "-h", "--help", "help":
		fmt.Println("partitions subcommands: list, reassign")
	default:
		fmt.Fprintf(os.Stderr, "unknown partitions subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func partitionsList(args []string) {
	fs := flag.NewFlagSet("partitions list", flag.ExitOnError)
	endpoints := fs.String("etcd", "localhost:2379", "comma-separated etcd endpoints")
	topicFilter := fs.String("topic", "", "filter by topic name")
	asJSON := fs.Bool("json", false, "emit raw JSON")
	_ = fs.Parse(args)

	client, err := dialEtcd(*endpoints)
	dieIf(err, "partitions list")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, assignmentPrefix, clientv3.WithPrefix())
	dieIf(err, "partitions list")

	var rows []partitionAssignment
	for _, kv := range resp.Kvs {
		var a partitionAssignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			continue
		}
		if *topicFilter != "" && a.Topic != *topicFilter {
			continue
		}
		rows = append(rows, a)
	}

	if *asJSON {
		out, _ := json.MarshalIndent(rows, "", "  ")
		fmt.Println(string(out))
		return
	}

	if len(rows) == 0 {
		fmt.Println("(no assignments)")
		return
	}

	// Sort by topic, then partition — makes diffing reassignments easier.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Topic != rows[j].Topic {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].Partition < rows[j].Partition
	})

	fmt.Printf("%-30s %-10s %s\n", "TOPIC", "PARTITION", "BROKER")
	for _, r := range rows {
		fmt.Printf("%-30s %-10d %s\n", r.Topic, r.Partition, r.BrokerID)
	}
}

func partitionsReassign(args []string) {
	fs := flag.NewFlagSet("partitions reassign", flag.ExitOnError)
	endpoints := fs.String("etcd", "localhost:2379", "comma-separated etcd endpoints")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", -1, "partition ID (required)")
	toBroker := fs.String("to", "", "destination broker ID (required)")
	throttle := fs.Int("throttle", 0, "migration throttle in MB/s (stub for now)")
	_ = fs.Parse(args)

	if *topic == "" || *partition < 0 || *toBroker == "" {
		fmt.Fprintln(os.Stderr, "partitions reassign: --topic, --partition, --to are required")
		os.Exit(2)
	}

	client, err := dialEtcd(*endpoints)
	dieIf(err, "partitions reassign")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Validate the destination broker actually exists in /streamq/brokers/.
	// Without this you can silently park a partition on a broker ID that
	// nobody is running, and produce/consume to it will fail in confusing
	// ways.
	brokerKey := brokerPrefix + *toBroker
	bresp, err := client.Get(ctx, brokerKey)
	dieIf(err, "partitions reassign: lookup broker")
	if len(bresp.Kvs) == 0 {
		fmt.Fprintf(os.Stderr, "partitions reassign: destination broker %q is not registered in etcd (key %s missing)\n", *toBroker, brokerKey)
		os.Exit(1)
	}
	var dest brokerInfo
	_ = json.Unmarshal(bresp.Kvs[0].Value, &dest)

	// Fetch the current assignment so we can show the diff.
	key := fmt.Sprintf("%s%s/%d", assignmentPrefix, *topic, *partition)
	cur, err := client.Get(ctx, key)
	dieIf(err, "partitions reassign: lookup current assignment")
	if len(cur.Kvs) == 0 {
		fmt.Fprintf(os.Stderr, "partitions reassign: no current assignment for %s/%d\n", *topic, *partition)
		os.Exit(1)
	}
	var curAsg partitionAssignment
	_ = json.Unmarshal(cur.Kvs[0].Value, &curAsg)

	if curAsg.BrokerID == *toBroker {
		fmt.Printf("partition %s/%d is already on broker %s — nothing to do\n", *topic, *partition, *toBroker)
		return
	}

	// Throttle stub. Real migration would stream the existing partition's
	// log segments from the source broker to the destination, applying
	// the throttle to cap network usage during a live rebalance. We don't
	// have replication yet (Phase 4) so right now this CLI just overwrites
	// the assignment key — the destination broker will materialize an
	// empty partition and the data on the old broker becomes orphaned
	// until cleanup. That's a known limitation documented here so we
	// don't ship this expecting it to do more than it does.
	//
	// TODO(phase-4): once replication exists, copy segments from the old
	// owner to the new owner before flipping the assignment key, and
	// respect --throttle for the data copy.
	if *throttle > 0 {
		fmt.Printf("would migrate ~%d MB/s (throttle stub — no data copy in Phase 3)\n", *throttle)
	} else {
		fmt.Println("no throttle set — would migrate at unlimited rate (stub)")
	}

	next := partitionAssignment{
		Topic:     *topic,
		Partition: *partition,
		BrokerID:  *toBroker,
	}
	data, _ := json.Marshal(next)
	if _, err := client.Put(ctx, key, string(data)); err != nil {
		fmt.Fprintf(os.Stderr, "partitions reassign: write etcd: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("reassigned %s/%d: %s -> %s\n", *topic, *partition, curAsg.BrokerID, *toBroker)
	if dest.Addr != "" {
		fmt.Printf("destination broker addr: %s\n", dest.Addr)
	}
	fmt.Println("note: partition data on the old broker is NOT copied (Phase 3 limitation)")
}

// dialEtcd opens a client against the given comma-separated endpoints.
// Fails the command (rather than hanging) if etcd doesn't respond promptly.
func dialEtcd(commaEndpoints string) (*clientv3.Client, error) {
	endpoints := strings.Split(commaEndpoints, ",")
	for i, e := range endpoints {
		endpoints[i] = strings.TrimSpace(e)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to etcd %v: %w", endpoints, err)
	}
	// Sanity ping — clientv3.New doesn't actually verify connectivity.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, endpoints[0]); err != nil {
		client.Close()
		return nil, fmt.Errorf("etcd unreachable at %v: %w", endpoints, err)
	}
	return client, nil
}
