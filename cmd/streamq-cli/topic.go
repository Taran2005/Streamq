package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// runTopic dispatches `topic <sub>` to the matching handler.
func runTopic(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: streamq-cli topic <create|list|describe> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		topicCreate(args[1:])
	case "list":
		topicList(args[1:])
	case "describe":
		topicDescribe(args[1:])
	case "-h", "--help", "help":
		fmt.Println("topic subcommands: create, list, describe")
	default:
		fmt.Fprintf(os.Stderr, "unknown topic subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func topicCreate(args []string) {
	fs := flag.NewFlagSet("topic create", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	name := fs.String("name", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "number of partitions")
	asJSON := fs.Bool("json", false, "emit raw JSON response")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintln(os.Stderr, "topic create: --name is required")
		os.Exit(2)
	}

	body, _ := json.Marshal(map[string]any{"name": *name, "partitions": *partitions})
	resp, err := httpPost(*broker+"/topics", body)
	dieIf(err, "topic create")
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "topic create: broker returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}
	fmt.Printf("created topic %q with %d partition(s)\n", *name, *partitions)
}

func topicList(args []string) {
	fs := flag.NewFlagSet("topic list", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	asJSON := fs.Bool("json", false, "emit raw JSON response")
	_ = fs.Parse(args)

	resp, err := http.Get(*broker + "/topics")
	dieIf(err, "topic list")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "topic list: broker returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	var parsed struct {
		Topics []struct {
			Name           string `json:"name"`
			PartitionCount int    `json:"partition_count"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		fmt.Fprintf(os.Stderr, "topic list: decode: %v\n", err)
		os.Exit(1)
	}
	if len(parsed.Topics) == 0 {
		fmt.Println("(no topics)")
		return
	}
	// Sort by name so output is stable across runs.
	sort.Slice(parsed.Topics, func(i, j int) bool {
		return parsed.Topics[i].Name < parsed.Topics[j].Name
	})
	fmt.Printf("%-30s %s\n", "NAME", "PARTITIONS")
	for _, t := range parsed.Topics {
		fmt.Printf("%-30s %d\n", t.Name, t.PartitionCount)
	}
}

func topicDescribe(args []string) {
	fs := flag.NewFlagSet("topic describe", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	name := fs.String("name", "", "topic name (required)")
	asJSON := fs.Bool("json", false, "emit raw JSON response")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintln(os.Stderr, "topic describe: --name is required")
		os.Exit(2)
	}

	resp, err := http.Get(*broker + "/metadata")
	dieIf(err, "topic describe")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "topic describe: broker returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	// Metadata shape:
	//   { "mode": "...", "broker": "...", "controller": bool,
	//     "brokers": [{id, addr}, ...],
	//     "topics": { "<name>": { "<partition>": "<brokerID>", ... } } }
	var md struct {
		Mode    string                       `json:"mode"`
		Broker  string                       `json:"broker"`
		Brokers []map[string]string          `json:"brokers"`
		Topics  map[string]map[string]string `json:"topics"`
	}
	if err := json.Unmarshal(raw, &md); err != nil {
		fmt.Fprintf(os.Stderr, "topic describe: decode metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("topic: %s\n", *name)
	fmt.Printf("mode:  %s\n", md.Mode)

	if md.Mode == "standalone" {
		// Standalone mode doesn't expose partition->broker mapping
		// through /metadata. Fall back to /topics to at least show
		// the partition count.
		resp2, err := http.Get(*broker + "/topics")
		dieIf(err, "topic describe (fallback)")
		defer resp2.Body.Close()
		raw2, _ := io.ReadAll(resp2.Body)
		var tlist struct {
			Topics []struct {
				Name           string `json:"name"`
				PartitionCount int    `json:"partition_count"`
			} `json:"topics"`
		}
		_ = json.Unmarshal(raw2, &tlist)
		for _, t := range tlist.Topics {
			if t.Name == *name {
				fmt.Printf("partitions: %d (all owned by broker %q)\n", t.PartitionCount, md.Broker)
				return
			}
		}
		fmt.Fprintf(os.Stderr, "topic describe: topic %q not found on broker\n", *name)
		os.Exit(1)
	}

	parts, ok := md.Topics[*name]
	if !ok {
		fmt.Fprintf(os.Stderr, "topic describe: topic %q not found in cluster metadata\n", *name)
		os.Exit(1)
	}

	// Sort partition IDs numerically for a tidy table.
	ids := make([]int, 0, len(parts))
	for k := range parts {
		n, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		ids = append(ids, n)
	}
	sort.Ints(ids)

	fmt.Printf("partitions: %d\n", len(ids))
	fmt.Printf("%-12s %s\n", "PARTITION", "BROKER")
	for _, id := range ids {
		fmt.Printf("%-12d %s\n", id, parts[strconv.Itoa(id)])
	}
}

// httpPost is a tiny helper because we POST JSON to a few places.
func httpPost(url string, body []byte) (*http.Response, error) {
	c := &http.Client{Timeout: 10 * time.Second}
	return c.Post(url, "application/json", bytes.NewReader(body))
}
