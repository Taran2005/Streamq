package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
)

// runGroups dispatches the `groups` family of subcommands.
func runGroups(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: streamq-cli groups <list|describe|offsets|dlq> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		groupsList(args[1:])
	case "describe":
		groupsDescribe(args[1:])
	case "offsets":
		groupsOffsets(args[1:])
	case "dlq":
		groupsDLQ(args[1:])
	case "-h", "--help", "help":
		fmt.Println("groups subcommands: list, describe, offsets, dlq")
	default:
		fmt.Fprintf(os.Stderr, "unknown groups subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func groupsList(args []string) {
	fs := flag.NewFlagSet("groups list", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	asJSON := fs.Bool("json", false, "emit raw JSON")
	_ = fs.Parse(args)

	resp, err := http.Get(*broker + "/groups")
	dieIf(err, "groups list")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "groups list: %d %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	var body struct {
		Groups []struct {
			Name       string   `json:"name"`
			Members    int      `json:"members"`
			Topics     []string `json:"topics"`
			Generation int      `json:"generation"`
			State      string   `json:"state"`
		} `json:"groups"`
	}
	_ = json.Unmarshal(raw, &body)
	if len(body.Groups) == 0 {
		fmt.Println("(no groups)")
		return
	}
	sort.Slice(body.Groups, func(i, j int) bool { return body.Groups[i].Name < body.Groups[j].Name })
	fmt.Printf("%-20s %-10s %-6s %-12s %s\n", "NAME", "STATE", "GEN", "MEMBERS", "TOPICS")
	for _, g := range body.Groups {
		fmt.Printf("%-20s %-10s %-6d %-12d %s\n", g.Name, g.State, g.Generation, g.Members, strings.Join(g.Topics, ","))
	}
}

func groupsDescribe(args []string) {
	fs := flag.NewFlagSet("groups describe", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	name := fs.String("group", "", "group name (required)")
	asJSON := fs.Bool("json", false, "emit raw JSON")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintln(os.Stderr, "groups describe: --group is required")
		os.Exit(2)
	}

	resp, err := http.Get(*broker + "/groups/" + url.PathEscape(*name))
	dieIf(err, "groups describe")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "groups describe: %d %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	var body struct {
		Name       string   `json:"name"`
		State      string   `json:"state"`
		Generation int      `json:"generation"`
		Strategy   string   `json:"strategy"`
		Topics     []string `json:"topics"`
		Members    []struct {
			ID         string `json:"id"`
			Topics     []string `json:"topics"`
			Assignment []struct {
				Topic      string `json:"topic"`
				Partitions []int  `json:"partitions"`
			} `json:"assignment"`
		} `json:"members"`
	}
	_ = json.Unmarshal(raw, &body)

	fmt.Printf("group:      %s\n", body.Name)
	fmt.Printf("state:      %s\n", body.State)
	fmt.Printf("generation: %d\n", body.Generation)
	fmt.Printf("strategy:   %s\n", body.Strategy)
	fmt.Printf("topics:     %s\n", strings.Join(body.Topics, ","))
	fmt.Printf("members:    %d\n", len(body.Members))
	for _, m := range body.Members {
		fmt.Printf("  - %s\n", m.ID)
		for _, a := range m.Assignment {
			parts := make([]string, len(a.Partitions))
			for i, p := range a.Partitions {
				parts[i] = fmt.Sprintf("%d", p)
			}
			fmt.Printf("      %s: [%s]\n", a.Topic, strings.Join(parts, ","))
		}
	}
}

func groupsOffsets(args []string) {
	fs := flag.NewFlagSet("groups offsets", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	name := fs.String("group", "", "group name (required)")
	topic := fs.String("topic", "", "filter by topic name (optional)")
	asJSON := fs.Bool("json", false, "emit raw JSON")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintln(os.Stderr, "groups offsets: --group is required")
		os.Exit(2)
	}

	u := *broker + "/groups/" + url.PathEscape(*name) + "/offsets"
	if *topic != "" {
		u += "?topic=" + url.QueryEscape(*topic)
	}
	resp, err := http.Get(u)
	dieIf(err, "groups offsets")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "groups offsets: %d %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	var body struct {
		Offsets []struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    uint64 `json:"committed_offset"`
		} `json:"offsets"`
	}
	_ = json.Unmarshal(raw, &body)

	if len(body.Offsets) == 0 {
		fmt.Println("(no offsets)")
		return
	}
	sort.Slice(body.Offsets, func(i, j int) bool {
		if body.Offsets[i].Topic != body.Offsets[j].Topic {
			return body.Offsets[i].Topic < body.Offsets[j].Topic
		}
		return body.Offsets[i].Partition < body.Offsets[j].Partition
	})
	fmt.Printf("%-30s %-10s %s\n", "TOPIC", "PARTITION", "OFFSET")
	for _, r := range body.Offsets {
		fmt.Printf("%-30s %-10d %d\n", r.Topic, r.Partition, r.Offset)
	}
}

func groupsDLQ(args []string) {
	fs := flag.NewFlagSet("groups dlq", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	groupName := fs.String("group", "", "group name (required)")
	topic := fs.String("topic", "", "source topic name (required)")
	partition := fs.Int("partition", -1, "source partition (required)")
	offset := fs.Uint64("offset", 0, "source offset (required)")
	reason := fs.String("reason", "unspecified", "reason this message is being dead-lettered")
	_ = fs.Parse(args)

	if *groupName == "" || *topic == "" || *partition < 0 {
		fmt.Fprintln(os.Stderr, "groups dlq: --group, --topic, --partition are required")
		os.Exit(2)
	}

	body, _ := json.Marshal(map[string]any{
		"topic":     *topic,
		"partition": *partition,
		"offset":    *offset,
		"reason":    *reason,
	})
	resp, err := httpPost(*broker+"/groups/"+url.PathEscape(*groupName)+"/dlq", body)
	dieIf(err, "groups dlq")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "groups dlq: %d %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}

	var out struct {
		DLQTopic     string `json:"dlq_topic"`
		DLQPartition int    `json:"dlq_partition"`
		DLQOffset    uint64 `json:"dlq_offset"`
	}
	_ = json.Unmarshal(raw, &out)
	fmt.Printf("dlq'd %s/%d@%d -> %s/%d@%d (reason: %s)\n",
		*topic, *partition, *offset, out.DLQTopic, out.DLQPartition, out.DLQOffset, *reason)
}
