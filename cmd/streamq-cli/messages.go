package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func runProduce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	topic := fs.String("topic", "", "topic name (required)")
	key := fs.String("key", "", "message key (optional, controls partition)")
	value := fs.String("value", "", "message value (required)")
	asJSON := fs.Bool("json", false, "emit raw JSON response")
	_ = fs.Parse(args)

	if *topic == "" || *value == "" {
		fmt.Fprintln(os.Stderr, "produce: --topic and --value are required")
		os.Exit(2)
	}

	body, _ := json.Marshal(map[string]any{"key": *key, "value": *value})
	resp, err := httpPost(*broker+"/topics/"+url.PathEscape(*topic)+"/messages", body)
	dieIf(err, "produce")
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "produce: broker returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	var out struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}
	_ = json.Unmarshal(raw, &out)
	fmt.Printf("produced to %s/%d at offset %d\n", out.Topic, out.Partition, out.Offset)
}

func runConsume(args []string) {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	broker := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", -1, "partition ID (required)")
	offset := fs.Uint64("offset", 0, "starting offset")
	timeout := fs.String("timeout", "", "long-poll timeout (e.g. 5s)")
	max := fs.Int("max", 100, "max messages to return")
	asJSON := fs.Bool("json", false, "emit raw JSON response")
	_ = fs.Parse(args)

	if *topic == "" || *partition < 0 {
		fmt.Fprintln(os.Stderr, "consume: --topic and --partition are required")
		os.Exit(2)
	}

	q := url.Values{}
	q.Set("partition", fmt.Sprintf("%d", *partition))
	q.Set("offset", fmt.Sprintf("%d", *offset))
	q.Set("max", fmt.Sprintf("%d", *max))
	if *timeout != "" {
		q.Set("timeout", *timeout)
	}

	resp, err := http.Get(*broker + "/topics/" + url.PathEscape(*topic) + "/messages?" + q.Encode())
	dieIf(err, "consume")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "consume: broker returned %d: %s\n", resp.StatusCode, strings.TrimSpace(string(raw)))
		os.Exit(1)
	}
	if *asJSON {
		fmt.Println(strings.TrimSpace(string(raw)))
		return
	}

	// Generic decode — message schema is owned by the broker, but the
	// fields we care about are always present.
	var out struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		fmt.Fprintf(os.Stderr, "consume: decode: %v\n", err)
		os.Exit(1)
	}

	if len(out.Messages) == 0 {
		fmt.Println("(no messages)")
		return
	}
	for _, m := range out.Messages {
		// best-effort field grabbing — partition log records use
		// offset/key/value/timestamp/partition.
		off, _ := m["offset"].(float64)
		key, _ := m["key"].(string)
		val, _ := m["value"].(string)
		fmt.Printf("offset=%d key=%q value=%q\n", uint64(off), key, val)
	}
}
