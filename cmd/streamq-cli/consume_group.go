package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// runConsumeGroup is the `streamq-cli consume-group` subcommand. It joins a
// consumer group, fetches messages from the partitions it's assigned, and
// commits offsets after printing — the at-least-once delivery pattern.
//
// Flow per iteration:
//  1. Sleep until ctx cancel or a polling tick.
//  2. For each assigned (topic, partition), fetch starting at the committed
//     offset (or 0 if no commit yet).
//  3. Print every message; track the new highwater per partition.
//  4. POST /groups/{group}/commit with the new offsets.
//  5. Heartbeat in a background goroutine. On 426 (rebalance/generation
//     mismatch) we re-join transparently.
//  6. On SIGINT, send /leave and exit.
func runConsumeGroup(args []string) {
	fs := flag.NewFlagSet("consume-group", flag.ExitOnError)
	brokerURL := fs.String("broker", "http://localhost:8080", "broker HTTP URL")
	groupName := fs.String("group", "", "consumer group name (required)")
	topicsArg := fs.String("topic", "", "comma-separated topic list (required)")
	strategy := fs.String("strategy", "range", "assignment strategy: range | round-robin")
	sessionMs := fs.Int("session-ms", 30_000, "session timeout in ms")
	pollMs := fs.Int("poll-ms", 500, "wait between fetch rounds when nothing arrives")
	maxMsgs := fs.Int("max", 100, "max messages per fetch call")
	asJSON := fs.Bool("json", false, "emit raw JSON per message")
	_ = fs.Parse(args)

	if *groupName == "" || *topicsArg == "" {
		fmt.Fprintln(os.Stderr, "consume-group: --group and --topic are required")
		os.Exit(2)
	}
	topics := splitNonEmpty(*topicsArg, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT/TERM gets us to clean shutdown (which calls /leave).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "consume-group: shutting down...")
		cancel()
	}()

	client := &groupClient{
		broker:    *brokerURL,
		group:     *groupName,
		topics:    topics,
		strategy:  *strategy,
		sessionMs: *sessionMs,
		maxMsgs:   *maxMsgs,
		asJSON:    *asJSON,
	}

	if err := client.join(); err != nil {
		dieIf(err, "consume-group: join")
	}
	defer client.leave()

	// Background heartbeat goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.heartbeatLoop(ctx)
	}()

	pollDur := time.Duration(*pollMs) * time.Millisecond
	committed := make(map[string]map[int]uint64) // topic → partition → next-to-read

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		default:
		}

		// Refresh assignment if a rebalance was signaled.
		if client.needsRejoin() {
			if err := client.join(); err != nil {
				fmt.Fprintf(os.Stderr, "consume-group: re-join: %v\n", err)
				time.Sleep(time.Second)
				continue
			}
		}

		got := 0
		commits := []map[string]any{}
		for _, asg := range client.assignment() {
			for _, partition := range asg.Partitions {
				topic := asg.Topic
				if _, ok := committed[topic]; !ok {
					committed[topic] = make(map[int]uint64)
				}
				offset := committed[topic][partition]
				if off, ok := client.lookupOffset(topic, partition); ok {
					if off+1 > offset {
						offset = off + 1
					}
				}
				msgs, err := fetchMessages(*brokerURL, topic, partition, offset, *maxMsgs)
				if err != nil {
					fmt.Fprintf(os.Stderr, "consume-group: fetch %s/%d: %v\n", topic, partition, err)
					continue
				}
				if len(msgs) == 0 {
					continue
				}

				var maxOff uint64
				for _, m := range msgs {
					printMessage(m, *asJSON)
					if m.Offset > maxOff {
						maxOff = m.Offset
					}
					got++
				}
				committed[topic][partition] = maxOff + 1
				commits = append(commits, map[string]any{
					"topic":     topic,
					"partition": partition,
					"offset":    maxOff,
				})
			}
		}

		if len(commits) > 0 {
			if err := client.commit(commits); err != nil {
				fmt.Fprintf(os.Stderr, "consume-group: commit: %v\n", err)
				if errors.Is(err, errNeedsRejoin) {
					client.markNeedsRejoin()
				}
			}
		}

		if got == 0 {
			// Nothing this round — backoff a bit so we don't spam the broker.
			select {
			case <-ctx.Done():
			case <-time.After(pollDur):
			}
		}
	}
}

// errNeedsRejoin is returned when the broker tells us our generation is stale
// (HTTP 426 Upgrade Required). The caller schedules a re-join on the next
// loop iteration.
var errNeedsRejoin = errors.New("rejoin required")

// groupClient is the shared state between the main loop and the heartbeat
// goroutine. All mutating fields are protected by mu.
type groupClient struct {
	broker    string
	group     string
	topics    []string
	strategy  string
	sessionMs int
	maxMsgs   int
	asJSON    bool

	mu          sync.Mutex
	memberID    string
	generation  int
	asg         []asgEntry
	rebalanceCh bool

	// committed mirrors the broker's offset store for the rows we care about,
	// so the consume loop knows where to resume after a re-join.
	committed map[string]map[int]uint64
}

type asgEntry struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

func (c *groupClient) needsRejoin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rebalanceCh || c.memberID == ""
}

func (c *groupClient) markNeedsRejoin() {
	c.mu.Lock()
	c.rebalanceCh = true
	c.mu.Unlock()
}

func (c *groupClient) assignment() []asgEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]asgEntry(nil), c.asg...)
}

func (c *groupClient) lookupOffset(topic string, partition int) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.committed[topic]; ok {
		off, ok := m[partition]
		return off, ok
	}
	return 0, false
}

// join performs the initial join (member_id empty) or a re-join (member_id
// preserved). Blocks until it either succeeds or hits a hard error.
func (c *groupClient) join() error {
	c.mu.Lock()
	memberID := c.memberID
	c.mu.Unlock()

	body, _ := json.Marshal(map[string]any{
		"member_id":          memberID,
		"topics":             c.topics,
		"strategy":           c.strategy,
		"session_timeout_ms": c.sessionMs,
	})

	resp, err := httpPost(c.broker+"/groups/"+url.PathEscape(c.group)+"/join", body)
	if err != nil {
		return fmt.Errorf("post join: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("join: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var jr struct {
		MemberID            string     `json:"member_id"`
		Generation          int        `json:"generation"`
		Assignment          []asgEntry `json:"assignment"`
		RebalanceInProgress bool       `json:"rebalance_in_progress"`
	}
	if err := json.Unmarshal(raw, &jr); err != nil {
		return fmt.Errorf("decode join response: %w", err)
	}

	c.mu.Lock()
	c.memberID = jr.MemberID
	c.generation = jr.Generation
	c.asg = jr.Assignment
	c.rebalanceCh = jr.RebalanceInProgress
	if c.committed == nil {
		c.committed = make(map[string]map[int]uint64)
	}
	c.mu.Unlock()

	// Best-effort backfill of committed offsets from the broker so we resume
	// where we left off after a crash + restart.
	c.refreshCommittedOffsets()

	fmt.Fprintf(os.Stderr, "consume-group: joined %s as %s (gen=%d), assignment=%v\n",
		c.group, jr.MemberID, jr.Generation, jr.Assignment)
	return nil
}

func (c *groupClient) commit(commits []map[string]any) error {
	c.mu.Lock()
	memberID := c.memberID
	generation := c.generation
	c.mu.Unlock()

	body, _ := json.Marshal(map[string]any{
		"member_id":  memberID,
		"generation": generation,
		"commits":    commits,
	})
	resp, err := httpPost(c.broker+"/groups/"+url.PathEscape(c.group)+"/commit", body)
	if err != nil {
		return fmt.Errorf("post commit: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUpgradeRequired {
		return errNeedsRejoin
	}
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	// Track locally so a quick restart in this process doesn't redeliver.
	c.mu.Lock()
	for _, c0 := range commits {
		topic, _ := c0["topic"].(string)
		partition := asInt(c0["partition"])
		offset := asUint64(c0["offset"])
		if topic == "" {
			continue
		}
		if _, ok := c.committed[topic]; !ok {
			c.committed[topic] = make(map[int]uint64)
		}
		c.committed[topic][partition] = offset
	}
	c.mu.Unlock()
	return nil
}

// asInt unwraps the various numeric types we end up with from map[string]any
// payloads (int from the local builder, float64 from JSON round-trips).
func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case uint64:
		return int(x)
	case float64:
		return int(x)
	default:
		return 0
	}
}

func asUint64(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case int64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	default:
		return 0
	}
}

func (c *groupClient) leave() {
	c.mu.Lock()
	memberID := c.memberID
	c.mu.Unlock()
	if memberID == "" {
		return
	}
	body, _ := json.Marshal(map[string]string{"member_id": memberID})
	resp, err := httpPost(c.broker+"/groups/"+url.PathEscape(c.group)+"/leave", body)
	if err == nil {
		resp.Body.Close()
	}
}

// heartbeatLoop fires every ~5s. On 426 it marks needsRejoin so the next loop
// iteration triggers a re-join.
func (c *groupClient) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		memberID := c.memberID
		generation := c.generation
		c.mu.Unlock()
		if memberID == "" {
			continue
		}
		body, _ := json.Marshal(map[string]any{
			"member_id":  memberID,
			"generation": generation,
		})
		resp, err := httpPost(c.broker+"/groups/"+url.PathEscape(c.group)+"/heartbeat", body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "consume-group: heartbeat: %v\n", err)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUpgradeRequired {
			c.markNeedsRejoin()
			continue
		}
		var hr struct {
			RebalanceRequired bool `json:"rebalance_required"`
		}
		_ = json.Unmarshal(raw, &hr)
		if hr.RebalanceRequired {
			c.markNeedsRejoin()
		}
	}
}

// refreshCommittedOffsets pulls /groups/{group}/offsets for every subscribed
// topic and caches it locally so the consume loop knows where to resume.
func (c *groupClient) refreshCommittedOffsets() {
	for _, topic := range c.topics {
		u := fmt.Sprintf("%s/groups/%s/offsets?topic=%s", c.broker, url.PathEscape(c.group), url.QueryEscape(topic))
		resp, err := http.Get(u)
		if err != nil {
			continue
		}
		var body struct {
			Offsets []struct {
				Topic     string `json:"topic"`
				Partition int    `json:"partition"`
				Offset    uint64 `json:"committed_offset"`
			} `json:"offsets"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()

		c.mu.Lock()
		for _, row := range body.Offsets {
			if _, ok := c.committed[row.Topic]; !ok {
				c.committed[row.Topic] = make(map[int]uint64)
			}
			c.committed[row.Topic][row.Partition] = row.Offset
		}
		c.mu.Unlock()
	}
}

// printMessage emits one record. JSON mode dumps the raw line; otherwise we
// print a compact one-liner that matches `consume`'s default output.
func printMessage(m fetchedMessage, asJSON bool) {
	if asJSON {
		raw, _ := json.Marshal(m)
		fmt.Println(string(raw))
		return
	}
	fmt.Printf("topic=%s partition=%d offset=%d key=%q value=%q\n",
		m.Topic, m.Partition, m.Offset, m.Key, m.Value)
}

type fetchedMessage struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

func fetchMessages(brokerURL, topic string, partition int, offset uint64, max int) ([]fetchedMessage, error) {
	q := url.Values{}
	q.Set("partition", fmt.Sprintf("%d", partition))
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("max", fmt.Sprintf("%d", max))

	u := fmt.Sprintf("%s/topics/%s/messages?%s", brokerURL, url.PathEscape(topic), q.Encode())
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fetch: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var body struct {
		Messages []fetchedMessage `json:"messages"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	// Stamp the topic in — the server doesn't include it in each message.
	for i := range body.Messages {
		body.Messages[i].Topic = topic
	}
	return body.Messages, nil
}

func splitNonEmpty(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

