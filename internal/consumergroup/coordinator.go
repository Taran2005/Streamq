package consumergroup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// TopicResolver is the broker-side primitive the coordinator uses to look up
// how many partitions a subscribed topic has. The coordinator deliberately
// doesn't know about broker.Broker — keeps the package decoupled.
type TopicResolver interface {
	PartitionCount(topic string) (int, bool)
}

// OffsetWriter persists a commit to the __consumer_offsets topic. The broker
// implements this by appending to the appropriate partition (and replicating
// it like any other message). Returns the appended offset.
type OffsetWriter interface {
	WriteCommit(ctx context.Context, key string, value []byte) (uint64, error)
}

// Errors surfaced to HTTP handlers. The server translates each to a status
// code (see broker.handler_groups.go).
var (
	ErrUnknownGroup       = errors.New("unknown group")
	ErrUnknownMember      = errors.New("unknown member")
	ErrGenerationMismatch = errors.New("generation mismatch")
	ErrRebalanceInProgress = errors.New("rebalance in progress")
)

// defaultSessionTimeout is used when the client doesn't supply one.
const defaultSessionTimeout = 30 * time.Second

// rebalanceGrace bounds how long the coordinator waits for all known members
// to re-join before forcing the new generation. Members that miss this window
// are simply excluded from the new assignment.
const rebalanceGrace = 10 * time.Second

// Coordinator is the in-memory authority for groups whose offsets-topic
// partition is owned by this broker. Methods are safe for concurrent use.
//
// State machine recap:
//
//	Empty ── Join ──► Rebalancing ── all-members-rejoin ──► Stable
//	                                                       │
//	   ▲                                                   │
//	   └──────── all-leave ────── Leave/timeout ◄──────────┘
type Coordinator struct {
	resolver TopicResolver
	writer   OffsetWriter
	store    *Store
	clock    func() time.Time

	mu     sync.Mutex
	groups map[string]*Group

	// done is closed once Stop returns so background loops can join.
	stopCh chan struct{}
	once   sync.Once
}

// NewCoordinator builds a coordinator. The session-monitor loop starts
// immediately on a freshly constructed coordinator; call Stop to halt it.
func NewCoordinator(resolver TopicResolver, writer OffsetWriter, store *Store) *Coordinator {
	c := &Coordinator{
		resolver: resolver,
		writer:   writer,
		store:    store,
		clock:    time.Now,
		groups:   make(map[string]*Group),
		stopCh:   make(chan struct{}),
	}
	go c.sessionLoop()
	return c
}

// Stop halts background loops. Safe to call multiple times.
func (c *Coordinator) Stop() {
	c.once.Do(func() { close(c.stopCh) })
}

// JoinResult is what /groups/{group}/join returns to a client.
type JoinResult struct {
	MemberID            string       `json:"member_id"`
	Generation          int          `json:"generation"`
	Assignment          []Assignment `json:"assignment"`
	RebalanceInProgress bool         `json:"rebalance_in_progress"`
	Strategy            string       `json:"strategy"`
}

// JoinRequest is the input to Join.
type JoinRequest struct {
	GroupName        string
	MemberID         string // empty → coordinator assigns a fresh UUID-ish ID
	Topics           []string
	SessionTimeoutMs int
	Strategy         string // "range" or "round-robin"; defaults to range
}

// Join is the entry point for a member joining or re-joining the group.
//
// Behavior:
//   - If the group doesn't exist, it's created.
//   - If member_id is empty, a new ID is assigned and a rebalance is triggered.
//   - If member_id is known but rebalance is in progress, this counts as a
//     rejoin — once every known member rejoins, the assignment is finalized.
//   - If the group is Stable and a known member rejoins (e.g. subscription
//     change), a new rebalance is triggered.
//   - If the group is Stable and an unknown member rejoins, that triggers a
//     rebalance too — new member joined.
func (c *Coordinator) Join(ctx context.Context, req JoinRequest) (JoinResult, error) {
	if req.GroupName == "" {
		return JoinResult{}, fmt.Errorf("group name required")
	}
	if len(req.Topics) == 0 {
		return JoinResult{}, fmt.Errorf("at least one topic required")
	}

	c.mu.Lock()
	g, ok := c.groups[req.GroupName]
	if !ok {
		g = newGroup(req.GroupName)
		c.groups[req.GroupName] = g
	}
	c.mu.Unlock()

	g.mu.Lock()
	defer g.mu.Unlock()

	memberID := req.MemberID
	isNew := memberID == ""
	if isNew {
		memberID = generateMemberID()
	}

	session := time.Duration(req.SessionTimeoutMs) * time.Millisecond
	if session <= 0 {
		session = defaultSessionTimeout
	}

	now := c.clock()
	prev, existing := g.Members[memberID]

	// Determine if a rebalance needs to fire.
	rebalanceNeeded := false
	switch {
	case isNew, !existing:
		// Brand-new member.
		rebalanceNeeded = true
		g.Members[memberID] = &Member{
			ID:            memberID,
			Topics:        append([]string(nil), req.Topics...),
			SessionMs:     int(session / time.Millisecond),
			LastHeartbeat: now,
			JoinedAt:      now,
			Strategy:      req.Strategy,
		}
	case existing && !topicsEqual(prev.Topics, req.Topics):
		// Same member, different subscription → rebalance.
		rebalanceNeeded = true
		prev.Topics = append([]string(nil), req.Topics...)
		prev.SessionMs = int(session / time.Millisecond)
		prev.LastHeartbeat = now
		prev.Strategy = req.Strategy
	default:
		// Known member, same subscription. If we're rebalancing, count this
		// as a rejoin; otherwise it's a no-op refresh.
		prev.LastHeartbeat = now
		prev.SessionMs = int(session / time.Millisecond)
	}

	if rebalanceNeeded && g.State != Rebalancing {
		c.startRebalanceLocked(g, req.Strategy)
	} else if g.State == Empty && len(g.Members) > 0 {
		c.startRebalanceLocked(g, req.Strategy)
	}

	g.markRejoined(memberID)

	// If every known member has rejoined, finalize now.
	if g.State == Rebalancing && g.allRejoined() {
		c.finalizeLocked(g)
	}

	res := JoinResult{
		MemberID:            memberID,
		Generation:          g.Generation,
		Assignment:          append([]Assignment(nil), g.Assignment[memberID]...),
		RebalanceInProgress: g.State == Rebalancing,
		Strategy:            g.StrategyName,
	}
	return res, nil
}

// Heartbeat updates the member's last-seen timestamp and reports whether they
// need to re-join (because a rebalance is in progress) or whether their
// generation is stale.
func (c *Coordinator) Heartbeat(group, memberID string, generation int) (rebalanceRequired bool, err error) {
	c.mu.Lock()
	g, ok := c.groups[group]
	c.mu.Unlock()
	if !ok {
		return false, ErrUnknownGroup
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.Members[memberID]
	if !ok {
		return false, ErrUnknownMember
	}
	if g.State == Stable && generation != g.Generation {
		return true, ErrGenerationMismatch
	}
	m.LastHeartbeat = c.clock()
	return g.State == Rebalancing, nil
}

// Leave removes a member. If the group becomes empty it stays around (with
// state=Empty) so future joins reuse the existing entry. If there are still
// members, a rebalance is triggered.
func (c *Coordinator) Leave(group, memberID string) error {
	c.mu.Lock()
	g, ok := c.groups[group]
	c.mu.Unlock()
	if !ok {
		return ErrUnknownGroup
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.Members[memberID]; !ok {
		return ErrUnknownMember
	}
	delete(g.Members, memberID)
	delete(g.Assignment, memberID)

	if len(g.Members) == 0 {
		g.State = Empty
		g.Assignment = make(map[string][]Assignment)
		g.rejoined = make(map[string]struct{})
		return nil
	}
	c.startRebalanceLocked(g, g.StrategyName)
	// If only one member is left and they've already (silently) rejoined
	// in spirit, we'll finalize on their next heartbeat that returns
	// rebalance_required=true. We don't preemptively finalize here because
	// the remaining member still needs to re-join with up-to-date topics.
	return nil
}

// CommitOffsets durably writes commits via the OffsetWriter and updates the
// in-memory store. Each commit becomes one message in __consumer_offsets.
//
// Generation check: if memberID isn't the current generation, returns
// ErrGenerationMismatch — caller (handler) should turn that into 426
// Upgrade Required so the consumer rejoins.
func (c *Coordinator) CommitOffsets(ctx context.Context, group, memberID string, generation int, commits []CommitInput) error {
	c.mu.Lock()
	g, ok := c.groups[group]
	c.mu.Unlock()
	if !ok {
		return ErrUnknownGroup
	}

	g.mu.RLock()
	currentGen := g.Generation
	_, memberOK := g.Members[memberID]
	g.mu.RUnlock()

	if !memberOK {
		return ErrUnknownMember
	}
	// Generation 0 means "no rebalance ever finalized" — accept any commits
	// pre-stable so the offset store is usable in single-broker flows that
	// don't run an explicit join (rare, but cheap to support).
	if currentGen != 0 && generation != currentGen {
		return ErrGenerationMismatch
	}

	now := c.clock().UnixNano()
	for _, c0 := range commits {
		rec := CommitRecord{
			Offset:      c0.Offset,
			CommittedAt: now,
			MemberID:    memberID,
		}
		value, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal commit: %w", err)
		}
		key := CommitKey(group, c0.Topic, c0.Partition)
		if _, err := c.writer.WriteCommit(ctx, key, value); err != nil {
			return fmt.Errorf("persist commit %s/%d: %w", c0.Topic, c0.Partition, err)
		}
		c.store.Apply(group, c0.Topic, c0.Partition, rec)
	}
	return nil
}

// CommitInput is a single (topic, partition, offset) row inside a commit batch.
type CommitInput struct {
	Topic     string
	Partition int
	Offset    uint64
}

// LookupOffsets returns committed offsets for a group, optionally filtered to
// a single topic. Used by GET /groups/{group}/offsets.
func (c *Coordinator) LookupOffsets(group, topicFilter string) map[string]map[int]CommitRecord {
	all := c.store.LookupAll(group)
	if topicFilter == "" {
		return all
	}
	if all == nil {
		return nil
	}
	filtered := make(map[string]map[int]CommitRecord)
	if rows, ok := all[topicFilter]; ok {
		filtered[topicFilter] = rows
	}
	return filtered
}

// Describe returns a snapshot of a single group. ok=false if missing.
func (c *Coordinator) Describe(group string) (GroupSnapshot, bool) {
	c.mu.Lock()
	g, ok := c.groups[group]
	c.mu.Unlock()
	if !ok {
		return GroupSnapshot{}, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snapshot(), true
}

// List returns snapshots for every known group, sorted by name.
func (c *Coordinator) List() []GroupSnapshot {
	c.mu.Lock()
	names := make([]string, 0, len(c.groups))
	for n := range c.groups {
		names = append(names, n)
	}
	c.mu.Unlock()
	sort.Strings(names)

	out := make([]GroupSnapshot, 0, len(names))
	for _, n := range names {
		c.mu.Lock()
		g, ok := c.groups[n]
		c.mu.Unlock()
		if !ok {
			continue
		}
		g.mu.RLock()
		out = append(out, g.snapshot())
		g.mu.RUnlock()
	}
	return out
}

// startRebalanceLocked transitions the group into Rebalancing. Caller must
// hold g.mu. Resets the rejoined set so we re-collect rejoins.
func (c *Coordinator) startRebalanceLocked(g *Group, strategyName string) {
	if g.State != Rebalancing {
		log.Printf("group %s: starting rebalance (gen %d → %d, members=%d)", g.Name, g.Generation, g.Generation+1, len(g.Members))
	}
	g.State = Rebalancing
	g.rebalanceStartedAt = c.clock()
	g.rejoined = make(map[string]struct{})
	if strategyName != "" {
		g.StrategyName = strategyName
	}
	if g.StrategyName == "" {
		g.StrategyName = "range"
	}
}

// finalizeLocked computes the new assignment and bumps generation. Caller
// must hold g.mu.
func (c *Coordinator) finalizeLocked(g *Group) {
	g.Generation++

	// Build topic → partitions map from all members' subscriptions.
	topics := make(map[string][]int)
	for _, t := range g.allSubscribedTopics() {
		count, ok := c.resolver.PartitionCount(t)
		if !ok || count <= 0 {
			// Unknown topic: zero partitions. Members subscribed to it just
			// won't get assignments from it; we don't fail the rebalance.
			topics[t] = nil
			continue
		}
		parts := make([]int, count)
		for i := 0; i < count; i++ {
			parts[i] = i
		}
		topics[t] = parts
	}

	members := g.memberIDs()
	strategy := LookupStrategy(g.StrategyName)
	g.Assignment = strategy.Assign(topics, members)
	g.State = Stable

	log.Printf("group %s: rebalance finalized at gen %d, %d member(s), strategy=%s", g.Name, g.Generation, len(members), strategy.Name())
}

// sessionLoop runs every second and drops members whose LastHeartbeat is
// older than their session timeout. Also force-finalizes rebalances that
// have been waiting longer than rebalanceGrace.
func (c *Coordinator) sessionLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.tick()
		}
	}
}

// tick is the worker the sessionLoop runs each interval. Split out so tests
// can drive it directly without waiting on a wall clock.
func (c *Coordinator) tick() {
	now := c.clock()

	c.mu.Lock()
	groups := make([]*Group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.Unlock()

	for _, g := range groups {
		g.mu.Lock()
		// 1. Drop timed-out members.
		var dropped []string
		for id, m := range g.Members {
			deadline := m.LastHeartbeat.Add(time.Duration(m.SessionMs) * time.Millisecond)
			if now.After(deadline) {
				dropped = append(dropped, id)
			}
		}
		for _, id := range dropped {
			delete(g.Members, id)
			delete(g.Assignment, id)
			log.Printf("group %s: member %s timed out", g.Name, id)
		}
		if len(dropped) > 0 {
			if len(g.Members) == 0 {
				g.State = Empty
				g.Assignment = make(map[string][]Assignment)
				g.rejoined = make(map[string]struct{})
			} else {
				c.startRebalanceLocked(g, g.StrategyName)
			}
		}

		// 2. Force-finalize stale rebalances. We treat the grace window as
		//    "best effort to wait for all members"; once it elapses we stamp
		//    the assignment with whoever has rejoined.
		if g.State == Rebalancing {
			if !g.rebalanceStartedAt.IsZero() && now.Sub(g.rebalanceStartedAt) >= rebalanceGrace {
				// Drop members who never rejoined.
				for id := range g.Members {
					if _, ok := g.rejoined[id]; !ok {
						delete(g.Members, id)
						delete(g.Assignment, id)
					}
				}
				if len(g.Members) == 0 {
					g.State = Empty
					g.Assignment = make(map[string][]Assignment)
					g.rejoined = make(map[string]struct{})
				} else {
					c.finalizeLocked(g)
				}
			} else if g.allRejoined() {
				c.finalizeLocked(g)
			}
		}
		g.mu.Unlock()
	}
}

// SetClock overrides the time source. Tests use this to drive session
// timeouts deterministically.
func (c *Coordinator) SetClock(clock func() time.Time) {
	c.clock = clock
}

// Tick exposes the internal tick for tests. Production code uses sessionLoop.
func (c *Coordinator) Tick() {
	c.tick()
}

// generateMemberID returns a short random hex string. Not cryptographically
// significant — just unique enough to disambiguate concurrent joins.
func generateMemberID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to time-based ID — unique within one process.
		return fmt.Sprintf("m-%d", time.Now().UnixNano())
	}
	return "m-" + hex.EncodeToString(b[:])
}

// topicsEqual checks set equality (order-independent).
func topicsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}
