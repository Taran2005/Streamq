package consumergroup

import (
	"sort"
	"sync"
	"time"
)

// GroupState tracks where a group is in its rebalance lifecycle. It's
// deliberately tiny — three states are enough for stop-the-world rebalance.
type GroupState int

const (
	// Stable: all current members have a finalized assignment.
	Stable GroupState = iota
	// Rebalancing: a join/leave/timeout fired, we're waiting for all known
	// members to re-join with their latest topic subscriptions.
	Rebalancing
	// Empty: no members yet (just initialized) or all members left.
	Empty
)

func (gs GroupState) String() string {
	switch gs {
	case Stable:
		return "stable"
	case Rebalancing:
		return "rebalancing"
	case Empty:
		return "empty"
	default:
		return "unknown"
	}
}

// Member is the coordinator's view of a single consumer in a group.
type Member struct {
	ID            string
	Topics        []string
	SessionMs     int
	LastHeartbeat time.Time
	JoinedAt      time.Time
	Strategy      string
}

// Group is the coordinator's in-memory state for one consumer group. The
// caller (Coordinator) holds the group lock for all mutations.
//
// A group rebalances when a member joins, leaves, or times out. During
// rebalance, members get told to re-join on their next heartbeat. Once every
// known member has re-joined (or a 10s grace window passes), the coordinator
// computes a fresh assignment and bumps Generation.
type Group struct {
	mu sync.RWMutex

	Name       string
	State      GroupState
	Generation int

	Members    map[string]*Member
	Assignment map[string][]Assignment // member ID → assignments

	StrategyName string // strategy chosen for the current generation

	// rebalanceStartedAt is when State flipped to Rebalancing. Used by the
	// finalize pass to bound how long we wait for stragglers.
	rebalanceStartedAt time.Time

	// rejoined is the set of member IDs that have re-joined since the
	// current rebalance started. Once it covers every Members entry (or
	// the grace window expires), the coordinator finalizes.
	rejoined map[string]struct{}
}

func newGroup(name string) *Group {
	return &Group{
		Name:       name,
		State:      Empty,
		Generation: 0,
		Members:    make(map[string]*Member),
		Assignment: make(map[string][]Assignment),
		rejoined:   make(map[string]struct{}),
	}
}

// memberIDs returns a sorted snapshot of member IDs.
func (g *Group) memberIDs() []string {
	out := make([]string, 0, len(g.Members))
	for id := range g.Members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// allSubscribedTopics returns the union of every member's subscription.
// Sorted so assignment computations are stable.
func (g *Group) allSubscribedTopics() []string {
	set := make(map[string]struct{})
	for _, m := range g.Members {
		for _, t := range m.Topics {
			set[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// snapshot returns a copy of the group's public view. Used by HTTP handlers
// so they don't keep the group locked while serializing JSON.
type GroupSnapshot struct {
	Name       string
	State      string
	Generation int
	Strategy   string
	Members    []MemberSnapshot
	Topics     []string
}

type MemberSnapshot struct {
	ID         string
	Topics     []string
	Assignment []Assignment
}

func (g *Group) snapshot() GroupSnapshot {
	out := GroupSnapshot{
		Name:       g.Name,
		State:      g.State.String(),
		Generation: g.Generation,
		Strategy:   g.StrategyName,
		Topics:     g.allSubscribedTopics(),
	}
	for _, id := range g.memberIDs() {
		m := g.Members[id]
		out.Members = append(out.Members, MemberSnapshot{
			ID:         m.ID,
			Topics:     append([]string(nil), m.Topics...),
			Assignment: append([]Assignment(nil), g.Assignment[m.ID]...),
		})
	}
	return out
}

// markRejoined records that a member has re-joined the current generation.
func (g *Group) markRejoined(memberID string) {
	g.rejoined[memberID] = struct{}{}
}

// allRejoined reports whether every current member has re-joined since the
// rebalance started.
func (g *Group) allRejoined() bool {
	if len(g.Members) == 0 {
		return false
	}
	for id := range g.Members {
		if _, ok := g.rejoined[id]; !ok {
			return false
		}
	}
	return true
}
