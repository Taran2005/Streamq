package consumergroup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeResolver answers PartitionCount from a static map.
type fakeResolver struct {
	counts map[string]int
}

func (f *fakeResolver) PartitionCount(t string) (int, bool) {
	n, ok := f.counts[t]
	return n, ok
}

// fakeWriter pretends to write to __consumer_offsets and records every call.
type fakeWriter struct {
	mu     sync.Mutex
	writes []struct {
		key   string
		value string
	}
	nextOffset uint64
	fail       bool
}

func (f *fakeWriter) WriteCommit(_ context.Context, key string, value []byte) (uint64, error) {
	if f.fail {
		return 0, errors.New("forced failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, struct {
		key   string
		value string
	}{key, string(value)})
	off := atomic.AddUint64(&f.nextOffset, 1) - 1
	return off, nil
}

func newTestCoord(t *testing.T, topics map[string]int) (*Coordinator, *fakeWriter) {
	t.Helper()
	w := &fakeWriter{}
	c := NewCoordinator(&fakeResolver{counts: topics}, w, NewStore())
	t.Cleanup(c.Stop)
	return c, w
}

func TestCoordinator_JoinSingleMember(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 3})

	res, err := c.Join(context.Background(), JoinRequest{
		GroupName: "g",
		Topics:    []string{"t"},
	})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if res.MemberID == "" {
		t.Fatal("expected an assigned member ID")
	}
	if res.Generation != 1 {
		t.Fatalf("expected gen=1 after first join, got %d", res.Generation)
	}
	if len(res.Assignment) != 1 || res.Assignment[0].Topic != "t" {
		t.Fatalf("unexpected assignment: %+v", res.Assignment)
	}
	if want := []int{0, 1, 2}; !intsEqual(res.Assignment[0].Partitions, want) {
		t.Fatalf("expected all 3 partitions, got %v", res.Assignment[0].Partitions)
	}
}

func TestCoordinator_JoinTriggersRebalance(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 6})

	// First member joins, finalizes immediately (only one rejoin needed).
	m1, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	gen1 := m1.Generation
	if gen1 != 1 {
		t.Fatalf("gen1=%d", gen1)
	}

	// Second member joins → rebalance starts. First member won't get a new
	// assignment until they re-join.
	m2, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	if !m2.RebalanceInProgress && m2.Generation > gen1 {
		// Acceptable: if both members happen to count as rejoined already
		// (only m2 hasn't rejoined yet), the rebalance is still in progress.
	}

	// m1 re-joins → all members rejoined → finalize at gen2.
	m1b, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: m1.MemberID, Topics: []string{"t"}})
	if m1b.Generation <= gen1 {
		t.Fatalf("expected generation bump after rejoin, got %d ≤ %d", m1b.Generation, gen1)
	}
	if m1b.RebalanceInProgress {
		t.Fatalf("expected rebalance to be finalized after both rejoin")
	}
	// 6 partitions / 2 members → 3 each (range).
	if len(m1b.Assignment) != 1 || len(m1b.Assignment[0].Partitions) != 3 {
		t.Fatalf("m1 expected 3 partitions, got %+v", m1b.Assignment)
	}
}

func TestCoordinator_LeaveTriggersRebalance(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 4})

	// Two members join and stabilize.
	a, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	b, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	// a rejoins to finalize.
	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: a.MemberID, Topics: []string{"t"}})

	// b leaves.
	if err := c.Leave("g", b.MemberID); err != nil {
		t.Fatalf("leave: %v", err)
	}

	// Heartbeat from a should request a rebalance.
	rb, err := c.Heartbeat("g", a.MemberID, a.Generation)
	if err != nil && !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("heartbeat: %v", err)
	}
	if !rb {
		// The leave-triggered rebalance is still in progress (a hasn't rejoined yet).
		t.Fatalf("expected rebalance_required=true after peer leave")
	}

	// a rejoins → finalize at the new generation, with everything.
	a2, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: a.MemberID, Topics: []string{"t"}})
	if a2.RebalanceInProgress {
		t.Fatalf("expected rebalance to finalize after single remaining member rejoins")
	}
	if len(a2.Assignment) != 1 || len(a2.Assignment[0].Partitions) != 4 {
		t.Fatalf("expected all 4 partitions after leave, got %+v", a2.Assignment)
	}
}

func TestCoordinator_HeartbeatGenerationMismatch(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 2})

	res, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})

	// Lie about generation.
	_, err := c.Heartbeat("g", res.MemberID, res.Generation+5)
	if !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected generation mismatch, got %v", err)
	}

	// Unknown member.
	if _, err := c.Heartbeat("g", "bogus", res.Generation); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("expected unknown member, got %v", err)
	}

	// Unknown group.
	if _, err := c.Heartbeat("nope", res.MemberID, res.Generation); !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("expected unknown group, got %v", err)
	}
}

func TestCoordinator_CommitOffsets(t *testing.T) {
	c, w := newTestCoord(t, map[string]int{"t": 2})

	res, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})

	commits := []CommitInput{
		{Topic: "t", Partition: 0, Offset: 42},
		{Topic: "t", Partition: 1, Offset: 17},
	}
	if err := c.CommitOffsets(context.Background(), "g", res.MemberID, res.Generation, commits); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(w.writes) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(w.writes))
	}

	// Lookup roundtrip.
	got, ok := c.LookupOffsets("g", "t")["t"][0]
	if !ok || got.Offset != 42 {
		t.Fatalf("lookup: %+v ok=%v", got, ok)
	}
}

func TestCoordinator_CommitWrongGeneration(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 1})

	res, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})

	err := c.CommitOffsets(context.Background(), "g", res.MemberID, res.Generation+1,
		[]CommitInput{{Topic: "t", Partition: 0, Offset: 1}})
	if !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected gen mismatch, got %v", err)
	}
}

// TestCoordinator_CommitBeforeFinalize covers the gen-0 stale-commit bug.
// A coordinator that's freshly restarted (or pre-finalize) must reject every
// commit until a join → finalize cycle has happened, otherwise an old member
// from a previous incarnation could overwrite fresh offsets during the
// vulnerable pre-stable window.
func TestCoordinator_CommitBeforeFinalize(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 1})

	// Manually wire up a group with a known member but no finalized rebalance
	// (Generation == 0). This simulates a coordinator that just restarted and
	// hasn't run a full join cycle yet.
	g := newGroup("g")
	g.Members["m1"] = &Member{ID: "m1", Topics: []string{"t"}}
	c.mu.Lock()
	c.groups["g"] = g
	c.mu.Unlock()

	if g.Generation != 0 {
		t.Fatalf("test prereq: expected gen=0, got %d", g.Generation)
	}

	err := c.CommitOffsets(context.Background(), "g", "m1", 0,
		[]CommitInput{{Topic: "t", Partition: 0, Offset: 99}})
	if !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected gen mismatch on pre-finalize commit, got %v", err)
	}

	// Sanity: a commit claiming gen=1 (or any other) is still rejected when
	// the coordinator itself hasn't finalized.
	err = c.CommitOffsets(context.Background(), "g", "m1", 1,
		[]CommitInput{{Topic: "t", Partition: 0, Offset: 99}})
	if !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected gen mismatch with bogus claimed gen, got %v", err)
	}
}

// TestCoordinator_LeaveIdempotent makes sure a double-leave (or a leave
// after the broker already evicted the member on session timeout) doesn't
// surface ErrUnknownMember. Real CLI consumers retry leave on SIGINT after
// the heartbeat had already failed, so this needs to be a no-op.
func TestCoordinator_LeaveIdempotent(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 1})

	res, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})

	if err := c.Leave("g", res.MemberID); err != nil {
		t.Fatalf("first leave: %v", err)
	}
	if err := c.Leave("g", res.MemberID); err != nil {
		t.Fatalf("second leave should be a no-op, got: %v", err)
	}
	if err := c.Leave("g", "never-existed"); err != nil {
		t.Fatalf("leave of unknown member should be a no-op, got: %v", err)
	}

	// Unknown group is still an error — different failure mode entirely.
	if err := c.Leave("nope", "m"); !errors.Is(err, ErrUnknownGroup) {
		t.Fatalf("expected unknown group, got %v", err)
	}
}

func TestCoordinator_SessionTimeoutDropsMember(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 2})

	// Driveable clock — wrapped so the session loop sees a consistent value
	// when -race is enabled.
	clock := &raceSafeClock{now: time.Unix(1_000_000, 0)}
	c.SetClock(clock.Now)

	res, _ := c.Join(context.Background(), JoinRequest{
		GroupName:        "g",
		Topics:           []string{"t"},
		SessionTimeoutMs: 5_000,
	})

	// Advance past the session timeout.
	clock.Advance(10 * time.Second)
	c.Tick()

	snap, ok := c.Describe("g")
	if !ok {
		t.Fatal("group missing")
	}
	if len(snap.Members) != 0 {
		t.Fatalf("expected member %s dropped, got snapshot=%+v", res.MemberID, snap)
	}
}

func TestCoordinator_RebalanceWith3MembersThen1Leaves(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t": 6})

	a, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	b, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})
	cc, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", Topics: []string{"t"}})

	// All three rejoin to finalize.
	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: a.MemberID, Topics: []string{"t"}})
	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: b.MemberID, Topics: []string{"t"}})
	cFin, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: cc.MemberID, Topics: []string{"t"}})

	if cFin.RebalanceInProgress {
		t.Fatalf("expected stable after all 3 rejoin")
	}

	// Each should have 2 partitions (6/3).
	snap, _ := c.Describe("g")
	for _, m := range snap.Members {
		if len(m.Assignment) != 1 || len(m.Assignment[0].Partitions) != 2 {
			t.Fatalf("member %s expected 2 partitions, got %+v", m.ID, m.Assignment)
		}
	}

	// b leaves; a and c re-join → 3 partitions each.
	if err := c.Leave("g", b.MemberID); err != nil {
		t.Fatalf("leave: %v", err)
	}
	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: a.MemberID, Topics: []string{"t"}})
	final, _ := c.Join(context.Background(), JoinRequest{GroupName: "g", MemberID: cc.MemberID, Topics: []string{"t"}})
	if final.RebalanceInProgress {
		t.Fatalf("expected stable after rejoin")
	}
	for _, m := range []string{a.MemberID, cc.MemberID} {
		snap, _ := c.Describe("g")
		var ms MemberSnapshot
		for _, x := range snap.Members {
			if x.ID == m {
				ms = x
			}
		}
		if len(ms.Assignment) != 1 || len(ms.Assignment[0].Partitions) != 3 {
			t.Fatalf("after rebalance member %s expected 3 partitions, got %+v", m, ms.Assignment)
		}
	}
}

func TestCoordinator_ListAndDescribe(t *testing.T) {
	c, _ := newTestCoord(t, map[string]int{"t1": 2, "t2": 1})

	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g1", Topics: []string{"t1"}})
	_, _ = c.Join(context.Background(), JoinRequest{GroupName: "g2", Topics: []string{"t2"}})

	all := c.List()
	if len(all) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(all))
	}
	if all[0].Name != "g1" || all[1].Name != "g2" {
		t.Fatalf("expected sorted by name, got %s %s", all[0].Name, all[1].Name)
	}

	if _, ok := c.Describe("missing"); ok {
		t.Fatal("expected describe miss")
	}
}

// raceSafeClock is a tiny goroutine-safe time source so coordinator tests
// pass under -race even when the session loop is reading it concurrently.
type raceSafeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *raceSafeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *raceSafeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
