package consumergroup

import (
	"reflect"
	"sort"
	"testing"
)

// pmap returns the partitions assigned to each member, flattened across topics
// for the given topic name. Helps assertions stay focused on the layout.
func pmap(result map[string][]Assignment, topic string) map[string][]int {
	out := make(map[string][]int)
	for m, asgs := range result {
		for _, a := range asgs {
			if a.Topic == topic {
				out[m] = append(out[m], a.Partitions...)
			}
		}
		// Always include a key for every member, even if empty.
		if _, ok := out[m]; !ok {
			out[m] = nil
		}
	}
	for _, v := range out {
		sort.Ints(v)
	}
	return out
}

func TestRangeStrategy_EvenSplit(t *testing.T) {
	topics := map[string][]int{
		"t": {0, 1, 2, 3, 4, 5},
	}
	members := []string{"m1", "m2", "m3"}
	got := pmap(RangeStrategy{}.Assign(topics, members), "t")
	want := map[string][]int{
		"m1": {0, 1},
		"m2": {2, 3},
		"m3": {4, 5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("range even: got %v want %v", got, want)
	}
}

func TestRangeStrategy_UnevenSplit(t *testing.T) {
	topics := map[string][]int{
		"t": {0, 1, 2, 3, 4, 5, 6},
	}
	members := []string{"m1", "m2", "m3"}
	got := pmap(RangeStrategy{}.Assign(topics, members), "t")
	// 7/3 = 2 with remainder 1 → first member gets the extra.
	want := map[string][]int{
		"m1": {0, 1, 2},
		"m2": {3, 4},
		"m3": {5, 6},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("range uneven: got %v want %v", got, want)
	}
}

func TestRoundRobinStrategy_EvenSplit(t *testing.T) {
	topics := map[string][]int{
		"t": {0, 1, 2, 3, 4, 5},
	}
	members := []string{"m1", "m2", "m3"}
	got := pmap(RoundRobinStrategy{}.Assign(topics, members), "t")
	want := map[string][]int{
		"m1": {0, 3},
		"m2": {1, 4},
		"m3": {2, 5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-robin even: got %v want %v", got, want)
	}
}

func TestStrategies_NoPartitions(t *testing.T) {
	topics := map[string][]int{"t": {}}
	members := []string{"m1", "m2"}
	for _, s := range []Strategy{RangeStrategy{}, RoundRobinStrategy{}} {
		got := s.Assign(topics, members)
		for _, m := range members {
			if len(got[m]) != 0 {
				// Empty topic shouldn't produce an Assignment entry.
				t.Fatalf("%s: member %s got %v, want empty", s.Name(), m, got[m])
			}
		}
	}
}

func TestStrategies_MoreMembersThanPartitions(t *testing.T) {
	topics := map[string][]int{"t": {0, 1}}
	members := []string{"m1", "m2", "m3", "m4"}

	t.Run("range", func(t *testing.T) {
		got := pmap(RangeStrategy{}.Assign(topics, members), "t")
		want := map[string][]int{
			"m1": {0},
			"m2": {1},
			"m3": nil,
			"m4": nil,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})

	t.Run("round-robin", func(t *testing.T) {
		got := pmap(RoundRobinStrategy{}.Assign(topics, members), "t")
		want := map[string][]int{
			"m1": {0},
			"m2": {1},
			"m3": nil,
			"m4": nil,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
}

func TestStrategies_SingleMember(t *testing.T) {
	topics := map[string][]int{
		"a": {0, 1, 2},
		"b": {0, 1},
	}
	for _, s := range []Strategy{RangeStrategy{}, RoundRobinStrategy{}} {
		got := s.Assign(topics, []string{"m1"})
		// Single member should get every partition from every topic.
		gotByTopic := make(map[string][]int)
		for _, a := range got["m1"] {
			gotByTopic[a.Topic] = a.Partitions
		}
		if !reflect.DeepEqual(gotByTopic["a"], []int{0, 1, 2}) {
			t.Fatalf("%s: topic a got %v", s.Name(), gotByTopic["a"])
		}
		if !reflect.DeepEqual(gotByTopic["b"], []int{0, 1}) {
			t.Fatalf("%s: topic b got %v", s.Name(), gotByTopic["b"])
		}
	}
}

func TestStrategies_Deterministic(t *testing.T) {
	topics := map[string][]int{
		"x": {0, 1, 2, 3},
		"y": {0, 1, 2},
	}
	// Same inputs, different order → same output.
	a := RangeStrategy{}.Assign(topics, []string{"a", "b", "c"})
	b := RangeStrategy{}.Assign(topics, []string{"c", "b", "a"})
	for m := range a {
		if !reflect.DeepEqual(a[m], b[m]) {
			t.Fatalf("range not deterministic: member %s got %v vs %v", m, a[m], b[m])
		}
	}
}

func TestLookupStrategy(t *testing.T) {
	cases := map[string]string{
		"range":       "range",
		"":            "range", // default
		"unknown":     "range", // fallback
		"round-robin": "round-robin",
		"roundrobin":  "round-robin",
	}
	for in, want := range cases {
		s := LookupStrategy(in)
		if s.Name() != want {
			t.Fatalf("LookupStrategy(%q) → %s, want %s", in, s.Name(), want)
		}
	}
}
