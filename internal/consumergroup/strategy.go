// Package consumergroup implements Kafka-style consumer groups: membership,
// partition assignment, offset tracking via an internal topic, and a thin
// dead-letter-queue primitive. The package is intentionally storage-agnostic;
// the broker wires it up to the underlying topic/partition log.
package consumergroup

import "sort"

// Assignment is a slice of partitions a member owns within one topic.
type Assignment struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

// Strategy partitions a set of (topic → partitions) across a set of members.
// Implementations must be deterministic: given the same topics and the same
// (sorted) members, they must return the same result. The coordinator depends
// on this so that two brokers computing assignments for the same generation
// agree without coordination.
type Strategy interface {
	Name() string
	Assign(topics map[string][]int, members []string) map[string][]Assignment
}

// RangeStrategy gives each member a contiguous range of partitions per topic.
// With 6 partitions and 3 members → m1=[0,1], m2=[2,3], m3=[4,5]. This is the
// Kafka default; it's good for join semantics (related partitions co-located)
// but uneven when partitions don't divide evenly across members.
type RangeStrategy struct{}

func (RangeStrategy) Name() string { return "range" }

func (RangeStrategy) Assign(topics map[string][]int, members []string) map[string][]Assignment {
	out := initAssignments(members)
	if len(members) == 0 {
		return out
	}

	// Sort members so two coordinators on the same generation pick the same
	// slices. We also sort the topic keys so iteration order is stable, which
	// matters for tests.
	sortedMembers := append([]string(nil), members...)
	sort.Strings(sortedMembers)

	for _, t := range sortedTopicKeys(topics) {
		parts := append([]int(nil), topics[t]...)
		sort.Ints(parts)
		if len(parts) == 0 {
			continue
		}

		numMembers := len(sortedMembers)
		partsPerMember := len(parts) / numMembers
		extra := len(parts) % numMembers // first `extra` members get one more

		start := 0
		for i, m := range sortedMembers {
			share := partsPerMember
			if i < extra {
				share++
			}
			if share == 0 {
				continue
			}
			end := start + share
			if end > len(parts) {
				end = len(parts)
			}
			out[m] = append(out[m], Assignment{
				Topic:      t,
				Partitions: append([]int(nil), parts[start:end]...),
			})
			start = end
		}
	}
	return out
}

// RoundRobinStrategy distributes partitions one-by-one across members. With 6
// partitions and 3 members → m1=[0,3], m2=[1,4], m3=[2,5]. Balances better
// than range when there are many small topics; less locality.
type RoundRobinStrategy struct{}

func (RoundRobinStrategy) Name() string { return "round-robin" }

func (RoundRobinStrategy) Assign(topics map[string][]int, members []string) map[string][]Assignment {
	out := initAssignments(members)
	if len(members) == 0 {
		return out
	}
	sortedMembers := append([]string(nil), members...)
	sort.Strings(sortedMembers)

	// Per-topic partitions per member (assemble then flatten).
	perMemberPerTopic := make(map[string]map[string][]int)
	for _, m := range sortedMembers {
		perMemberPerTopic[m] = make(map[string][]int)
	}

	idx := 0
	for _, t := range sortedTopicKeys(topics) {
		parts := append([]int(nil), topics[t]...)
		sort.Ints(parts)
		for _, p := range parts {
			m := sortedMembers[idx%len(sortedMembers)]
			perMemberPerTopic[m][t] = append(perMemberPerTopic[m][t], p)
			idx++
		}
	}

	for _, m := range sortedMembers {
		topicsForMember := perMemberPerTopic[m]
		for _, t := range sortedTopicKeys2(topicsForMember) {
			out[m] = append(out[m], Assignment{
				Topic:      t,
				Partitions: topicsForMember[t],
			})
		}
	}
	return out
}

// LookupStrategy returns a strategy by name. Falls back to RangeStrategy if
// the name is unknown so callers don't need to error-handle missing strategies.
func LookupStrategy(name string) Strategy {
	switch name {
	case "round-robin", "roundrobin":
		return RoundRobinStrategy{}
	case "range", "":
		return RangeStrategy{}
	default:
		return RangeStrategy{}
	}
}

// initAssignments builds the empty result map so callers can append without
// worrying about nil maps.
func initAssignments(members []string) map[string][]Assignment {
	out := make(map[string][]Assignment, len(members))
	for _, m := range members {
		out[m] = nil
	}
	return out
}

func sortedTopicKeys(m map[string][]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedTopicKeys2(m map[string][]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
