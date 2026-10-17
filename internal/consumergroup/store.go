package consumergroup

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"

	"github.com/streamq/streamq/internal/topic"
)

// InternalTopic is the name of the internal compacted-style topic used to durably
// store consumer-group offset commits. Each commit becomes one message whose key
// encodes (group, topic, partition) so a single group's offsets live in one
// partition and replay is local.
const InternalTopic = "__consumer_offsets"

// DLQPrefix is prepended to a source topic name to derive the DLQ topic.
// e.g. produce a bad message on "orders" → /groups/g/dlq → it's appended to
// __dlq.orders with the same partition layout.
const DLQPrefix = "__dlq."

// CommitRecord is the value half of an offset commit message.
// JSON is intentionally tiny — every commit pays this on the wire.
type CommitRecord struct {
	Offset      uint64 `json:"offset"`
	CommittedAt int64  `json:"committed_at"` // unix nanos
	MemberID    string `json:"member_id"`
}

// CommitKey returns the canonical message key for a (group, topic, partition)
// commit. fnv-hashing it picks the offsets-topic partition that owns this row.
func CommitKey(group, topicName string, partition int) string {
	return fmt.Sprintf("%s:%s:%d", group, topicName, partition)
}

// PartitionForKey deterministically maps a commit key (or any string) to one
// of the offsets-topic partitions. Same fnv-64a used elsewhere in StreamQ so
// the result is comparable.
func PartitionForKey(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(numPartitions))
}

// PartitionForGroup returns the offsets-topic partition that owns a given
// group's metadata + commits. The coordinator broker for a group is whoever
// leads this partition.
func PartitionForGroup(group string, numPartitions int) int {
	return PartitionForKey(group, numPartitions)
}

// commitRow is one (group, topic, partition) entry in the in-memory store.
type commitRow struct {
	Offset      uint64
	CommittedAt int64
	MemberID    string
}

// Store holds the latest committed offset for every (group, topic, partition)
// row this broker knows about. It's populated on startup by replaying the
// internal topic and updated live as new commits flow through.
//
// All methods are safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	rows map[string]map[string]map[int]commitRow // group → topic → partition → row
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{rows: make(map[string]map[string]map[int]commitRow)}
}

// Apply records a commit. It's idempotent and "last-write-wins" by message
// order — callers replaying the log must apply in append order so the final
// state matches what was committed.
func (s *Store) Apply(group, topicName string, partition int, rec CommitRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	gm := s.rows[group]
	if gm == nil {
		gm = make(map[string]map[int]commitRow)
		s.rows[group] = gm
	}
	tm := gm[topicName]
	if tm == nil {
		tm = make(map[int]commitRow)
		gm[topicName] = tm
	}
	tm[partition] = commitRow{
		Offset:      rec.Offset,
		CommittedAt: rec.CommittedAt,
		MemberID:    rec.MemberID,
	}
}

// Lookup returns the last committed offset for one (group, topic, partition).
// ok=false if there's no commit yet — caller should treat that as "start from 0".
func (s *Store) Lookup(group, topicName string, partition int) (CommitRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gm := s.rows[group]
	if gm == nil {
		return CommitRecord{}, false
	}
	tm := gm[topicName]
	if tm == nil {
		return CommitRecord{}, false
	}
	row, ok := tm[partition]
	if !ok {
		return CommitRecord{}, false
	}
	return CommitRecord{
		Offset:      row.Offset,
		CommittedAt: row.CommittedAt,
		MemberID:    row.MemberID,
	}, true
}

// LookupAll returns every committed (topic, partition, offset) row for a
// group. Used by the GET /groups/{group}/offsets endpoint.
func (s *Store) LookupAll(group string) map[string]map[int]CommitRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gm := s.rows[group]
	if gm == nil {
		return nil
	}
	out := make(map[string]map[int]CommitRecord, len(gm))
	for tn, tm := range gm {
		dst := make(map[int]CommitRecord, len(tm))
		for pid, row := range tm {
			dst[pid] = CommitRecord{
				Offset:      row.Offset,
				CommittedAt: row.CommittedAt,
				MemberID:    row.MemberID,
			}
		}
		out[tn] = dst
	}
	return out
}

// Groups returns the names of every group with at least one row.
func (s *Store) Groups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.rows))
	for g := range s.rows {
		out = append(out, g)
	}
	return out
}

// DecodeMessage parses a __consumer_offsets message into its component parts.
// The key format is "<group>:<topic>:<partition>" and the value is a JSON
// CommitRecord. Returns (group, topic, partition, record, error) — the error
// is non-nil only for malformed records, which the caller should log and skip.
func DecodeMessage(m topic.Message) (string, string, int, CommitRecord, error) {
	group, tn, part, err := parseCommitKey(m.Key)
	if err != nil {
		return "", "", 0, CommitRecord{}, err
	}
	var rec CommitRecord
	if len(m.Value) > 0 {
		if err := json.Unmarshal([]byte(m.Value), &rec); err != nil {
			return "", "", 0, CommitRecord{}, fmt.Errorf("decode commit value: %w", err)
		}
	}
	return group, tn, part, rec, nil
}

// parseCommitKey splits "<group>:<topic>:<partition>". We only split on the
// last two colons so groups or topic names containing colons survive the round
// trip — there are no validators forbidding them at the API.
func parseCommitKey(key string) (string, string, int, error) {
	if key == "" {
		return "", "", 0, fmt.Errorf("empty commit key")
	}
	// Find the last colon (partition separator), then the second-to-last
	// (topic/group separator).
	lastColon := -1
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == ':' {
			lastColon = i
			break
		}
	}
	if lastColon <= 0 || lastColon == len(key)-1 {
		return "", "", 0, fmt.Errorf("malformed commit key %q", key)
	}

	prefix := key[:lastColon]
	partStr := key[lastColon+1:]

	secondColon := -1
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == ':' {
			secondColon = i
			break
		}
	}
	if secondColon <= 0 || secondColon == len(prefix)-1 {
		return "", "", 0, fmt.Errorf("malformed commit key %q", key)
	}

	group := prefix[:secondColon]
	tn := prefix[secondColon+1:]

	// strconv.Atoi requires the whole string to be a valid integer, so junk
	// suffixes like "42abc" are rejected. fmt.Sscanf("%d", ...) would have
	// happily returned 42, silently corrupting parsing.
	part, err := strconv.Atoi(partStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("malformed commit key partition in %q: %w", key, err)
	}
	if part < 0 {
		return "", "", 0, fmt.Errorf("malformed commit key partition in %q: negative", key)
	}
	return group, tn, part, nil
}
