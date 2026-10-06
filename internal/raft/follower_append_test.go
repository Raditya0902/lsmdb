package raft

import (
	"math"
	"runtime"
	"slices"
	"testing"
	"time"
)

func jointEntry(index, term uint64) Entry {
	return Entry{Index: index, Term: term, Data: encodeMembership(membershipCommand{
		Phase: configJoint, Old: []uint64{1, 2, 3}, New: []uint64{1, 2, 3, 4},
	})}
}

func TestConflictTruncationRollsBackMembership(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 2}, []Entry{{Index: 1, Term: 1}, jointEntry(2, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if joint := node.Status().Membership.JointVoters; len(joint) == 0 {
		t.Fatal("setup: the uncommitted joint entry is not active")
	}
	// The term-2 leader replaces the uncommitted joint entry with a normal one.
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 2, Data: []byte("replacement")}}, LeaderCommit: 1,
	})
	if update.TruncateFrom != 2 {
		t.Fatalf("TruncateFrom = %d, want 2", update.TruncateFrom)
	}
	membership := node.Status().Membership
	if len(membership.JointVoters) != 0 || !slices.Equal(membership.Voters, []uint64{1, 2, 3}) {
		t.Fatalf("membership after truncating the joint entry = %+v, want voters [1 2 3] and no joint set", membership)
	}
}

func TestFailedRebuildRestoresTruncatedTail(t *testing.T) {
	original := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Data: []byte("two")}, {Index: 3, Term: 1, Data: []byte("three")}}
	node, err := New(testConfig(2), HardState{Term: 2}, original)
	if err != nil {
		t.Fatal(err)
	}
	// A conflicting append truncates 2..3, then brings a malformed configuration
	// entry, so the rebuild fails and the follower must restore its old log.
	bad := append(append([]byte(nil), membershipPrefix...), []byte("not-json")...)
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 2, Data: bad}},
	})
	if len(update.Messages) != 1 || !update.Messages[0].Reject || update.TruncateFrom != 0 || len(update.Entries) != 0 {
		t.Fatalf("update = %+v, want a rejection with no persistence effects", update)
	}
	got := node.Entries()
	if len(got) != len(original) {
		t.Fatalf("log after the failed append has %d entries, want %d", len(got), len(original))
	}
	for i := range original {
		if got[i].Index != original[i].Index || got[i].Term != original[i].Term || string(got[i].Data) != string(original[i].Data) {
			t.Fatalf("entry %d after the failed append = %+v, want %+v", i+1, got[i], original[i])
		}
	}
}

func TestAppendedConfigurationEntryUpdatesMembership(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 2}, []Entry{{Index: 1, Term: 1}})
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 1, LogTerm: 1, Entries: []Entry{jointEntry(2, 2)},
	})
	if update.TruncateFrom != 0 || len(update.Entries) != 1 {
		t.Fatalf("append: TruncateFrom %d, %d new entries; want 0 and 1", update.TruncateFrom, len(update.Entries))
	}
	if joint := node.Status().Membership.JointVoters; !slices.Equal(joint, []uint64{1, 2, 3, 4}) {
		t.Fatalf("joint voters after appending C_old,new = %v, want [1 2 3 4]", joint)
	}
}

type appendCost struct{ bytesPerOp, nsPerOp float64 }

// measureFollowerAppend steps a follower holding retained entries with entry-less
// heartbeats or one-entry appends, and returns the best of five rounds.
func measureFollowerAppend(t *testing.T, retained int, withEntry bool) appendCost {
	t.Helper()
	entries := make([]Entry, retained)
	for i := range entries {
		entries[i] = Entry{Index: uint64(i + 1), Term: 1, Data: make([]byte, 16)}
	}
	node, err := New(testConfig(2), HardState{Term: 1}, entries)
	if err != nil {
		t.Fatal(err)
	}
	last := uint64(retained)
	step := func() {
		message := Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: last, LogTerm: 1}
		if withEntry {
			message.Entries = []Entry{{Index: last + 1, Term: 1, Data: make([]byte, 16)}}
			last++
		}
		if update := node.Step(message); len(update.Messages) != 1 || update.Messages[0].Reject {
			t.Fatalf("append at %d not accepted: %+v", last, update.Messages)
		}
	}
	step() // the first append grows the log slice; keep that out of the rounds
	const rounds, ops = 5, 200
	best := appendCost{bytesPerOp: math.Inf(1), nsPerOp: math.Inf(1)}
	for round := 0; round < rounds; round++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		for i := 0; i < ops; i++ {
			step()
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		best.bytesPerOp = min(best.bytesPerOp, float64(after.TotalAlloc-before.TotalAlloc)/ops)
		best.nsPerOp = min(best.nsPerOp, float64(elapsed.Nanoseconds())/ops)
	}
	return best
}

func TestFollowerAppendCostDoesNotGrowWithRetainedLog(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withEntry bool
	}{
		{"heartbeat", false},
		{"one new entry", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small := measureFollowerAppend(t, 1_000, tc.withEntry)
			large := measureFollowerAppend(t, 100_000, tc.withEntry)
			t.Logf("N=1000: %.0f B/op %.0f ns/op; N=100000: %.0f B/op %.0f ns/op; time ratio %.2f",
				small.bytesPerOp, small.nsPerOp, large.bytesPerOp, large.nsPerOp, large.nsPerOp/small.nsPerOp)
			if large.bytesPerOp > 2*small.bytesPerOp+4096 {
				t.Errorf("bytes per append grow with the retained log: %.0f at N=100000 vs %.0f at N=1000", large.bytesPerOp, small.bytesPerOp)
			}
			if large.nsPerOp > 3*small.nsPerOp {
				t.Errorf("time per append grows with the retained log: %.0f ns at N=100000 vs %.0f ns at N=1000", large.nsPerOp, small.nsPerOp)
			}
		})
	}
}
