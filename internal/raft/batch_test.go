package raft

import (
	"bytes"
	"testing"
)

func TestProposeBatchAppendsInInputOrderWithOneUpdate(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	indexes, update, err := leader.ProposeBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 3 || indexes[0] != 2 || indexes[1] != 3 || indexes[2] != 4 {
		t.Fatalf("indexes = %v, want [2 3 4]", indexes)
	}
	if len(update.Entries) != 3 {
		t.Fatalf("update carries %d entries, want 3", len(update.Entries))
	}
	for i, want := range []string{"a", "b", "c"} {
		if entry := update.Entries[i]; entry.Index != indexes[i] || string(entry.Data) != want {
			t.Fatalf("entry %d = index %d data %q, want index %d data %q", i, entry.Index, entry.Data, indexes[i], want)
		}
	}
	for _, peer := range []uint64{2, 3} {
		appends := appendsTo(update, peer)
		if len(appends) != 1 || len(appends[0].Entries) != 3 || string(appends[0].Entries[0].Data) != "a" {
			t.Fatalf("appends to %d = %d messages, entries %v; want one append carrying a, b, c", peer, len(appends), entrySizes(appends))
		}
	}
}

func TestProposeBatchRejectsOversizeItemAlone(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	oversize := bytes.Repeat([]byte{'x'}, MaxEntryBytes+1)
	indexes, update, err := leader.ProposeBatch([][]byte{[]byte("a"), oversize, []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 3 || indexes[0] != 2 || indexes[1] != 0 || indexes[2] != 3 {
		t.Fatalf("indexes = %v, want [2 0 3]: the oversize item alone is rejected", indexes)
	}
	if len(update.Entries) != 2 || leader.Status().LastLogIndex != 3 {
		t.Fatalf("update carries %d entries, last index %d; want 2 and 3", len(update.Entries), leader.Status().LastLogIndex)
	}
}

func TestProposeBatchOnFollowerAppendsNothing(t *testing.T) {
	nodes := newTestCluster(t)
	electNodeOne(t, nodes)
	follower := nodes[2]
	last := follower.Status().LastLogIndex
	if _, _, err := follower.ProposeBatch([][]byte{[]byte("a"), []byte("b")}); err != ErrNotLeader {
		t.Fatalf("ProposeBatch on a follower: error %v, want ErrNotLeader", err)
	}
	if got := follower.Status().LastLogIndex; got != last {
		t.Fatalf("follower last index moved from %d to %d", last, got)
	}
}
