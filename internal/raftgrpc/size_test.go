package raftgrpc

import (
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

	"lsmdb/internal/raft"
)

// The core caps appends by accounted size (D019) without importing protobuf.
// Every encoded message must stay within that accounting plus the envelope,
// even with every numeric field at its widest varint.
func TestEncodedAppendFitsAccountedSize(t *testing.T) {
	for _, sizes := range [][]int{nil, {0}, {1}, {200, 200, 200, 200}, {1<<20 - raft.EntryOverheadBytes}, {raft.MaxEntryBytes}} {
		const wide = math.MaxUint64
		accounted := 0
		var entries []raft.Entry
		for _, size := range sizes {
			entries = append(entries, raft.Entry{Index: wide, Term: wide, Data: make([]byte, size)})
			accounted += size + raft.EntryOverheadBytes
		}
		message := raft.Message{
			Type: raft.MsgSnapshotResponse, From: wide, To: wide, Term: wide, LogIndex: wide, LogTerm: wide,
			Entries: entries, LeaderCommit: wide, Reject: true, RejectHint: wide, Context: wide,
		}
		if encoded, bound := proto.Size(ToProto(message)), accounted+raft.MessageEnvelopeBytes; encoded > bound {
			t.Errorf("entries %v: encoded %d bytes, accounted bound %d", sizes, encoded, bound)
		}
	}
}
