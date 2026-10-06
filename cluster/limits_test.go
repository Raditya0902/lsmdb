package cluster

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"
	"lsmdb/internal/raftgrpc"
)

// D019 ties three limits together: the largest command the API accepts fits
// one Raft entry, and the largest single-entry append fits the gRPC limit.
func TestEntryLimitFitsBetweenCommandAndServerLimits(t *testing.T) {
	command, err := kvstate.EncodeCommand(lsmdbv1.Command_OPERATION_PUT,
		bytes.Repeat([]byte{'k'}, kvstate.MaxKeyBytes), bytes.Repeat([]byte{'v'}, kvstate.MaxValueBytes),
		strings.Repeat("c", 64), math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if len(command) > raft.MaxEntryBytes {
		t.Fatalf("largest accepted command is %d bytes, over the %d-byte entry limit", len(command), raft.MaxEntryBytes)
	}
	if largest := raft.MaxEntryBytes + raft.EntryOverheadBytes + raft.MessageEnvelopeBytes; largest > serverMessageLimit {
		t.Fatalf("largest single-entry append is %d bytes, over the %d-byte gRPC limit", largest, serverMessageLimit)
	}
}

// The largest command the API accepts, carried alone in an AppendEntries with
// every numeric field at its widest (ids, terms, indexes and a ReadIndex
// context), must serialize as a real RaftMessage within the gRPC server limit.
func TestMaxSizeAppendSerializesWithinServerLimit(t *testing.T) {
	const wide = math.MaxUint64
	command, err := kvstate.EncodeCommand(lsmdbv1.Command_OPERATION_PUT,
		bytes.Repeat([]byte{'k'}, kvstate.MaxKeyBytes), bytes.Repeat([]byte{'v'}, kvstate.MaxValueBytes),
		strings.Repeat("c", 64), wide)
	if err != nil {
		t.Fatal(err)
	}
	if len(command) > raft.MaxEntryBytes {
		t.Fatalf("encoded max command is %d bytes, over raft.MaxEntryBytes %d", len(command), raft.MaxEntryBytes)
	}
	message := raft.Message{
		Type: raft.MsgAppend, From: wide, To: wide, Term: wide, LogIndex: wide, LogTerm: wide,
		Entries: []raft.Entry{{Index: wide, Term: wide, Data: command}}, LeaderCommit: wide, Context: wide,
	}
	size := proto.Size(raftgrpc.ToProto(message))
	t.Logf("command %d bytes (entry limit %d, margin %d); append %d bytes (gRPC limit %d, margin %d)",
		len(command), raft.MaxEntryBytes, raft.MaxEntryBytes-len(command), size, serverMessageLimit, serverMessageLimit-size)
	if size > serverMessageLimit {
		t.Fatalf("largest single-entry append serializes to %d bytes, over the %d-byte gRPC limit", size, serverMessageLimit)
	}
}
