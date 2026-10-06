package cluster

import (
	"bytes"
	"math"
	"strings"
	"testing"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"
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
