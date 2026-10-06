package raftgrpc

import (
	"testing"

	"lsmdb/internal/raft"
)

// The origin tag is local instrumentation; it must never cross the wire.
func TestProtoConversionDropsOrigin(t *testing.T) {
	message := raft.Message{
		Type: raft.MsgAppend, From: 1, To: 2, Term: 3, LogIndex: 4, LogTerm: 3,
		Entries: []raft.Entry{{Index: 5, Term: 3, Data: []byte("x")}}, LeaderCommit: 4,
		Origin: raft.OriginAckResend,
	}
	decoded, err := FromProto(ToProto(message))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Origin != raft.OriginOther {
		t.Fatalf("origin crossed the proto conversion: %v", decoded.Origin)
	}
	decoded.Origin = message.Origin
	if decoded.Term != message.Term || decoded.LogIndex != message.LogIndex || len(decoded.Entries) != 1 {
		t.Fatalf("conversion changed other fields: %+v", decoded)
	}
}
