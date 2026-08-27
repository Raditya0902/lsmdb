package db

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestStreamedReplicaSnapshotReplacesStateAtomically(t *testing.T) {
	source, err := Open(t.TempDir(), &Options{DurabilityMode: DurabilityReplica, FlushThreshold: 2, CompactionThreshold: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	updates := [][]Mutation{
		{{Type: MutationPut, Key: "a", Value: []byte("old")}},
		{{Type: MutationPut, Key: "b", Value: []byte("two")}},
		{{Type: MutationPut, Key: "a", Value: []byte("new")}},
		{{Type: MutationDelete, Key: "b"}, {Type: MutationPut, Key: "c", Value: []byte("three")}},
	}
	for i, mutations := range updates {
		if err := source.ApplyBatch(uint64(i+1), mutations); err != nil {
			t.Fatal(err)
		}
	}
	var image bytes.Buffer
	index, err := source.WriteReplicaSnapshot(&image)
	if err != nil {
		t.Fatal(err)
	}
	if index != 4 {
		t.Fatalf("snapshot index = %d, want 4", index)
	}

	target, err := Open(t.TempDir(), &Options{DurabilityMode: DurabilityReplica})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.ReplaceReplicaSnapshot(index, uint64(image.Len()), bytes.NewReader(image.Bytes())); err != nil {
		t.Fatal(err)
	}
	if value, ok := target.Get("a"); !ok || string(value) != "new" {
		t.Fatalf("a = (%q,%v), want new", value, ok)
	}
	if _, ok := target.Get("b"); ok {
		t.Fatal("deleted key b survived snapshot")
	}
	if value, ok := target.Get("c"); !ok || string(value) != "three" {
		t.Fatalf("c = (%q,%v), want three", value, ok)
	}
	if target.AppliedIndex() != index || target.DurableIndex() != index {
		t.Fatalf("target indexes = applied %d durable %d", target.AppliedIndex(), target.DurableIndex())
	}
}

func TestStreamedReplicaSnapshotRejectsTruncationBeforePublish(t *testing.T) {
	target, err := Open(t.TempDir(), &Options{DurabilityMode: DurabilityReplica})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.ApplyBatch(1, []Mutation{{Type: MutationPut, Key: "existing", Value: []byte("safe")}}); err != nil {
		t.Fatal(err)
	}
	var truncated bytes.Buffer
	truncated.Write(replicaSnapshotMagic[:])
	_ = binary.Write(&truncated, binary.BigEndian, uint64(1))
	_ = binary.Write(&truncated, binary.BigEndian, uint32(8))
	if err := target.ReplaceReplicaSnapshot(2, uint64(truncated.Len()), &truncated); err == nil {
		t.Fatal("truncated snapshot was accepted")
	}
	if value, ok := target.Get("existing"); !ok || string(value) != "safe" {
		t.Fatalf("existing state changed to (%q,%v)", value, ok)
	}
	if target.AppliedIndex() != 1 {
		t.Fatalf("applied index = %d, want 1", target.AppliedIndex())
	}
}
