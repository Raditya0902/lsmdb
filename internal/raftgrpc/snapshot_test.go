package raftgrpc

import (
	"bytes"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
)

type chunkReceiver struct {
	chunks []*lsmdbv1.SnapshotChunk
	index  int
}

type generatedSnapshotReceiver struct {
	total    uint64
	offset   uint64
	checksum uint32
	data     []byte
}

func (r *generatedSnapshotReceiver) Recv() (*lsmdbv1.SnapshotChunk, error) {
	if r.offset == r.total {
		return nil, io.EOF
	}
	size := min(uint64(len(r.data)), r.total-r.offset)
	chunk := &lsmdbv1.SnapshotChunk{
		From: 1, To: 2, RaftTerm: 4, SnapshotIndex: 9, SnapshotTerm: 3,
		Offset: r.offset, TotalSize: r.total, Checksum: r.checksum,
		Data: r.data[:int(size)], Voters: []uint64{1, 2, 3}, MembershipIndex: 7,
	}
	r.offset += size
	return chunk, nil
}

func TestReceiveSnapshotStreamsBeyondFormer256MiBLimit(t *testing.T) {
	total := uint64(257 << 20)
	block := make([]byte, SnapshotChunkBytes)
	hash := crc32.NewIEEE()
	for remaining := total; remaining > 0; {
		size := min(uint64(len(block)), remaining)
		_, _ = hash.Write(block[:int(size)])
		remaining -= size
	}
	receiver := &generatedSnapshotReceiver{total: total, checksum: hash.Sum32(), data: block}
	message, err := ReceiveSnapshotTo(receiver, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if message.Snapshot == nil || message.Snapshot.Index != 9 || len(message.Snapshot.Data) != 0 {
		t.Fatalf("snapshot metadata = %#v", message.Snapshot)
	}
}

func (r *chunkReceiver) Recv() (*lsmdbv1.SnapshotChunk, error) {
	if r.index == len(r.chunks) {
		return nil, io.EOF
	}
	chunk := r.chunks[r.index]
	r.index++
	return chunk, nil
}

func snapshotChunks(data []byte) []*lsmdbv1.SnapshotChunk {
	checksum := crc32.ChecksumIEEE(data)
	var chunks []*lsmdbv1.SnapshotChunk
	for offset := 0; offset < len(data) || (len(data) == 0 && offset == 0); offset += SnapshotChunkBytes {
		end := min(offset+SnapshotChunkBytes, len(data))
		chunks = append(chunks, &lsmdbv1.SnapshotChunk{From: 1, To: 2, RaftTerm: 4, SnapshotIndex: 9, SnapshotTerm: 3, Offset: uint64(offset), TotalSize: uint64(len(data)), Checksum: checksum, Data: append([]byte(nil), data[offset:end]...), Voters: []uint64{1, 2, 3}, MembershipIndex: 7})
		if len(data) == 0 {
			break
		}
	}
	return chunks
}

func TestReceiveSnapshotReassemblesOrderedChunks(t *testing.T) {
	data := bytes.Repeat([]byte("snapshot-data"), SnapshotChunkBytes/4)
	message, err := ReceiveSnapshot(&chunkReceiver{chunks: snapshotChunks(data)})
	if err != nil {
		t.Fatal(err)
	}
	if message.From != 1 || message.To != 2 || message.Term != 4 || message.Snapshot == nil || message.Snapshot.Index != 9 || message.Snapshot.Term != 3 || !bytes.Equal(message.Snapshot.Data, data) {
		t.Fatalf("message = %#v", message)
	}
	if message.Snapshot.Membership.Index != 7 || len(message.Snapshot.Membership.Voters) != 3 {
		t.Fatalf("membership = %+v", message.Snapshot.Membership)
	}
}

func TestReceiveSnapshotRejectsInterruptedStream(t *testing.T) {
	chunks := snapshotChunks(bytes.Repeat([]byte("x"), SnapshotChunkBytes+10))
	_, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks[:1]})
	if err == nil || !strings.Contains(err.Error(), "ended") {
		t.Fatalf("error = %v", err)
	}
}

func TestReceiveSnapshotRejectsOffsetAndChecksumMismatch(t *testing.T) {
	chunks := snapshotChunks(bytes.Repeat([]byte("x"), SnapshotChunkBytes+10))
	chunks[1].Offset++
	if _, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks}); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("offset error = %v", err)
	}
	chunks = snapshotChunks([]byte("state"))
	chunks[0].Checksum++
	if _, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum error = %v", err)
	}
}

func TestReceiveSnapshotRejectsMetadataChangesAndOversize(t *testing.T) {
	chunks := snapshotChunks(bytes.Repeat([]byte("x"), SnapshotChunkBytes+10))
	chunks[1].SnapshotTerm++
	if _, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks}); err == nil || !strings.Contains(err.Error(), "metadata changed") {
		t.Fatalf("metadata error = %v", err)
	}
	chunks = snapshotChunks(bytes.Repeat([]byte("x"), SnapshotChunkBytes+10))
	chunks[1].Voters[0] = 9
	if _, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks}); err == nil || !strings.Contains(err.Error(), "metadata changed") {
		t.Fatalf("membership metadata error = %v", err)
	}
	chunks = snapshotChunks(nil)
	chunks[0].TotalSize = MaxSnapshotBytes + 1
	if _, err := ReceiveSnapshot(&chunkReceiver{chunks: chunks}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversize error = %v", err)
	}
}
