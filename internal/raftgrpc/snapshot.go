package raftgrpc

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/raft"
)

const (
	SnapshotChunkBytes = 1 << 20
	MaxSnapshotBytes   = uint64(64 << 30)
)

// SnapshotReceiver is the narrow receive seam implemented by a gRPC stream and tests.
type SnapshotReceiver interface {
	Recv() (*lsmdbv1.SnapshotChunk, error)
}

// ReceiveSnapshot validates and reassembles one complete snapshot stream.
func ReceiveSnapshot(stream SnapshotReceiver) (raft.Message, error) {
	var data bytes.Buffer
	message, err := ReceiveSnapshotTo(stream, &data)
	if err != nil {
		return raft.Message{}, err
	}
	message.Snapshot.Data = data.Bytes()
	return message, nil
}

// ReceiveSnapshotTo validates and writes one complete snapshot stream.
func ReceiveSnapshotTo(stream SnapshotReceiver, writer io.Writer) (raft.Message, error) {
	var first *lsmdbv1.SnapshotChunk
	var received uint64
	checksum := crc32.NewIEEE()
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return raft.Message{}, fmt.Errorf("receive snapshot chunk: %w", err)
		}
		if chunk == nil {
			return raft.Message{}, errors.New("nil snapshot chunk")
		}
		if first == nil {
			if chunk.From == 0 || chunk.To == 0 || chunk.RaftTerm == 0 || chunk.SnapshotIndex == 0 || chunk.SnapshotTerm == 0 {
				return raft.Message{}, errors.New("snapshot stream metadata must be positive")
			}
			if chunk.TotalSize > MaxSnapshotBytes {
				return raft.Message{}, fmt.Errorf("snapshot exceeds %d bytes", MaxSnapshotBytes)
			}
			first = chunk
		} else if chunk.From != first.From || chunk.To != first.To || chunk.RaftTerm != first.RaftTerm || chunk.SnapshotIndex != first.SnapshotIndex || chunk.SnapshotTerm != first.SnapshotTerm || chunk.TotalSize != first.TotalSize || chunk.Checksum != first.Checksum || chunk.MembershipIndex != first.MembershipIndex || !equalIDs(chunk.Voters, first.Voters) || !equalIDs(chunk.JointVoters, first.JointVoters) {
			return raft.Message{}, errors.New("snapshot stream metadata changed between chunks")
		}
		if chunk.Offset != received {
			return raft.Message{}, fmt.Errorf("snapshot chunk offset %d, want %d", chunk.Offset, received)
		}
		if len(chunk.Data) > SnapshotChunkBytes {
			return raft.Message{}, fmt.Errorf("snapshot chunk exceeds %d bytes", SnapshotChunkBytes)
		}
		if uint64(len(chunk.Data)) > chunk.TotalSize-received {
			return raft.Message{}, errors.New("snapshot chunk exceeds declared total size")
		}
		if _, err := io.Copy(io.MultiWriter(writer, checksum), bytes.NewReader(chunk.Data)); err != nil {
			return raft.Message{}, fmt.Errorf("write snapshot chunk at %d: %w", chunk.Offset, err)
		}
		received += uint64(len(chunk.Data))
	}
	if first == nil {
		return raft.Message{}, errors.New("empty snapshot stream")
	}
	if received != first.TotalSize {
		return raft.Message{}, fmt.Errorf("snapshot stream ended at %d bytes, want %d", received, first.TotalSize)
	}
	if checksum.Sum32() != first.Checksum {
		return raft.Message{}, errors.New("snapshot stream checksum mismatch")
	}
	return raft.Message{Type: raft.MsgSnapshot, From: first.From, To: first.To, Term: first.RaftTerm, Snapshot: &raft.Snapshot{
		Index: first.SnapshotIndex, Term: first.SnapshotTerm,
		Membership: raft.Membership{Voters: append([]uint64(nil), first.Voters...), JointVoters: append([]uint64(nil), first.JointVoters...), Index: first.MembershipIndex},
	}}, nil
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
