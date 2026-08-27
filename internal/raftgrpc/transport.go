// Package raftgrpc adapts Raft messages to the generated gRPC transport.
package raftgrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport caches one gRPC connection per static peer.
type Transport struct {
	mu               sync.Mutex
	peers            map[uint64]string
	conns            map[uint64]*grpc.ClientConn
	clients          map[uint64]lsmdbv1.RaftClient
	snapshotInFlight map[uint64]bool
}

func New(peers map[uint64]string) *Transport {
	copy := make(map[uint64]string, len(peers))
	for id, address := range peers {
		copy[id] = address
	}
	return &Transport{peers: copy, conns: make(map[uint64]*grpc.ClientConn), clients: make(map[uint64]lsmdbv1.RaftClient), snapshotInFlight: make(map[uint64]bool)}
}

func (t *Transport) Send(ctx context.Context, message raft.Message) error {
	if message.Type == raft.MsgSnapshot {
		if message.Snapshot == nil {
			return errors.New("snapshot message has no snapshot")
		}
		data := message.Snapshot.Data
		return t.SendSnapshot(ctx, message, bytes.NewReader(data), uint64(len(data)), crc32.ChecksumIEEE(data))
	}
	client, err := t.client(message.To)
	if err != nil {
		return err
	}
	_, err = client.Send(ctx, ToProto(message))
	return err
}

// SendSnapshot streams one durable snapshot image to a peer.
func (t *Transport) SendSnapshot(ctx context.Context, message raft.Message, reader io.Reader, size uint64, checksum uint32) error {
	if !t.beginSnapshot(message.To) {
		return nil
	}
	defer t.endSnapshot(message.To)
	client, err := t.client(message.To)
	if err != nil {
		return err
	}
	return sendSnapshot(ctx, client, message, reader, size, checksum)
}

func (t *Transport) beginSnapshot(peer uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.snapshotInFlight[peer] {
		return false
	}
	t.snapshotInFlight[peer] = true
	return true
}

func (t *Transport) endSnapshot(peer uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.snapshotInFlight, peer)
}

func sendSnapshot(ctx context.Context, client lsmdbv1.RaftClient, message raft.Message, reader io.Reader, size uint64, checksum uint32) error {
	if message.Snapshot == nil {
		return fmt.Errorf("snapshot message has no snapshot")
	}
	if size > MaxSnapshotBytes {
		return fmt.Errorf("snapshot exceeds %d bytes", MaxSnapshotBytes)
	}
	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		return err
	}
	buffer := make([]byte, SnapshotChunkBytes)
	for offset := uint64(0); offset < size || (size == 0 && offset == 0); {
		chunkSize := min(uint64(SnapshotChunkBytes), size-offset)
		data := buffer[:int(chunkSize)]
		if chunkSize > 0 {
			if _, err := io.ReadFull(reader, data); err != nil {
				return fmt.Errorf("read snapshot chunk at %d: %w", offset, err)
			}
		}
		chunk := &lsmdbv1.SnapshotChunk{
			From: message.From, To: message.To, RaftTerm: message.Term,
			SnapshotIndex: message.Snapshot.Index, SnapshotTerm: message.Snapshot.Term,
			Offset: offset, TotalSize: size, Checksum: checksum,
			Data:            append([]byte(nil), data...),
			Voters:          append([]uint64(nil), message.Snapshot.Membership.Voters...),
			JointVoters:     append([]uint64(nil), message.Snapshot.Membership.JointVoters...),
			MembershipIndex: message.Snapshot.Membership.Index,
		}
		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("send snapshot chunk at %d: %w", offset, err)
		}
		if size == 0 {
			break
		}
		offset += chunkSize
	}
	var trailing [1]byte
	if n, err := io.ReadFull(reader, trailing[:]); n != 0 || !errors.Is(err, io.EOF) {
		return errors.New("snapshot source exceeds declared size")
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("finish snapshot stream: %w", err)
	}
	return nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var first error
	for _, connection := range t.conns {
		if err := connection.Close(); err != nil && first == nil {
			first = err
		}
	}
	t.conns = make(map[uint64]*grpc.ClientConn)
	t.clients = make(map[uint64]lsmdbv1.RaftClient)
	t.snapshotInFlight = make(map[uint64]bool)
	return first
}

func (t *Transport) client(id uint64) (lsmdbv1.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if client := t.clients[id]; client != nil {
		return client, nil
	}
	address, ok := t.peers[id]
	if !ok || address == "" {
		return nil, fmt.Errorf("unknown raft peer %d", id)
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial raft peer %d: %w", id, err)
	}
	client := lsmdbv1.NewRaftClient(connection)
	t.conns[id] = connection
	t.clients[id] = client
	return client, nil
}

func ToProto(message raft.Message) *lsmdbv1.RaftMessage {
	entries := make([]*lsmdbv1.LogEntry, 0, len(message.Entries))
	for _, entry := range message.Entries {
		entries = append(entries, &lsmdbv1.LogEntry{Index: entry.Index, Term: entry.Term, Data: entry.Data})
	}
	return &lsmdbv1.RaftMessage{
		Type: uint32(message.Type), From: message.From, To: message.To, Term: message.Term,
		LogIndex: message.LogIndex, LogTerm: message.LogTerm, Entries: entries,
		LeaderCommit: message.LeaderCommit, Reject: message.Reject,
		RejectHint: message.RejectHint, Context: message.Context,
	}
}

func FromProto(message *lsmdbv1.RaftMessage) (raft.Message, error) {
	if message == nil || message.Type > uint32(raft.MsgSnapshotResponse) {
		return raft.Message{}, fmt.Errorf("invalid raft message type")
	}
	entries := make([]raft.Entry, 0, len(message.Entries))
	for _, entry := range message.Entries {
		if entry == nil {
			return raft.Message{}, fmt.Errorf("nil raft log entry")
		}
		entries = append(entries, raft.Entry{Index: entry.Index, Term: entry.Term, Data: append([]byte(nil), entry.Data...)})
	}
	return raft.Message{
		Type: raft.MessageType(message.Type), From: message.From, To: message.To, Term: message.Term,
		LogIndex: message.LogIndex, LogTerm: message.LogTerm, Entries: entries,
		LeaderCommit: message.LeaderCommit, Reject: message.Reject,
		RejectHint: message.RejectHint, Context: message.Context,
	}, nil
}
