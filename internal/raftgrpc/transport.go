// Package raftgrpc adapts Raft messages to the generated gRPC transport.
package raftgrpc

import (
	"context"
	"fmt"
	"sync"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport caches one gRPC connection per static peer.
type Transport struct {
	mu      sync.Mutex
	peers   map[uint64]string
	conns   map[uint64]*grpc.ClientConn
	clients map[uint64]lsmdbv1.RaftClient
}

func New(peers map[uint64]string) *Transport {
	copy := make(map[uint64]string, len(peers))
	for id, address := range peers {
		copy[id] = address
	}
	return &Transport{peers: copy, conns: make(map[uint64]*grpc.ClientConn), clients: make(map[uint64]lsmdbv1.RaftClient)}
}

func (t *Transport) Send(ctx context.Context, message raft.Message) error {
	client, err := t.client(message.To)
	if err != nil {
		return err
	}
	_, err = client.Send(ctx, ToProto(message))
	return err
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
	if message == nil || message.Type > uint32(raft.MsgAppendResponse) {
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
