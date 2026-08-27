package raftnet

import (
	"context"
	"sync"
	"testing"

	"lsmdb/internal/raft"
)

type recorder struct {
	mu       sync.Mutex
	messages []raft.Message
}

func (r *recorder) Step(_ context.Context, message raft.Message) error {
	r.mu.Lock()
	r.messages = append(r.messages, message)
	r.mu.Unlock()
	return nil
}

func TestNetworkPartitionPauseAndReorder(t *testing.T) {
	network := New()
	target := &recorder{}
	network.Register(2, target)
	adapter := network.Adapter(1)
	message := func(index uint64) raft.Message {
		return raft.Message{Type: raft.MsgAppend, From: 1, To: 2, LogIndex: index}
	}

	network.Partition([]uint64{1}, []uint64{2})
	if err := adapter.Send(context.Background(), message(1)); err == nil {
		t.Fatal("partitioned send succeeded")
	}
	network.Heal()
	network.Pause(2, true)
	if err := adapter.Send(context.Background(), message(1)); err == nil {
		t.Fatal("send to paused node succeeded")
	}
	network.Pause(2, false)
	network.Hold(1, 2, true)
	if err := adapter.Send(context.Background(), message(1)); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Send(context.Background(), message(2)); err != nil {
		t.Fatal(err)
	}
	if err := network.Release(1, 2, true); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if len(target.messages) != 2 || target.messages[0].LogIndex != 2 || target.messages[1].LogIndex != 1 {
		t.Fatalf("released order = %#v", target.messages)
	}
}
