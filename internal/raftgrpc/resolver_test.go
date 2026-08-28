package raftgrpc

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type mutableResolver struct {
	mu      sync.Mutex
	address string
}

func (r *mutableResolver) Resolve(_ context.Context, id uint64) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == 0 || r.address == "" {
		return "", fmt.Errorf("unknown peer %d", id)
	}
	return r.address, nil
}

func (r *mutableResolver) set(address string) {
	r.mu.Lock()
	r.address = address
	r.mu.Unlock()
}

func TestTransportRotatesConnectionWhenResolvedAddressChanges(t *testing.T) {
	resolver := &mutableResolver{address: "127.0.0.1:7001"}
	transport := NewWithResolver(resolver)
	defer transport.Close()
	if _, err := transport.client(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	first := transport.conns[1]
	if _, err := transport.client(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if transport.conns[1] != first {
		t.Fatal("unchanged address rotated connection")
	}
	resolver.set("127.0.0.1:7101")
	if _, err := transport.client(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if transport.conns[1] == first || transport.addresses[1] != "127.0.0.1:7101" {
		t.Fatal("changed address did not rotate connection")
	}
}
