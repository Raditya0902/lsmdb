// Package raftnet provides a deterministic, faultable in-memory Raft transport.
package raftnet

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"

	"lsmdb/internal/raft"
)

// Handler accepts an inbound Raft message.
type Handler interface {
	Step(context.Context, raft.Message) error
}

type link struct {
	drop  bool
	delay time.Duration
	hold  bool
	queue []raft.Message
}

// Network controls directional links and node availability.
type Network struct {
	mu       sync.Mutex
	handlers map[uint64]Handler
	paused   map[uint64]bool
	links    map[[2]uint64]*link
}

func New() *Network {
	return &Network{
		handlers: make(map[uint64]Handler), paused: make(map[uint64]bool),
		links: make(map[[2]uint64]*link),
	}
}

// Adapter returns the transport view for one node.
func (n *Network) Adapter(id uint64) *Adapter { return &Adapter{id: id, network: n} }

func (n *Network) Register(id uint64, handler Handler) {
	n.mu.Lock()
	n.handlers[id] = handler
	n.mu.Unlock()
}

func (n *Network) Unregister(id uint64) {
	n.mu.Lock()
	delete(n.handlers, id)
	n.mu.Unlock()
}

func (n *Network) Pause(id uint64, paused bool) {
	n.mu.Lock()
	n.paused[id] = paused
	n.mu.Unlock()
}

func (n *Network) Drop(from, to uint64, drop bool) {
	n.mu.Lock()
	n.link(from, to).drop = drop
	n.mu.Unlock()
}

func (n *Network) Delay(from, to uint64, delay time.Duration) {
	n.mu.Lock()
	n.link(from, to).delay = delay
	n.mu.Unlock()
}

// Hold queues messages on a directional link until Release is called.
func (n *Network) Hold(from, to uint64, hold bool) {
	n.mu.Lock()
	n.link(from, to).hold = hold
	n.mu.Unlock()
}

// Partition drops traffic in both directions between the two groups.
func (n *Network) Partition(left, right []uint64) {
	for _, from := range left {
		for _, to := range right {
			n.Drop(from, to, true)
			n.Drop(to, from, true)
		}
	}
}

// Heal restores all links and releases no held messages.
func (n *Network) Heal() {
	n.mu.Lock()
	for _, link := range n.links {
		link.drop = false
		link.delay = 0
		link.hold = false
		link.queue = nil
	}
	n.paused = make(map[uint64]bool)
	n.mu.Unlock()
}

// Release delivers held messages in FIFO or reverse order.
func (n *Network) Release(from, to uint64, reverse bool) error {
	n.mu.Lock()
	link := n.link(from, to)
	messages := append([]raft.Message(nil), link.queue...)
	link.queue = nil
	link.hold = false
	n.mu.Unlock()
	if reverse {
		for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
			messages[left], messages[right] = messages[right], messages[left]
		}
	}
	for _, message := range messages {
		if err := n.deliver(context.Background(), message); err != nil {
			return err
		}
	}
	return nil
}

func (n *Network) deliver(ctx context.Context, message raft.Message) error {
	n.mu.Lock()
	if n.paused[message.From] || n.paused[message.To] {
		n.mu.Unlock()
		return errors.New("raft node is paused")
	}
	link := n.link(message.From, message.To)
	if link.drop {
		n.mu.Unlock()
		return errors.New("raft message dropped")
	}
	if link.hold {
		link.queue = append(link.queue, message)
		n.mu.Unlock()
		return nil
	}
	delay := link.delay
	handler := n.handlers[message.To]
	n.mu.Unlock()
	if handler == nil {
		return fmt.Errorf("raft node %d is unavailable", message.To)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return handler.Step(ctx, message)
}

func (n *Network) link(from, to uint64) *link {
	key := [2]uint64{from, to}
	if n.links[key] == nil {
		n.links[key] = &link{}
	}
	return n.links[key]
}

// Adapter implements the raftnode transport seam.
type Adapter struct {
	id      uint64
	network *Network
}

func (a *Adapter) Send(ctx context.Context, message raft.Message) error {
	if message.From != a.id {
		return fmt.Errorf("adapter %d cannot send as node %d", a.id, message.From)
	}
	return a.network.deliver(ctx, message)
}

// SendSnapshot materializes bytes only in this deterministic in-memory adapter.
func (a *Adapter) SendSnapshot(ctx context.Context, message raft.Message, reader io.Reader, size uint64, checksum uint32) error {
	if size > uint64(^uint(0)>>1) {
		return errors.New("snapshot exceeds in-memory adapter limit")
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(size)+1))
	if err != nil {
		return err
	}
	if uint64(len(data)) != size || crc32.ChecksumIEEE(data) != checksum {
		return errors.New("snapshot source size or checksum mismatch")
	}
	if message.Snapshot == nil {
		return errors.New("snapshot message has no metadata")
	}
	copy := *message.Snapshot
	copy.Data = data
	message.Snapshot = &copy
	return a.Send(ctx, message)
}
