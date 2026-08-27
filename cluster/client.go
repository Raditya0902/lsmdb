package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lsmdbv1 "lsmdb/api/lsmdb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client retries requests across a static address list while preserving write identity.
type Client struct {
	addresses []string
	clientID  string
	sequence  atomic.Uint64
	writeMu   sync.Mutex
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
	clients   map[string]lsmdbv1.KVClient
	leader    string
}

// NewClient creates a client with a random stable identity for its lifetime.
func NewClient(addresses []string) (*Client, error) {
	if len(addresses) == 0 {
		return nil, errors.New("at least one cluster address is required")
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("create client identity: %w", err)
	}
	return &Client{
		addresses: append([]string(nil), addresses...), clientID: hex.EncodeToString(id),
		conns: make(map[string]*grpc.ClientConn), clients: make(map[string]lsmdbv1.KVClient),
	}, nil
}

// Put stores a value and retries the same client sequence through leader changes.
func (c *Client) Put(ctx context.Context, key, value []byte) (*lsmdbv1.WriteResponse, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	sequence := c.sequence.Add(1)
	request := &lsmdbv1.PutRequest{
		Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		ClientId: c.clientID, RequestSeq: sequence,
	}
	var response *lsmdbv1.WriteResponse
	err := c.retry(ctx, func(client lsmdbv1.KVClient) error {
		var err error
		response, err = client.Put(ctx, request)
		return err
	})
	return response, err
}

// Delete removes a key with the same retry guarantees as Put.
func (c *Client) Delete(ctx context.Context, key []byte) (*lsmdbv1.WriteResponse, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	sequence := c.sequence.Add(1)
	request := &lsmdbv1.DeleteRequest{Key: append([]byte(nil), key...), ClientId: c.clientID, RequestSeq: sequence}
	var response *lsmdbv1.WriteResponse
	err := c.retry(ctx, func(client lsmdbv1.KVClient) error {
		var err error
		response, err = client.Delete(ctx, request)
		return err
	})
	return response, err
}

// Get performs a linearizable point read.
func (c *Client) Get(ctx context.Context, key []byte) (*lsmdbv1.GetResponse, error) {
	request := &lsmdbv1.GetRequest{Key: append([]byte(nil), key...)}
	var response *lsmdbv1.GetResponse
	err := c.retry(ctx, func(client lsmdbv1.KVClient) error {
		var err error
		response, err = client.Get(ctx, request)
		return err
	})
	return response, err
}

// Status fetches status from the first reachable node; it does not require a leader.
func (c *Client) Status(ctx context.Context) (*lsmdbv1.StatusResponse, error) {
	var last error
	for _, address := range c.orderedAddresses() {
		client, err := c.client(address)
		if err != nil {
			last = err
			continue
		}
		response, err := client.Status(ctx, &lsmdbv1.StatusRequest{})
		if err == nil {
			return response, nil
		}
		last = err
	}
	return nil, last
}

// ChangeMembership replaces the voter set using Raft joint consensus.
func (c *Client) ChangeMembership(ctx context.Context, voters []uint64) (*lsmdbv1.ChangeMembershipResponse, error) {
	request := &lsmdbv1.ChangeMembershipRequest{VoterIds: append([]uint64(nil), voters...)}
	var response *lsmdbv1.ChangeMembershipResponse
	err := c.retry(ctx, func(client lsmdbv1.KVClient) error {
		var err error
		response, err = client.ChangeMembership(ctx, request)
		return err
	})
	return response, err
}

// Close releases cached gRPC connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, connection := range c.conns {
		if err := connection.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *Client) retry(ctx context.Context, call func(lsmdbv1.KVClient) error) error {
	var last error
	addresses := c.orderedAddresses()
	for attempt := 0; attempt < len(addresses)*20; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		address := addresses[attempt%len(addresses)]
		client, err := c.client(address)
		if err != nil {
			last = err
			continue
		}
		if err := call(client); err == nil {
			c.setLeader(address)
			return nil
		} else {
			last = err
			if status.Code(err) == codes.InvalidArgument {
				return err
			}
			hint, notLeader := leaderHint(err)
			if status.Code(err) == codes.FailedPrecondition && !notLeader {
				return err
			}
			if hint != "" {
				c.setLeader(hint)
				addresses = c.orderedAddresses()
			}
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if last == nil {
		last = errors.New("cluster request failed")
	}
	return last
}

func (c *Client) client(address string) (lsmdbv1.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if client := c.clients[address]; client != nil {
		return client, nil
	}
	connection, err := grpc.NewClient(
		address, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(kvstateMessageLimit), grpc.MaxCallSendMsgSize(kvstateMessageLimit),
		),
	)
	if err != nil {
		return nil, err
	}
	client := lsmdbv1.NewKVClient(connection)
	c.conns[address] = connection
	c.clients[address] = client
	return client, nil
}

const kvstateMessageLimit = (4 << 20) + (64 << 10)

func (c *Client) orderedAddresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ordered := make([]string, 0, len(c.addresses)+1)
	if c.leader != "" {
		ordered = append(ordered, c.leader)
	}
	for _, address := range c.addresses {
		if address != c.leader {
			ordered = append(ordered, address)
		}
	}
	return ordered
}

func (c *Client) setLeader(address string) {
	c.mu.Lock()
	c.leader = address
	c.mu.Unlock()
}

func leaderHint(err error) (string, bool) {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	for _, detail := range grpcStatus.Details() {
		if notLeader, ok := detail.(*lsmdbv1.NotLeader); ok {
			return notLeader.LeaderAddress, true
		}
	}
	return "", false
}
