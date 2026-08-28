package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PeerResolver maps a Raft node ID to its current transport address. It does not
// define or mutate voter membership.
type PeerResolver interface {
	Resolve(context.Context, uint64) (string, error)
}

type staticPeerResolver map[uint64]string

func newStaticPeerResolver(peers map[uint64]string) staticPeerResolver {
	copy := make(staticPeerResolver, len(peers))
	for id, address := range peers {
		copy[id] = address
	}
	return copy
}

func (r staticPeerResolver) Resolve(_ context.Context, id uint64) (string, error) {
	if address := strings.TrimSpace(r[id]); id != 0 && address != "" {
		return address, nil
	}
	return "", fmt.Errorf("peer %d has no static address", id)
}

type fallbackPeerResolver struct {
	primary  PeerResolver
	fallback PeerResolver
}

func (r fallbackPeerResolver) Resolve(ctx context.Context, id uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r.primary != nil {
		if address, err := r.primary.Resolve(ctx, id); err == nil {
			return address, nil
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	return r.fallback.Resolve(ctx, id)
}

// FilePeerResolver refreshes a JSON object of node IDs to addresses. Updates
// should be published with atomic rename. A bad refresh retains the last valid
// directory so transient operator errors do not sever established routing.
type FilePeerResolver struct {
	mu          sync.Mutex
	path        string
	refresh     time.Duration
	lastRefresh time.Time
	peers       map[uint64]string
}

// NewFilePeerResolver opens a refreshable peer directory.
func NewFilePeerResolver(path string, refresh time.Duration) (*FilePeerResolver, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("peer directory path is required")
	}
	if refresh <= 0 {
		refresh = time.Second
	}
	resolver := &FilePeerResolver{path: path, refresh: refresh}
	if err := resolver.refreshLocked(); err != nil {
		return nil, err
	}
	return resolver, nil
}

func (r *FilePeerResolver) Resolve(ctx context.Context, id uint64) (string, error) {
	if id == 0 {
		return "", errors.New("peer ID must be positive")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastRefresh) >= r.refresh {
		_ = r.refreshLocked()
	}
	if address := r.peers[id]; address != "" {
		return address, nil
	}
	return "", fmt.Errorf("peer %d is absent from directory %s", id, r.path)
}

func (r *FilePeerResolver) refreshLocked() error {
	r.lastRefresh = time.Now()
	data, err := os.ReadFile(r.path)
	if err != nil {
		return fmt.Errorf("read peer directory: %w", err)
	}
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode peer directory: %w", err)
	}
	peers := make(map[uint64]string, len(raw))
	for key, value := range raw {
		id, err := strconv.ParseUint(strings.TrimSpace(key), 10, 64)
		address := strings.TrimSpace(value)
		if err != nil || id == 0 || address == "" {
			return fmt.Errorf("invalid peer directory entry %q=%q", key, value)
		}
		peers[id] = address
	}
	r.peers = peers
	return nil
}
