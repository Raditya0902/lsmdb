// Package manifest atomically publishes the set of SSTables that make up a DB.
package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	fileName = "MANIFEST"
	version  = 1
)

// State is the durable description of a database generation. SSTables are
// ordered oldest to newest. AppliedIndex is only used by replica mode.
type State struct {
	Version      int      `json:"version"`
	SSTables     []string `json:"sstables"`
	NextSST      uint64   `json:"next_sst"`
	AppliedIndex uint64   `json:"applied_index,omitempty"`
}

// New returns an empty manifest at the current format version.
func New() State { return State{Version: version} }

// Load reads the manifest in dir. The boolean is false when no manifest has
// been published yet, allowing older databases to be bootstrapped by scanning.
func Load(dir string) (State, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("read manifest: %w", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, false, fmt.Errorf("decode manifest: %w", err)
	}
	if err := state.Validate(); err != nil {
		return State{}, false, err
	}
	return state, true, nil
}

// Store atomically publishes state by syncing a temporary file, renaming it,
// and syncing the directory. A crash exposes either the old or new generation.
func Store(dir string, state State) error {
	state.Version = version
	if err := state.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	data = append(data, '\n')

	tmpPath := filepath.Join(dir, ".MANIFEST.tmp")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create manifest temp: %w", err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close manifest: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, fileName)); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	removeTemp = false

	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open manifest dir: %w", err)
	}
	defer d.Close() //nolint:errcheck
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync manifest dir: %w", err)
	}
	return nil
}

// Validate rejects paths that could escape the DB directory and malformed
// generations before any files are opened or published.
func (s State) Validate() error {
	if s.Version != version {
		return fmt.Errorf("manifest version %d is unsupported", s.Version)
	}
	seen := make(map[string]struct{}, len(s.SSTables))
	for _, name := range s.SSTables {
		if name == "" || filepath.Base(name) != name || !strings.HasSuffix(name, ".sst") {
			return fmt.Errorf("invalid manifest SSTable name %q", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate manifest SSTable %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}
