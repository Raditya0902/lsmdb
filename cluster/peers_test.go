package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFilePeerResolverRefreshesAndRetainsLastValidDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	if err := os.WriteFile(path, []byte(`{"1":"node1:7001"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewFilePeerResolver(path, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if address, err := resolver.Resolve(ctx, 1); err != nil || address != "node1:7001" {
		t.Fatalf("initial resolve = (%q,%v)", address, err)
	}
	if err := os.WriteFile(path, []byte(`{"1":`), 0o644); err != nil {
		t.Fatal(err)
	}
	if address, err := resolver.Resolve(ctx, 1); err != nil || address != "node1:7001" {
		t.Fatalf("resolve after invalid update = (%q,%v)", address, err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"1":"node1-new:7101","4":"node4:7004"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	if address, err := resolver.Resolve(ctx, 1); err != nil || address != "node1-new:7101" {
		t.Fatalf("updated resolve = (%q,%v)", address, err)
	}
	if address, err := resolver.Resolve(ctx, 4); err != nil || address != "node4:7004" {
		t.Fatalf("new peer resolve = (%q,%v)", address, err)
	}
}

func TestFilePeerResolverRejectsInvalidInitialDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	if err := os.WriteFile(path, []byte(`{"zero":"node:7000"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFilePeerResolver(path, time.Second); err == nil {
		t.Fatal("invalid peer directory was accepted")
	}
}
