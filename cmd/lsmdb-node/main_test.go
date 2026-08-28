package main

import "testing"

func TestParsePeersAllowsFileOnlyDiscovery(t *testing.T) {
	peers, err := parsePeers("")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("empty peer list parsed as %#v", peers)
	}
}
