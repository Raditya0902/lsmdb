package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"lsmdb/cluster"
)

func main() {
	id := flag.Uint64("id", 0, "non-zero node ID")
	listen := flag.String("listen", "", "gRPC listen address")
	metrics := flag.String("metrics", "", "Prometheus/health HTTP listen address")
	dataDir := flag.String("data-dir", "", "persistent node data directory")
	peerList := flag.String("peers", "", "comma-separated static peers: 1=host:port,2=host:port")
	snapshotThreshold := flag.Uint64("snapshot-threshold", 1000, "applied entries between Raft snapshots (0 uses default)")
	flag.Parse()

	peers, err := parsePeers(*peerList)
	if err != nil {
		log.Fatal(err)
	}
	node, err := cluster.StartNode(cluster.NodeConfig{
		ID: *id, ListenAddress: *listen, MetricsAddress: *metrics,
		DataDir: *dataDir, Peers: peers, SnapshotThreshold: *snapshotThreshold,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("lsmdb node %d listening on %s", *id, node.Address())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	if err := node.Close(); err != nil {
		log.Printf("close node: %v", err)
		os.Exit(1)
	}
}

func parsePeers(value string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer %q; use ID=host:port", item)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid peer ID %q", parts[0])
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate peer ID %d", id)
		}
		peers[id] = parts[1]
	}
	return peers, nil
}
