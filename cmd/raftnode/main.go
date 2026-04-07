package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/raftnode"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node-1"
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "7001"
	}

	walPath := os.Getenv("WAL_PATH")
	if walPath == "" {
		walPath = "data/" + nodeID + ".wal"
	}

	store, err := persistence.OpenStore(walPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	node := raftnode.New(nodeID, parsePeers(os.Getenv("PEERS")), store)

	addr := ":" + httpPort
	log.Printf("starting raft node %s on %s", nodeID, addr)
	if err := http.ListenAndServe(addr, node.Handler()); err != nil {
		log.Fatalf("raft node stopped: %v", err)
	}
}

func parsePeers(raw string) map[string]string {
	peers := make(map[string]string)
	if strings.TrimSpace(raw) == "" {
		return peers
	}

	for _, item := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			continue
		}
		peers[parts[0]] = parts[1]
	}

	return peers
}
