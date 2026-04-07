package raftnode

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/raft"
)

func TestThreeNodeClusterElectionAndReplication(t *testing.T) {
	type runningNode struct {
		node   *Node
		server *httptest.Server
		store  *persistence.Store
	}

	newRunningNode := func(id string) runningNode {
		store, err := persistence.OpenStore(filepath.Join(t.TempDir(), id+".wal"))
		if err != nil {
			t.Fatalf("open store for %s: %v", id, err)
		}

		node := New(id, nil, store)
		server := httptest.NewServer(node.Handler())

		return runningNode{
			node:   node,
			server: server,
			store:  store,
		}
	}

	n1 := newRunningNode("node-1")
	n2 := newRunningNode("node-2")
	n3 := newRunningNode("node-3")

	defer n1.server.Close()
	defer n2.server.Close()
	defer n3.server.Close()
	defer n1.store.Close()
	defer n2.store.Close()
	defer n3.store.Close()

	n1.node.SetPeers(map[string]string{
		"node-2": n2.server.URL,
		"node-3": n3.server.URL,
	})
	n2.node.SetPeers(map[string]string{
		"node-1": n1.server.URL,
		"node-3": n3.server.URL,
	})
	n3.node.SetPeers(map[string]string{
		"node-1": n1.server.URL,
		"node-2": n2.server.URL,
	})

	if err := n1.node.Campaign(context.Background()); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if n1.node.Role() != raft.RoleLeader {
		t.Fatalf("expected node-1 to become leader, got %s", n1.node.Role())
	}

	if err := n1.node.ReplicateSet(context.Background(), "pet", "cat"); err != nil {
		t.Fatalf("replicate set: %v", err)
	}

	nodes := []runningNode{n1, n2, n3}
	for _, node := range nodes {
		got, ok := node.node.Get("pet")
		if !ok || got != "cat" {
			t.Fatalf("expected %s to have pet=cat, got %q ok=%v", node.node.id, got, ok)
		}
	}

	if err := n1.node.ReplicateDelete(context.Background(), "pet"); err != nil {
		t.Fatalf("replicate delete: %v", err)
	}
	for _, node := range nodes {
		if _, ok := node.node.Get("pet"); ok {
			t.Fatalf("expected %s to delete pet", node.node.id)
		}
	}
}
