package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/raftnode"
)

func TestRouterRoutesRequestsToAssignedShardLeader(t *testing.T) {
	makeNode := func(id string) (*raftnode.Node, *httptest.Server, *persistence.Store) {
		store, err := persistence.OpenStore(filepath.Join(t.TempDir(), id+".wal"))
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		node := raftnode.New(id, nil, store)
		server := httptest.NewServer(node.Handler())
		return node, server, store
	}

	nodeA, serverA, storeA := makeNode("shard-a")
	defer serverA.Close()
	defer storeA.Close()

	nodeB, serverB, storeB := makeNode("shard-b")
	defer serverB.Close()
	defer storeB.Close()

	router := New(map[string]string{
		"shard-a": serverA.URL,
		"shard-b": serverB.URL,
	}, 32)

	if err := nodeA.Campaign(t.Context()); err != nil {
		t.Fatalf("campaign shard-a: %v", err)
	}
	if err := nodeB.Campaign(t.Context()); err != nil {
		t.Fatalf("campaign shard-b: %v", err)
	}

	key := "pet"
	shard := router.ring.ShardFor(key)

	req := httptest.NewRequest(http.MethodPut, "/kv/"+key, bytes.NewBufferString(`{"value":"cat"}`))
	res := httptest.NewRecorder()
	router.Handler().ServeHTTP(res, req)

	if res.Code != http.StatusCreated {
		t.Fatalf("expected created, got %d body=%s", res.Code, res.Body.String())
	}
	if res.Header().Get("X-Shard-ID") != shard {
		t.Fatalf("expected shard header %q, got %q", shard, res.Header().Get("X-Shard-ID"))
	}

	checkNode := nodeA
	otherNode := nodeB
	if shard == "shard-b" {
		checkNode = nodeB
		otherNode = nodeA
	}

	if value, ok := checkNode.Get(key); !ok || value != "cat" {
		t.Fatalf("expected routed shard to store value, got %q ok=%v", value, ok)
	}
	if _, ok := otherNode.Get(key); ok {
		t.Fatalf("expected non-owning shard to remain untouched")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/"+key, nil)
	getRes := httptest.NewRecorder()
	router.Handler().ServeHTTP(getRes, getReq)

	if getRes.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", getRes.Code)
	}

	var payload map[string]string
	if err := json.Unmarshal(getRes.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal get response: %v", err)
	}
	if payload["value"] != "cat" {
		t.Fatalf("expected value cat, got %q", payload["value"])
	}
}

func TestRouterShardInventoryEndpoint(t *testing.T) {
	router := New(map[string]string{
		"shard-a": "http://127.0.0.1:7001",
		"shard-b": "http://127.0.0.1:7011",
	}, 16)

	req := httptest.NewRequest(http.MethodGet, "/admin/shards", nil)
	res := httptest.NewRecorder()
	router.Handler().ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", res.Code)
	}
}
