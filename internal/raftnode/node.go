package raftnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-store/internal/persistence"
	"distributed-kv-store/internal/raft"
)

type putRequest struct {
	Value string `json:"value"`
}

type statusResponse struct {
	Status string `json:"status"`
}

type valueResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type metrics struct {
	requestVoteRPCs      uint64
	appendEntriesRPCs    uint64
	clientGets           uint64
	clientPuts           uint64
	clientDeletes        uint64
	replicationSuccesses uint64
	replicationFailures  uint64
	electionCampaigns    uint64
	electionWins         uint64
	appliedEntries       uint64
}

type statusPayload struct {
	NodeID      string            `json:"node_id"`
	Role        raft.Role         `json:"role"`
	LeaderID    string            `json:"leader_id"`
	Term        uint64            `json:"term"`
	CommitIndex int               `json:"commit_index"`
	LastApplied int               `json:"last_applied"`
	Peers       map[string]string `json:"peers"`
}

// Node combines a Raft state machine, transport, and durable KV engine.
type Node struct {
	mu      sync.Mutex
	id      string
	peers   map[string]string
	state   *raft.StateMachine
	store   *persistence.Store
	client  *http.Client
	metrics metrics
}

// New creates a node with a durable local store and Raft state machine.
func New(id string, peers map[string]string, store *persistence.Store) *Node {
	peerIDs := make([]string, 0, len(peers))
	peerURLs := make(map[string]string, len(peers))
	for peerID, peerURL := range peers {
		peerIDs = append(peerIDs, peerID)
		peerURLs[peerID] = peerURL
	}

	return &Node{
		id:    id,
		peers: peerURLs,
		state: raft.NewStateMachine(id, peerIDs),
		store: store,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// SetPeers replaces the current peer address book.
func (n *Node) SetPeers(peers map[string]string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.peers = make(map[string]string, len(peers))
	peerIDs := make([]string, 0, len(peers))
	for peerID, peerURL := range peers {
		n.peers[peerID] = peerURL
		peerIDs = append(peerIDs, peerID)
	}
	n.state.SetPeers(peerIDs)
}

// Handler exposes Raft RPCs and a small client API.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/request-vote", n.handleVote)
	mux.HandleFunc("/raft/append-entries", n.handleAppendEntries)
	mux.HandleFunc("/admin/campaign", n.handleCampaign)
	mux.HandleFunc("/admin/status", n.handleStatus)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/healthz", n.handleHealth)
	return mux
}

// Campaign requests votes from peers and tries to become leader.
func (n *Node) Campaign(ctx context.Context) error {
	atomic.AddUint64(&n.metrics.electionCampaigns, 1)

	n.mu.Lock()
	req := n.state.StartElection()
	peers := n.snapshotPeersLocked()
	n.mu.Unlock()

	for peerID, baseURL := range peers {
		var resp raft.VoteResponse
		if err := n.postJSON(ctx, baseURL+"/raft/request-vote", req, &resp); err != nil {
			continue
		}

		n.mu.Lock()
		n.state.HandleVoteResponse(peerID, resp)
		becameLeader := n.state.Role() == raft.RoleLeader
		n.mu.Unlock()

		if becameLeader {
			atomic.AddUint64(&n.metrics.electionWins, 1)
			return nil
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state.Role() != raft.RoleLeader {
		return fmt.Errorf("campaign did not reach quorum")
	}
	return nil
}

// ReplicateSet appends and commits a write through the Raft leader.
func (n *Node) ReplicateSet(ctx context.Context, key, value string) error {
	return n.replicateCommand(ctx, "set", key, value)
}

// ReplicateDelete appends and commits a delete through the Raft leader.
func (n *Node) ReplicateDelete(ctx context.Context, key string) error {
	return n.replicateCommand(ctx, "delete", key, "")
}

// Get reads a locally applied value.
func (n *Node) Get(key string) (string, bool) {
	return n.store.Get(key)
}

// Role returns the current role.
func (n *Node) Role() raft.Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.Role()
}

// LeaderID returns the known leader.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.LeaderID()
}

// Status returns a snapshot of the node state for observability.
func (n *Node) Status() statusPayload {
	n.mu.Lock()
	defer n.mu.Unlock()

	return statusPayload{
		NodeID:      n.id,
		Role:        n.state.Role(),
		LeaderID:    n.state.LeaderID(),
		Term:        n.state.CurrentTerm(),
		CommitIndex: n.state.CommitIndex(),
		LastApplied: n.state.LastApplied(),
		Peers:       n.snapshotPeersLocked(),
	}
}

func (n *Node) replicateCommand(ctx context.Context, operation, key, value string) error {
	n.mu.Lock()
	entry, index, err := n.state.AppendCommand(operation, key, value)
	if err != nil {
		n.mu.Unlock()
		return err
	}

	prevIndex := index - 1
	var prevTerm uint64
	if prevIndex >= 0 {
		prevEntry, ok := n.state.EntryAt(prevIndex)
		if !ok {
			n.mu.Unlock()
			return fmt.Errorf("missing previous log entry at %d", prevIndex)
		}
		prevTerm = prevEntry.Term
	}

	req := raft.AppendEntriesRequest{
		Term:         n.state.CurrentTerm(),
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      []raft.Entry{entry},
		LeaderCommit: n.state.CommitIndex(),
	}
	peers := n.snapshotPeersLocked()
	n.mu.Unlock()

	successes := 1
	for _, baseURL := range peers {
		var resp raft.AppendEntriesResponse
		if err := n.postJSON(ctx, baseURL+"/raft/append-entries", req, &resp); err != nil {
			continue
		}
		if resp.Success {
			successes++
		}
		if resp.Term > req.Term {
			n.mu.Lock()
			n.state.ObserveTerm(resp.Term, "")
			n.mu.Unlock()
			return fmt.Errorf("leader observed higher term %d", resp.Term)
		}
	}

	n.mu.Lock()
	if successes < n.state.ClusterSize()/2+1 {
		atomic.AddUint64(&n.metrics.replicationFailures, 1)
		n.mu.Unlock()
		return fmt.Errorf("replication did not reach quorum")
	}
	n.state.AdvanceCommitIndex(index)
	toApply := n.state.TakeCommittedEntries()
	currentTerm := n.state.CurrentTerm()
	n.mu.Unlock()

	if err := n.applyEntries(toApply); err != nil {
		atomic.AddUint64(&n.metrics.replicationFailures, 1)
		return err
	}
	atomic.AddUint64(&n.metrics.replicationSuccesses, 1)

	heartbeat := raft.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: index,
		PrevLogTerm:  entry.Term,
		LeaderCommit: index,
	}

	for _, baseURL := range peers {
		var resp raft.AppendEntriesResponse
		_ = n.postJSON(ctx, baseURL+"/raft/append-entries", heartbeat, &resp)
	}

	return nil
}

func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&n.metrics.requestVoteRPCs, 1)

	var req raft.VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	resp := n.state.HandleVoteRequest(req)
	n.mu.Unlock()

	writeJSON(w, http.StatusOK, resp)
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&n.metrics.appendEntriesRPCs, 1)

	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	resp := n.state.HandleAppendEntries(req)
	toApply := n.state.TakeCommittedEntries()
	n.mu.Unlock()

	if resp.Success {
		if err := n.applyEntries(toApply); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

func (n *Node) handleCampaign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := n.Campaign(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, statusResponse{Status: "leader"})
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || key == r.URL.Path {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		atomic.AddUint64(&n.metrics.clientGets, 1)
		value, ok := n.Get(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, valueResponse{Key: key, Value: value})
	case http.MethodPut:
		atomic.AddUint64(&n.metrics.clientPuts, 1)
		var req putRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := n.ReplicateSet(r.Context(), key, req.Value); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusCreated, valueResponse{Key: key, Value: req.Value})
	case http.MethodDelete:
		atomic.AddUint64(&n.metrics.clientDeletes, 1)
		if err := n.ReplicateDelete(r.Context(), key); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{Status: "deleted"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, n.Status())
}

func (n *Node) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	writeMetricLine(w, "raft_request_vote_rpcs_total", atomic.LoadUint64(&n.metrics.requestVoteRPCs))
	writeMetricLine(w, "raft_append_entries_rpcs_total", atomic.LoadUint64(&n.metrics.appendEntriesRPCs))
	writeMetricLine(w, "kv_client_get_total", atomic.LoadUint64(&n.metrics.clientGets))
	writeMetricLine(w, "kv_client_put_total", atomic.LoadUint64(&n.metrics.clientPuts))
	writeMetricLine(w, "kv_client_delete_total", atomic.LoadUint64(&n.metrics.clientDeletes))
	writeMetricLine(w, "raft_replication_success_total", atomic.LoadUint64(&n.metrics.replicationSuccesses))
	writeMetricLine(w, "raft_replication_failure_total", atomic.LoadUint64(&n.metrics.replicationFailures))
	writeMetricLine(w, "raft_campaign_total", atomic.LoadUint64(&n.metrics.electionCampaigns))
	writeMetricLine(w, "raft_campaign_win_total", atomic.LoadUint64(&n.metrics.electionWins))
	writeMetricLine(w, "raft_applied_entries_total", atomic.LoadUint64(&n.metrics.appliedEntries))
}

func (n *Node) applyEntries(entries []raft.Entry) error {
	for _, entry := range entries {
		switch entry.Operation {
		case "set":
			if err := n.store.Set(entry.Key, entry.Value); err != nil {
				return err
			}
		case "delete":
			if _, err := n.store.Delete(entry.Key); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation %q", entry.Operation)
		}
		atomic.AddUint64(&n.metrics.appliedEntries, 1)
	}
	return nil
}

func (n *Node) snapshotPeersLocked() map[string]string {
	out := make(map[string]string, len(n.peers))
	for peerID, peerURL := range n.peers {
		out[peerID] = peerURL
	}
	return out
}

func (n *Node) postJSON(ctx context.Context, url string, reqBody any, out any) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeMetricLine(w io.Writer, name string, value uint64) {
	_, _ = io.WriteString(w, name+" "+strconv.FormatUint(value, 10)+"\n")
}
