package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"distributed-kv-store/internal/sharding"
)

type putRequest struct {
	Value string `json:"value"`
}

type shardStatus struct {
	Shard   string `json:"shard"`
	Leader  string `json:"leader"`
	BaseURL string `json:"base_url"`
}

// Router forwards client requests to the shard leader selected by the hash ring.
type Router struct {
	ring    *sharding.Ring
	leaders map[string]string
	client  *http.Client
}

// New creates a router from shard leader addresses.
func New(leaders map[string]string, virtualNodes int) *Router {
	shards := make([]string, 0, len(leaders))
	copied := make(map[string]string, len(leaders))
	for shard, baseURL := range leaders {
		shards = append(shards, shard)
		copied[shard] = strings.TrimRight(baseURL, "/")
	}

	return &Router{
		ring:    sharding.NewRing(shards, virtualNodes),
		leaders: copied,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// Handler exposes the router API.
func (r *Router) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", r.handleKV)
	mux.HandleFunc("/admin/shards", r.handleShards)
	mux.HandleFunc("/healthz", r.handleHealth)
	return mux
}

func (r *Router) handleKV(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, "/kv/")
	if key == "" || key == req.URL.Path {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	shard := r.ring.ShardFor(key)
	baseURL, ok := r.leaders[shard]
	if !ok {
		http.Error(w, "no shard leader available", http.StatusServiceUnavailable)
		return
	}

	targetURL := baseURL + "/kv/" + key
	var body io.Reader
	if req.Method == http.MethodPut {
		var put putRequest
		if err := json.NewDecoder(req.Body).Decode(&put); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		raw, err := json.Marshal(put)
		if err != nil {
			http.Error(w, "encode request", http.StatusInternalServerError)
			return
		}
		body = bytes.NewReader(raw)
	}

	proxyReq, err := http.NewRequestWithContext(req.Context(), req.Method, targetURL, body)
	if err != nil {
		http.Error(w, "build proxy request", http.StatusInternalServerError)
		return
	}
	if req.Method == http.MethodPut {
		proxyReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := r.client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("forward request: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("X-Shard-ID", shard)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (r *Router) handleShards(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	shards := make([]shardStatus, 0, len(r.leaders))
	for shard, baseURL := range r.leaders {
		shards = append(shards, shardStatus{
			Shard:   shard,
			Leader:  shard,
			BaseURL: baseURL,
		})
	}

	writeJSON(w, http.StatusOK, shards)
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
