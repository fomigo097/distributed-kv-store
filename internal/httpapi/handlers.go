package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"distributed-kv-store/internal/store"
)

type putRequest struct {
	Value string `json:"value"`
}

type valueResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type statusResponse struct {
	Status string `json:"status"`
}

// Handler exposes the HTTP surface for the KV store.
type Handler struct {
	store *store.Store
}

// NewHandler creates a new HTTP handler.
func NewHandler(s *store.Store) *Handler {
	return &Handler{store: s}
}

// Routes returns the HTTP routes for the service.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/kv/", h.handleKey)
	return mux
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handler) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || key == r.URL.Path {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, key string) {
	value, ok := h.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, valueResponse{
		Key:   key,
		Value: value,
	})
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	defer r.Body.Close()

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	h.store.Set(key, req.Value)

	writeJSON(w, http.StatusCreated, valueResponse{
		Key:   key,
		Value: req.Value,
	})
}

func (h *Handler) handleDelete(w http.ResponseWriter, key string) {
	deleted := h.store.Delete(key)
	if !deleted {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "deleted"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
