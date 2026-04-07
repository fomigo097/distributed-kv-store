package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-kv-store/internal/store"
)

func TestPutGetDeleteFlow(t *testing.T) {
	handler := NewHandler(store.New()).Routes()

	putReq := httptest.NewRequest(http.MethodPut, "/kv/name", bytes.NewBufferString(`{"value":"raft"}`))
	putReq.Header.Set("Content-Type", "application/json")
	putRes := httptest.NewRecorder()
	handler.ServeHTTP(putRes, putReq)

	if putRes.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, putRes.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/name", nil)
	getRes := httptest.NewRecorder()
	handler.ServeHTTP(getRes, getReq)

	if getRes.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, getRes.Code)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/kv/name", nil)
	delRes := httptest.NewRecorder()
	handler.ServeHTTP(delRes, delReq)

	if delRes.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, delRes.Code)
	}
}

func TestMissingKeyReturnsNotFound(t *testing.T) {
	handler := NewHandler(store.New()).Routes()

	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, res.Code)
	}
}

func TestInvalidBodyReturnsBadRequest(t *testing.T) {
	handler := NewHandler(store.New()).Routes()

	req := httptest.NewRequest(http.MethodPut, "/kv/name", bytes.NewBufferString(`{"value"`))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, res.Code)
	}
}
