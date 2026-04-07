package persistence

import (
	"path/filepath"
	"testing"
)

func TestStoreRecoversFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.wal")

	first, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if err := first.Set("pet", "cat"); err != nil {
		t.Fatalf("set pet: %v", err)
	}
	if err := first.Set("color", "blue"); err != nil {
		t.Fatalf("set color: %v", err)
	}
	deleted, err := first.Delete("color")
	if err != nil {
		t.Fatalf("delete color: %v", err)
	}
	if !deleted {
		t.Fatalf("expected delete to succeed")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer second.Close()

	if got, ok := second.Get("pet"); !ok || got != "cat" {
		t.Fatalf("expected pet=cat after replay, got %q, ok=%v", got, ok)
	}

	if _, ok := second.Get("color"); ok {
		t.Fatalf("expected color to stay deleted after replay")
	}
}

func TestDeleteMissingKeyDoesNotWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.wal")

	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	deleted, err := s.Delete("missing")
	if err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if deleted {
		t.Fatalf("expected delete on missing key to return false")
	}
}
