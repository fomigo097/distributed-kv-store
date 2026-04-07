package store

import "testing"

func TestStoreSetGetDelete(t *testing.T) {
	s := New()

	s.Set("language", "go")

	got, ok := s.Get("language")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if got != "go" {
		t.Fatalf("expected value %q, got %q", "go", got)
	}

	deleted := s.Delete("language")
	if !deleted {
		t.Fatalf("expected delete to return true")
	}

	_, ok = s.Get("language")
	if ok {
		t.Fatalf("expected key to be deleted")
	}
}

func TestStoreDeleteMissingKey(t *testing.T) {
	s := New()

	deleted := s.Delete("missing")
	if deleted {
		t.Fatalf("expected delete on missing key to return false")
	}
}
