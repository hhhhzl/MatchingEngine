package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreAppendReplaySnapshot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.Append("a", "i1", map[string]any{"x": 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := s.Append("b", "i2", map[string]any{"y": "z"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	type snapState struct {
		Foo string `json:"foo"`
	}
	if err := s.SaveSnapshot(0, snapState{Foo: "bar"}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	var loaded snapState
	ok, lastSeq, err := s.LoadSnapshot(&loaded)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !ok {
		t.Fatalf("expected snapshot to exist")
	}
	if lastSeq <= 0 {
		t.Fatalf("expected lastSeq > 0, got %d", lastSeq)
	}
	if loaded.Foo != "bar" {
		t.Fatalf("unexpected snapshot state: %+v", loaded)
	}

	// Replay should see no events when afterSeq is lastSeq.
	seen := 0
	if err := s.Replay(lastSeq, func(_ Event) error {
		seen++
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if seen != 0 {
		t.Fatalf("expected 0 events after lastSeq, got %d", seen)
	}

	// Snapshot file should exist.
	if _, err := os.Stat(filepath.Join(dir, "snapshot.json")); err != nil {
		t.Fatalf("snapshot.json missing: %v", err)
	}
}

