package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is a small durable store for the execution-engine.
//
// Design goals:
// - Crash-safe append-only log for state transitions.
// - Deterministic recovery by replaying state events in order.
// - No external dependencies (single-file durability).
//
// This is intentionally similar to other Phase1 components (ledger/eventlog),
// but scoped to execution intent state rather than the global trading ledger.
type Store struct {
	dir string

	mu  sync.Mutex
	seq int64

	eventsPath   string
	snapshotPath string

	eventsFile *os.File
	eventsW    *bufio.Writer
}

type Event struct {
	Seq int64  `json:"seq"`
	TS  int64  `json:"ts_ns"`
	Typ string `json:"type"`

	IntentID string          `json:"intent_id,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// Snapshot wraps an arbitrary JSON-serializable state plus a last applied sequence.
// We intentionally keep State as raw JSON to avoid generic methods (Go 1.21).
type Snapshot struct {
	Version string          `json:"version"`
	TS      int64           `json:"ts_ns"`
	LastSeq int64           `json:"last_seq"`
	State   json.RawMessage `json:"state"`
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:          dir,
		eventsPath:   filepath.Join(dir, "events.ndjson"),
		snapshotPath: filepath.Join(dir, "snapshot.json"),
	}

	// Initialize seq based on existing log.
	seq, err := scanLastSeq(s.eventsPath)
	if err != nil {
		return nil, err
	}
	s.seq = seq

	f, err := os.OpenFile(s.eventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.eventsFile = f
	s.eventsW = bufio.NewWriterSize(f, 256*1024)
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventsW != nil {
		_ = s.eventsW.Flush()
	}
	if s.eventsFile != nil {
		err := s.eventsFile.Close()
		s.eventsFile = nil
		s.eventsW = nil
		return err
	}
	return nil
}

func (s *Store) Append(typ string, intentID string, data any) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.eventsW == nil {
		return Event{}, errors.New("store is closed")
	}
	s.seq++
	ev := Event{
		Seq:      s.seq,
		TS:       time.Now().UnixNano(),
		Typ:      typ,
		IntentID: intentID,
	}
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return Event{}, err
		}
		ev.Data = json.RawMessage(b)
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return Event{}, err
	}
	if _, err := s.eventsW.Write(append(line, '\n')); err != nil {
		return Event{}, err
	}
	if err := s.eventsW.Flush(); err != nil {
		return Event{}, err
	}
	// Best-effort sync: keep the log durable across crashes.
	_ = s.eventsFile.Sync()
	return ev, nil
}

// LoadSnapshot reads snapshot.json and unmarshals its state into outState.
// It returns (false, 0, nil) if the snapshot does not exist.
func (s *Store) LoadSnapshot(outState any) (bool, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return false, 0, err
	}
	if outState != nil && len(snap.State) > 0 {
		if err := json.Unmarshal(snap.State, outState); err != nil {
			return false, 0, err
		}
	}
	return true, snap.LastSeq, nil
}

// SaveSnapshot writes snapshot.json atomically.
func (s *Store) SaveSnapshot(lastSeq int64, state any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := Snapshot{
		Version: "v1",
		TS:      time.Now().UnixNano(),
		LastSeq: lastSeq,
	}
	if snap.LastSeq == 0 {
		snap.LastSeq = s.seq
	}
	if state != nil {
		bs, err := json.Marshal(state)
		if err != nil {
			return err
		}
		snap.State = json.RawMessage(bs)
	}

	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.snapshotPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.snapshotPath)
}

// Replay replays events with Seq > afterSeq.
func (s *Store) Replay(afterSeq int64, fn func(ev Event) error) error {
	f, err := os.Open(s.eventsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for sc.Scan() {
		var ev Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}
		if ev.Seq <= afterSeq {
			continue
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return sc.Err()
}

func scanLastSeq(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var last int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for sc.Scan() {
		var ev Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}
		if ev.Seq > last {
			last = ev.Seq
		}
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("scan last seq: %w", err)
	}
	return last, nil
}

