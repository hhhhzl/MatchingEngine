package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// microSidecarClient wraps the Rust execution-micro sidecar binary.
// It keeps the process alive and uses JSON lines over stdin/stdout.
//
// Rationale:
// - Avoids FFI complexity.
// - Keeps the micro decision logic in Rust (deterministic, hot-path ready).
// - Allows the Go execution-engine to inject dynamic constraints (e.g. max_slippage_bps).
type microSidecarClient struct {
	path string

	mu  sync.Mutex
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
}

func newMicroSidecarClient(path string) *microSidecarClient {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return &microSidecarClient{path: path}
}

func (m *microSidecarClient) ensureStarted() error {
	if m == nil {
		return errors.New("micro sidecar disabled")
	}
	if m.cmd != nil {
		return nil
	}
	cmd := exec.Command(m.path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	// Forward stderr so sidecar errors are visible in service logs.
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	m.cmd = cmd
	m.in = stdin
	m.out = bufio.NewReaderSize(stdout, 256*1024)
	return nil
}

// decide sends one MicroInput and returns one MicroDecision.
func (m *microSidecarClient) decide(input map[string]any) (map[string]any, error) {
	if m == nil {
		return nil, errors.New("micro sidecar disabled")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureStarted(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if _, err := m.in.Write(append(b, '\n')); err != nil {
		_ = m.restartLocked()
		return nil, err
	}

	// Read one line response.
	line, err := m.out.ReadBytes('\n')
	if err != nil {
		_ = m.restartLocked()
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(bytesTrimSpace(line), &out); err != nil {
		return map[string]any{"action": "noop", "reason": "sidecar_parse_error"}, nil
	}
	return out, nil
}

func (m *microSidecarClient) restartLocked() error {
	if m.cmd != nil && m.cmd.Process != nil {
		_ = m.cmd.Process.Kill()
		_, _ = m.cmd.Process.Wait()
	}
	m.cmd = nil
	m.in = nil
	m.out = nil
	// small backoff to avoid hot-looping on crash
	time.Sleep(50 * time.Millisecond)
	return nil
}

func bytesTrimSpace(b []byte) []byte {
	s := strings.TrimSpace(string(b))
	return []byte(s)
}

