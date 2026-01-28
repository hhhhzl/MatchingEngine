package wire

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// GatewayClient is a small helper around the Event Gateway TCP protocol.
// It is intentionally simple and mirrors the patterns used by Phase1 Go services.
type GatewayClient struct {
	addr string

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

func NewGatewayClient(addr string) *GatewayClient {
	return &GatewayClient{addr: addr}
}

func (c *GatewayClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	_ = conn.(*net.TCPConn).SetNoDelay(true)
	c.conn = conn
	c.r = bufio.NewReaderSize(conn, 256*1024)
	return nil
}

func (c *GatewayClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.r = nil
	}
}

func (c *GatewayClient) Subscribe(topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("gateway not connected")
	}
	frame := map[string]any{"op": "sub", "topics": topics}
	wire, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(append(wire, '\n'))
	return err
}

func (c *GatewayClient) Publish(topic string, record EventLogRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("gateway not connected")
	}
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	frame := map[string]any{"op": "pub", "topic": topic, "msg": json.RawMessage(b)}
	wire, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(append(wire, '\n'))
	return err
}

// Read blocks until it reads one GatewayMessage.
// It returns (nil, io.EOF) if the connection is closed.
func (c *GatewayClient) Read() (*GatewayMessage, error) {
	c.mu.Lock()
	r := c.r
	c.mu.Unlock()

	if r == nil {
		return nil, errors.New("gateway not connected")
	}
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	lineStr := strings.TrimSpace(string(line))
	if lineStr == "" {
		return nil, nil
	}
	var gm GatewayMessage
	if err := json.Unmarshal([]byte(lineStr), &gm); err != nil {
		return nil, nil
	}
	if gm.Topic == "" || len(gm.Msg) == 0 {
		return nil, nil
	}
	return &gm, nil
}

// ReconnectLoop runs fn with a connected client and automatic reconnect.
// The callback should return io.EOF to force a reconnect.
func (c *GatewayClient) ReconnectLoop(topics []string, fn func() error) {
	backoff := 200 * time.Millisecond
	for {
		if err := c.Connect(); err != nil {
			time.Sleep(backoff)
			backoff = minDur(5*time.Second, backoff*2)
			continue
		}
		if err := c.Subscribe(topics); err != nil {
			c.Close()
			time.Sleep(backoff)
			backoff = minDur(5*time.Second, backoff*2)
			continue
		}
		backoff = 200 * time.Millisecond
		if err := fn(); err != nil {
			// Any error triggers reconnect; keep the loop simple and robust.
			c.Close()
			if err != io.EOF {
				time.Sleep(backoff)
			}
			continue
		}
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

