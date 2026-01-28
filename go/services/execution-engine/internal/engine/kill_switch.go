package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// killSwitchClient is a small cached client for the Kill Switch service.
// It mirrors the approach used by RiskGate, but is used here to adjust execution behavior
// (pause/reduce-only/flatten) before emitting orders.
type killSwitchClient struct {
	baseURL string
	c       *http.Client

	mu    sync.Mutex
	cache map[string]cachedMode // key: "" (global) or account_id
}

type cachedMode struct {
	mode      string
	expiresAt time.Time
}

func newKillSwitchClient(baseURL string) *killSwitchClient {
	baseURL = strings.TrimRight(baseURL, "/")
	return &killSwitchClient{
		baseURL: baseURL,
		c: &http.Client{
			Timeout: 150 * time.Millisecond,
		},
		cache: map[string]cachedMode{},
	}
}

func (k *killSwitchClient) getMode(ctx context.Context, accountID string) string {
	if k == nil || k.baseURL == "" {
		return "ON"
	}
	now := time.Now()

	k.mu.Lock()
	if cm, ok := k.cache[accountID]; ok && now.Before(cm.expiresAt) {
		mode := cm.mode
		k.mu.Unlock()
		return mode
	}
	k.mu.Unlock()

	u := k.baseURL + "/mode"
	if accountID != "" {
		u = u + "?account_id=" + accountID
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := k.c.Do(req)
	if err != nil {
		return "ON"
	}
	defer resp.Body.Close()
	var obj map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return "ON"
	}
	mode, _ := obj["mode"].(string)
	if mode == "" {
		mode = "ON"
	}
	k.mu.Lock()
	k.cache[accountID] = cachedMode{mode: mode, expiresAt: now.Add(200 * time.Millisecond)}
	k.mu.Unlock()
	return mode
}

