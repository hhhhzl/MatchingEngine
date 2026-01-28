package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// GuardrailsConfigV2 defines execution-level guardrails.
// It is intentionally explicit and deterministic: all decisions are based on
// observable metrics from the event stream (ORDER_CMD/EXEC_REPORT/MD_TICK).
type GuardrailsConfigV2 struct {
	Version string `yaml:"version"`

	// WindowSize controls how many samples we keep for percentile metrics.
	WindowSize int `yaml:"window_size"`

	// CooldownSec enforces a minimum time between two guardrail actions per intent.
	CooldownSec int `yaml:"cooldown_sec"`

	// DegradeLadder is the ordered list of algorithms for degrade actions.
	// Example: ["is_min_v1","pov","twap"]
	DegradeLadder []string `yaml:"degrade_ladder"`

	Thresholds struct {
		SlippageP95BpsMax   float64 `yaml:"slippage_p95_bps_max"`
		RejectRateMax       float64 `yaml:"reject_rate_max"`
		AckLatencyP95MsMax  float64 `yaml:"ack_latency_p95_ms_max"`
		FillLatencyP95MsMax float64 `yaml:"fill_latency_p95_ms_max"`
		MDStalenessP95MsMax float64 `yaml:"md_staleness_p95_ms_max"`
	} `yaml:"thresholds"`

	Actions struct {
		OnSlippage string `yaml:"on_slippage"` // degrade | pause
		OnReject   string `yaml:"on_reject"`   // pause | degrade
		OnLatency  string `yaml:"on_latency"`  // pause_cancel_all
		OnMDStale  string `yaml:"on_md_stale"` // pause_cancel_all

		// If true, a PAUSE action will also attempt a release-controller rollback
		// when intent.tags.release_id is present and ReleaseControllerURL is configured.
		RollbackOnPause bool `yaml:"rollback_on_pause"`
	} `yaml:"actions"`
}

func defaultGuardrails() GuardrailsConfigV2 {
	var cfg GuardrailsConfigV2
	cfg.Version = "v2"
	cfg.WindowSize = 200
	cfg.CooldownSec = 30
	cfg.DegradeLadder = []string{"is_min_v1", "pov", "twap"}
	cfg.Thresholds.SlippageP95BpsMax = 25
	cfg.Thresholds.RejectRateMax = 0.2
	cfg.Thresholds.AckLatencyP95MsMax = 300
	cfg.Thresholds.FillLatencyP95MsMax = 1500
	cfg.Thresholds.MDStalenessP95MsMax = 500
	cfg.Actions.OnSlippage = "degrade"
	cfg.Actions.OnReject = "pause"
	cfg.Actions.OnLatency = "pause_cancel_all"
	cfg.Actions.OnMDStale = "pause_cancel_all"
	cfg.Actions.RollbackOnPause = false
	return cfg
}

type guardrails struct {
	mu  sync.Mutex
	cfg GuardrailsConfigV2

	byIntent map[string]*guardIntent

	lastLoadedAt time.Time
	lastMTime    time.Time
	path         string

	releaseControllerURL string
	orgID                string
	userID               string
}

type guardIntent struct {
	// Samples (ring buffers)
	slippageBps *floatRing
	ackLatNS    *floatRing
	fillLatNS   *floatRing
	mdStaleNS   *floatRing

	orders  int
	rejects int

	lastActionAt time.Time
	degradeIdx   int
}

func newGuardrails(path string, releaseControllerURL string, orgID string, userID string) *guardrails {
	gr := &guardrails{
		cfg:                  defaultGuardrails(),
		byIntent:             map[string]*guardIntent{},
		path:                 path,
		releaseControllerURL: releaseControllerURL,
		orgID:                orgID,
		userID:               userID,
	}
	_ = gr.reloadIfNeeded()
	return gr
}

func (g *guardrails) reloadIfNeeded() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.path == "" {
		return nil
	}
	abs := g.path
	if !filepath.IsAbs(abs) {
		// Treat relative paths as relative to CWD.
		abs = g.path
	}
	st, err := os.Stat(abs)
	if err != nil {
		return nil
	}
	mt := st.ModTime()
	if !g.lastMTime.IsZero() && !mt.After(g.lastMTime) {
		return nil
	}
	raw, err := os.ReadFile(abs)
	if err != nil {
		return nil
	}
	var cfg GuardrailsConfigV2
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	if cfg.Version == "" {
		return errors.New("guardrails missing version")
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 200
	}
	if cfg.CooldownSec <= 0 {
		cfg.CooldownSec = 30
	}
	if len(cfg.DegradeLadder) == 0 {
		cfg.DegradeLadder = []string{"is_min_v1", "pov", "twap"}
	}
	g.cfg = cfg
	g.lastMTime = mt
	g.lastLoadedAt = time.Now()
	// Re-init rings with new size (keep simple; reset buffers).
	for k := range g.byIntent {
		g.byIntent[k] = g.newIntent()
	}
	return nil
}

func (g *guardrails) newIntent() *guardIntent {
	return &guardIntent{
		slippageBps: newFloatRing(g.cfg.WindowSize),
		ackLatNS:    newFloatRing(g.cfg.WindowSize),
		fillLatNS:   newFloatRing(g.cfg.WindowSize),
		mdStaleNS:   newFloatRing(g.cfg.WindowSize),
	}
}

func (g *guardrails) intent(intentID string) *guardIntent {
	gi := g.byIntent[intentID]
	if gi == nil {
		gi = g.newIntent()
		g.byIntent[intentID] = gi
	}
	return gi
}

func (g *guardrails) observeOrder(intentID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gi := g.intent(intentID)
	gi.orders++
}

func (g *guardrails) observeReject(intentID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gi := g.intent(intentID)
	gi.rejects++
}

func (g *guardrails) observeSlippage(intentID string, bps float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.intent(intentID).slippageBps.add(bps)
}

func (g *guardrails) observeAckLatency(intentID string, ns float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.intent(intentID).ackLatNS.add(ns)
}

func (g *guardrails) observeFillLatency(intentID string, ns float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.intent(intentID).fillLatNS.add(ns)
}

func (g *guardrails) observeMDStaleness(intentID string, ns float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.intent(intentID).mdStaleNS.add(ns)
}

type guardAction struct {
	Kind   string
	Reason string
	// DegradeTo is set when Kind == "degrade".
	DegradeTo string
}

func (g *guardrails) evaluate(intentID string) *guardAction {
	g.mu.Lock()
	defer g.mu.Unlock()

	gi := g.intent(intentID)
	// Cooldown.
	if !gi.lastActionAt.IsZero() && time.Since(gi.lastActionAt) < time.Duration(g.cfg.CooldownSec)*time.Second {
		return nil
	}

	// Percentile checks.
	slipP95 := percentile(gi.slippageBps.snapshot(), 0.95)
	ackP95Ms := percentile(gi.ackLatNS.snapshot(), 0.95) / 1e6
	fillP95Ms := percentile(gi.fillLatNS.snapshot(), 0.95) / 1e6
	mdP95Ms := percentile(gi.mdStaleNS.snapshot(), 0.95) / 1e6

	rejRate := 0.0
	if gi.orders > 0 {
		rejRate = float64(gi.rejects) / float64(gi.orders)
	}

	if g.cfg.Thresholds.MDStalenessP95MsMax > 0 && mdP95Ms > g.cfg.Thresholds.MDStalenessP95MsMax {
		gi.lastActionAt = time.Now()
		return &guardAction{Kind: g.cfg.Actions.OnMDStale, Reason: fmt.Sprintf("md_staleness_p95_ms=%.1f", mdP95Ms)}
	}
	if g.cfg.Thresholds.AckLatencyP95MsMax > 0 && ackP95Ms > g.cfg.Thresholds.AckLatencyP95MsMax {
		gi.lastActionAt = time.Now()
		return &guardAction{Kind: g.cfg.Actions.OnLatency, Reason: fmt.Sprintf("ack_latency_p95_ms=%.1f", ackP95Ms)}
	}
	if g.cfg.Thresholds.FillLatencyP95MsMax > 0 && fillP95Ms > g.cfg.Thresholds.FillLatencyP95MsMax {
		gi.lastActionAt = time.Now()
		return &guardAction{Kind: g.cfg.Actions.OnLatency, Reason: fmt.Sprintf("fill_latency_p95_ms=%.1f", fillP95Ms)}
	}
	if g.cfg.Thresholds.RejectRateMax > 0 && rejRate > g.cfg.Thresholds.RejectRateMax {
		gi.lastActionAt = time.Now()
		return &guardAction{Kind: g.cfg.Actions.OnReject, Reason: fmt.Sprintf("reject_rate=%.3f", rejRate)}
	}
	if g.cfg.Thresholds.SlippageP95BpsMax > 0 && slipP95 > g.cfg.Thresholds.SlippageP95BpsMax {
		gi.lastActionAt = time.Now()
		return g.actionForSlippageLocked(gi, slipP95)
	}
	return nil
}

func (g *guardrails) actionForSlippageLocked(gi *guardIntent, slipP95 float64) *guardAction {
	switch g.cfg.Actions.OnSlippage {
	case "pause":
		return &guardAction{Kind: "pause", Reason: fmt.Sprintf("slippage_p95_bps=%.2f", slipP95)}
	case "degrade":
		// Move down the ladder.
		if gi.degradeIdx < len(g.cfg.DegradeLadder)-1 {
			gi.degradeIdx++
		}
		return &guardAction{
			Kind:      "degrade",
			Reason:    fmt.Sprintf("slippage_p95_bps=%.2f", slipP95),
			DegradeTo: g.cfg.DegradeLadder[gi.degradeIdx],
		}
	default:
		return &guardAction{Kind: "noop", Reason: fmt.Sprintf("slippage_p95_bps=%.2f", slipP95)}
	}
}

func (g *guardrails) shouldRollbackOnPause() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cfg.Actions.RollbackOnPause && g.releaseControllerURL != ""
}

func (g *guardrails) rollbackRelease(releaseID string, reason string) error {
	g.mu.Lock()
	base := strings.TrimRight(g.releaseControllerURL, "/")
	orgID := g.orgID
	userID := g.userID
	g.mu.Unlock()

	if base == "" || releaseID == "" {
		return nil
	}
	body := map[string]any{"release_id": releaseID, "reason": reason}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/releases/"+releaseID+"/rollback", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if orgID != "" {
		req.Header.Set("X-Org-ID", orgID)
	}
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}
	c := &http.Client{Timeout: 400 * time.Millisecond}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

