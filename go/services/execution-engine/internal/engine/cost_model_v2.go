package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// costParamsV2 matches the JSON lines written by python CostParamsStoreV2.
type costParamsV2 struct {
	Version          string  `json:"version"`
	Symbol           string  `json:"symbol"`
	Venue            string  `json:"venue"`
	Bucket           string  `json:"bucket"`
	FeeBps           float64 `json:"fee_bps"`
	HalfSpreadP50Bps float64 `json:"half_spread_bps_p50"`
	HalfSpreadP95Bps float64 `json:"half_spread_bps_p95"`
	ImpactK          float64 `json:"impact_k"`
	ImpactAlpha      float64 `json:"impact_alpha"`
	ResidualP50Bps   float64 `json:"residual_bps_p50"`
	ResidualP95Bps   float64 `json:"residual_bps_p95"`
}

// costModelV2Go is a minimal, deterministic replica of CostModelV2.suggest_max_slippage_bps(p95).
// It intentionally keeps the same param semantics as python:
// total_bps_p95 = fee + half_spread_p95 + impact(notional) + residual_p95
type costModelV2Go struct {
	mu      sync.RWMutex
	byKey   map[string]costParamsV2 // key: venue|symbol|bucket
	version string
}

func newCostModelV2Go(rootDir string, version string) (*costModelV2Go, error) {
	if rootDir == "" {
		return nil, nil
	}
	if version == "" {
		ver, err := pickLatestVersion(rootDir)
		if err != nil {
			return nil, err
		}
		version = ver
	}
	if version == "" {
		return nil, errors.New("no cost params version found")
	}
	path := filepath.Join(rootDir, version, "params.jsonl")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := &costModelV2Go{
		byKey:   map[string]costParamsV2{},
		version: version,
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var p costParamsV2
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			continue
		}
		k := key3(p.Venue, p.Symbol, p.Bucket)
		m.byKey[k] = p
	}
	return m, nil
}

func (m *costModelV2Go) SuggestMaxSlippageBpsP95(symbol, venue, bucket string, qty float64, midPrice float64, participation float64) (float64, bool) {
	if m == nil {
		return 0, false
	}
	if qty <= 0 || midPrice <= 0 {
		return 0, false
	}
	notional := math.Abs(qty * midPrice)
	if notional <= 0 {
		return 0, false
	}

	m.mu.RLock()
	p, ok := m.byKey[key3(venue, symbol, bucket)]
	m.mu.RUnlock()
	if !ok {
		return 0, false
	}

	fee := p.FeeBps
	halfSpread := p.HalfSpreadP95Bps
	impact := p.ImpactK * math.Pow(notional, p.ImpactAlpha)
	if participation > 0 {
		impact *= math.Min(5.0, math.Max(0.5, participation*10.0))
	}
	resid := p.ResidualP95Bps
	total := fee + halfSpread + impact + resid
	if total < 0 {
		total = 0
	}
	return total, true
}

func pickLatestVersion(rootDir string) (string, error) {
	ents, err := os.ReadDir(rootDir)
	if err != nil {
		return "", err
	}
	var vs []string
	for _, e := range ents {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			vs = append(vs, e.Name())
		}
	}
	sort.Strings(vs)
	if len(vs) == 0 {
		return "", nil
	}
	return vs[len(vs)-1], nil
}

func key3(venue, symbol, bucket string) string {
	return venue + "|" + symbol + "|" + bucket
}

