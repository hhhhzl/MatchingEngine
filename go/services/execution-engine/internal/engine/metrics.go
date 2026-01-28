package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/marginforest/go/services/execution-engine/internal/wire"
)

// recordMetric emits a metric to:
// - observability-service (HTTP) if configured
// - gateway exec-metrics topic (optional) so it can be captured in eventlogs
func (e *Engine) recordMetric(name string, value float64, it *intentRuntime, tsNS int64, meta map[string]interface{}) {
	tags := map[string]string{
		"service":      "execution-engine",
		"run_id":       e.cfg.RunID,
		"code_version": e.cfg.CodeVersion,
	}
	if it != nil {
		if it.Intent.RunID != "" {
			tags["run_id"] = it.Intent.RunID
		}
		tags["intent_id"] = it.Intent.IntentID
		tags["strategy_id"] = it.Intent.StrategyID
		tags["symbol"] = it.Intent.InstrumentID.Symbol
		tags["venue"] = it.Intent.InstrumentID.Venue
		tags["algo"] = it.Intent.ExecutionAlgo
		if it.Intent.AccountScope.AccountID != "" {
			tags["account_id"] = it.Intent.AccountScope.AccountID
		}
	}
	if e.obs != nil {
		e.obs.RecordMetric(name, value, tags, tsNS, meta)
	}
	if e.cfg.ExecMetricsTopic != "" {
		env := map[string]any{
			"event_id":   fmt.Sprintf("exec-metric-%d", time.Now().UnixNano()),
			"event_type": "EVENT_TYPE_UNSPECIFIED",
			"ts": map[string]any{
				"ts_event_ns": 0,
				"ts_recv_ns":  tsNS,
				"ts_emit_ns":  tsNS,
			},
			"run_id":       e.cfg.RunID,
			"code_version": e.cfg.CodeVersion,
			"account_id":   tags["account_id"],
			"venue":        tags["venue"],
			"symbol":       tags["symbol"],
			"payload": map[string]any{
				"exec_metric": map[string]any{
					"name":     name,
					"value":    value,
					"tags":     tags,
					"metadata": meta,
					"ts_ns":    tsNS,
				},
			},
		}
		_ = e.gw.Publish(e.cfg.ExecMetricsTopic, wire.EventLogRecord{Type: "EXEC_METRIC", Envelope: env})
	}
}

func (e *Engine) publishAudit(who string, what string, intent wire.ExecutionIntent, context map[string]any) {
	if e.cfg.AuditTopic == "" {
		return
	}
	now := time.Now().UnixNano()
	b, _ := json.Marshal(context)
	env := map[string]any{
		"event_id":   fmt.Sprintf("audit-%d", now),
		"event_type": "EVENT_TYPE_AUDIT",
		"ts": map[string]any{
			"ts_event_ns": 0,
			"ts_recv_ns":  now,
			"ts_emit_ns":  now,
		},
		"run_id":       intent.RunID,
		"code_version": e.cfg.CodeVersion,
		"account_id":   intent.AccountScope.AccountID,
		"venue":        intent.InstrumentID.Venue,
		"symbol":       intent.InstrumentID.Symbol,
		"payload": map[string]any{
			"audit": map[string]any{
				"who":          who,
				"what":         what,
				"when":         time.Now().Format(time.RFC3339Nano),
				"where":        string(b),
				"run_id":       intent.RunID,
				"code_version": e.cfg.CodeVersion,
			},
		},
	}
	_ = e.gw.Publish(e.cfg.AuditTopic, wire.EventLogRecord{Type: "AUDIT", Envelope: env})
}

// ===== small quantile helper for guardrails/online metrics =====

type floatRing struct {
	buf []float64
	i   int
	n   int
}

func newFloatRing(cap int) *floatRing {
	return &floatRing{buf: make([]float64, cap)}
}

func (r *floatRing) add(v float64) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.i] = v
	r.i = (r.i + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
}

func (r *floatRing) snapshot() []float64 {
	out := make([]float64, 0, r.n)
	for i := 0; i < r.n; i++ {
		out = append(out, r.buf[i])
	}
	return out
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	if p <= 0 {
		return cp[0]
	}
	if p >= 1 {
		return cp[len(cp)-1]
	}
	idx := int(math.Ceil(p*float64(len(cp)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

