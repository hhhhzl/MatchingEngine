package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marginforest/go/services/execution-engine/internal/store"
	"github.com/marginforest/go/services/execution-engine/internal/wire"
)

// Config controls runtime behavior of the execution-engine.
type Config struct {
	RunID       string
	CodeVersion string

	IntentTopic      string
	PlanTopic        string
	OrderCmdTopic    string
	AuditTopic       string
	ExecMetricsTopic string
	ExecReportTopic  string
	MDTopic          string
	SnapshotTopic    string
	RiskTopic        string

	HTTPAddr string

	DecisionInterval time.Duration
	SliceInterval    time.Duration

	KillSwitchURL string

	// Observability service integration (optional).
	ObservabilityURL string
	OrgID            string
	UserID           string

	// Guardrails configuration (optional).
	GuardrailsPath string

	// Release-controller integration (optional).
	ReleaseControllerURL string

	// Micro decision integration (optional).
	MicroSidecarPath string
	InstrumentsPath  string

	// Cost params store v2 for max_slippage injection (optional).
	CostParamsDir     string
	CostParamsVersion string
	CostBucket        string
}

// Engine consumes ExecutionIntent events and emits ORDER_CMD events that flow through
// RiskGate -> Allocator -> RiskGate(post) -> Router -> Adapter.
//
// The engine is intentionally conservative:
// - It never duplicates client_order_id values (idempotency).
// - If it cannot prove a safe action (e.g. missing ACK/order_id), it pauses an intent
//   rather than emitting potentially duplicated orders.
type Engine struct {
	cfg   Config
	gw    *wire.GatewayClient
	store *store.Store

	mu sync.Mutex
	st runtimeState

	// Marketdata cache (venue,symbol) -> last md tick payload map
	lastMD map[mdKey]map[string]any

	// Position cache (for reduce-only).
	pos map[posKey]float64

	ks          *killSwitchClient
	limiter     *rateLimiter
	execs       map[string]Executor
	obs         *obsClient
	gr          *guardrails
	micro       *microSidecarClient
	instruments *instrumentIndex
	costModel   *costModelV2Go

	// Online metric state.
	lastMDTS map[mdKey]int64 // ts_recv_ns of last MD_TICK envelope
	// client_order_id -> ts_emit_ns when ORDER_CMD was published.
	orderSentTS map[string]int64

	// Metrics
	intentsAccepted atomic.Int64
	ordersEmitted   atomic.Int64
	plansPublished  atomic.Int64
}

type mdKey struct {
	Venue  string
	Symbol string
}

type posKey struct {
	AccountID string
	Venue     string
	Symbol    string
}

type runtimeState struct {
	Intents map[string]*intentRuntime `json:"intents"`
}

type intentRuntime struct {
	Intent wire.ExecutionIntent `json:"intent"`
	Status string              `json:"status"` // pending/running/paused/completed/terminated

	Plan wire.OrderPlan `json:"plan"`

	// Derived numeric quantities for scheduling.
	TargetQty   float64 `json:"target_qty"`
	FilledQty   float64 `json:"filled_qty"`
	ArrivalMid  float64 `json:"arrival_mid"`

	// Online market volume tracking (for VWAP/POV style control).
	LastMDVolume float64 `json:"last_md_volume"`
	CumMDVolume  float64 `json:"cum_md_volume"`
	FirstMDTSNS  int64   `json:"first_md_ts_ns"`

	// Child orders emitted for this intent (client_order_id -> state).
	Child map[string]*childOrder `json:"child_orders"`

	// Sequencing counters for deterministic client_order_id generation.
	NextSliceSeq   int `json:"next_slice_seq"`
	NextReplaceSeq int `json:"next_replace_seq"`

	// Fault tolerance counters.
	ConsecutiveRejects int `json:"consecutive_rejects"`
	TotalRejects       int `json:"total_rejects"`
}

type childOrder struct {
	ClientOrderID string `json:"client_order_id"`
	SliceID       string `json:"slice_id"`
	AccountID     string `json:"account_id"`
	Venue         string `json:"venue"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`

	Qty        float64 `json:"qty"`
	LimitPrice float64 `json:"limit_price"`

	SentTSNS     int64   `json:"sent_ts_ns"`
	OrderID      string  `json:"order_id,omitempty"`
	Status       string  `json:"status"` // new/ack/partial/filled/canceled/rejected
	FilledQty    float64 `json:"filled_qty"`
	LastEventNS  int64   `json:"last_event_ns"`

	Replaces int `json:"replaces"`
}

func New(cfg Config, gw *wire.GatewayClient, st *store.Store) *Engine {
	instr, _ := loadInstruments(cfg.InstrumentsPath)
	cm, _ := newCostModelV2Go(cfg.CostParamsDir, cfg.CostParamsVersion)
	e := &Engine{
		cfg:        cfg,
		gw:         gw,
		store:      st,
		lastMD:     map[mdKey]map[string]any{},
		lastMDTS:   map[mdKey]int64{},
		pos:        map[posKey]float64{},
		ks:         newKillSwitchClient(cfg.KillSwitchURL),
		limiter:    newRateLimiter(),
		execs:      defaultExecutors(),
		obs:        newObsClient(cfg.ObservabilityURL, cfg.OrgID, cfg.UserID),
		gr:         newGuardrails(cfg.GuardrailsPath, cfg.ReleaseControllerURL, cfg.OrgID, cfg.UserID),
		micro:      newMicroSidecarClient(cfg.MicroSidecarPath),
		instruments: instr,
		costModel:   cm,
		orderSentTS: map[string]int64{},
		st: runtimeState{
			Intents: map[string]*intentRuntime{},
		},
	}
	return e
}

func (e *Engine) execFor(it *intentRuntime) Executor {
	if it == nil {
		return twapExecutor{}
	}
	if ex, ok := e.execs[it.Intent.ExecutionAlgo]; ok {
		return ex
	}
	return twapExecutor{}
}

func (e *Engine) StartHTTP() {
	if e.cfg.HTTPAddr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		e.mu.Lock()
		defer e.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"intents":          len(e.st.Intents),
			"intents_accepted": e.intentsAccepted.Load(),
			"orders_emitted":   e.ordersEmitted.Load(),
			"plans_published":  e.plansPublished.Load(),
		})
	})
	mux.HandleFunc("/intents", func(w http.ResponseWriter, _ *http.Request) {
		e.mu.Lock()
		defer e.mu.Unlock()
		out := make([]*intentRuntime, 0, len(e.st.Intents))
		for _, it := range e.st.Intents {
			out = append(out, it)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"intents": out})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		// Minimal Prometheus-like counters (no external dependency).
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "# HELP mf_exec_engine_intents_accepted_total Total accepted execution intents\n")
		fmt.Fprintf(&buf, "# TYPE mf_exec_engine_intents_accepted_total counter\n")
		fmt.Fprintf(&buf, "mf_exec_engine_intents_accepted_total %d\n", e.intentsAccepted.Load())
		fmt.Fprintf(&buf, "# HELP mf_exec_engine_orders_emitted_total Total emitted ORDER_CMD\n")
		fmt.Fprintf(&buf, "# TYPE mf_exec_engine_orders_emitted_total counter\n")
		fmt.Fprintf(&buf, "mf_exec_engine_orders_emitted_total %d\n", e.ordersEmitted.Load())
		fmt.Fprintf(&buf, "# HELP mf_exec_engine_plans_published_total Total published ORDER_PLAN_UPDATED\n")
		fmt.Fprintf(&buf, "# TYPE mf_exec_engine_plans_published_total counter\n")
		fmt.Fprintf(&buf, "mf_exec_engine_plans_published_total %d\n", e.plansPublished.Load())
		w.Header().Set("content-type", "text/plain; version=0.0.4")
		_, _ = w.Write(buf.Bytes())
	})

	go func() {
		log.Printf("execution-engine http listening on %s", e.cfg.HTTPAddr)
		_ = http.ListenAndServe(e.cfg.HTTPAddr, mux)
	}()
}

// Recover restores state from snapshot + event log.
func (e *Engine) Recover() error {
	type snapshotState struct {
		Runtime runtimeState        `json:"runtime"`
		Pos     map[posKey]float64 `json:"pos"`
	}
	var snap snapshotState
	ok, lastSeqFromSnap, err := e.store.LoadSnapshot(&snap)
	if err != nil {
		return err
	}
	lastSeq := int64(0)
	if ok {
		e.mu.Lock()
		e.st = snap.Runtime
		e.pos = snap.Pos
		e.mu.Unlock()
		lastSeq = lastSeqFromSnap
	}
	return e.store.Replay(lastSeq, func(ev store.Event) error {
		switch ev.Typ {
		case "intent_accepted":
			var intent wire.ExecutionIntent
			if err := json.Unmarshal(ev.Data, &intent); err != nil {
				return nil
			}
			e.applyIntentAccepted(intent)
		case "plan_published":
			var plan wire.OrderPlan
			if err := json.Unmarshal(ev.Data, &plan); err != nil {
				return nil
			}
			e.applyPlanPublished(plan)
		case "child_order_emitted":
			var o childOrder
			if err := json.Unmarshal(ev.Data, &o); err != nil {
				return nil
			}
			e.applyChildOrderEmitted(ev.IntentID, o)
		case "child_order_cancel_sent":
			var o childOrder
			if err := json.Unmarshal(ev.Data, &o); err != nil {
				return nil
			}
			e.applyChildOrderEmitted(ev.IntentID, o)
		case "exec_report_seen":
			var er map[string]any
			if err := json.Unmarshal(ev.Data, &er); err != nil {
				return nil
			}
			e.applyExecReport(er)
		case "snapshot_pos":
			var p map[posKey]float64
			if err := json.Unmarshal(ev.Data, &p); err != nil {
				return nil
			}
			e.mu.Lock()
			e.pos = p
			e.mu.Unlock()
		}
		return nil
	})
}

func (e *Engine) SaveSnapshot() {
	type snapshotState struct {
		Runtime runtimeState        `json:"runtime"`
		Pos     map[posKey]float64 `json:"pos"`
	}
	e.mu.Lock()
	snap := snapshotState{Runtime: e.st, Pos: e.pos}
	e.mu.Unlock()
	_ = e.store.SaveSnapshot(0, snap)
}

func (e *Engine) Run() {
	e.StartHTTP()

	// Periodic snapshot (crash recovery).
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			e.SaveSnapshot()
		}
	}()

	// Scheduler loop.
	go func() {
		t := time.NewTicker(e.cfg.DecisionInterval)
		defer t.Stop()
		for range t.C {
			if e.gr != nil {
				_ = e.gr.reloadIfNeeded()
			}
			e.stepAll()
		}
	}()

	topics := []string{
		e.cfg.IntentTopic,
		e.cfg.MDTopic,
		e.cfg.ExecReportTopic,
		e.cfg.SnapshotTopic,
		e.cfg.RiskTopic,
	}
	e.gw.ReconnectLoop(topics, func() error {
		for {
			gm, err := e.gw.Read()
			if err != nil {
				return err
			}
			if gm == nil {
				continue
			}
			e.handleGatewayMessage(*gm)
		}
	})
}

func (e *Engine) handleGatewayMessage(gm wire.GatewayMessage) {
	var rec wire.EventLogRecord
	if err := json.Unmarshal(gm.Msg, &rec); err != nil {
		return
	}
	switch rec.Type {
	case "MD_TICK":
		if gm.Topic != e.cfg.MDTopic {
			return
		}
		e.ingestMD(rec)
	case "SNAPSHOT":
		if gm.Topic != e.cfg.SnapshotTopic {
			return
		}
		e.ingestSnapshot(rec)
	case "EXEC_REPORT":
		if gm.Topic != e.cfg.ExecReportTopic {
			return
		}
		e.ingestExecReport(rec)
	case "RISK_EVENT":
		if gm.Topic != e.cfg.RiskTopic {
			return
		}
		e.ingestRiskEvent(rec)
	case "EXEC_INTENT_PUBLISHED":
		if gm.Topic != e.cfg.IntentTopic {
			return
		}
		e.ingestIntent(rec)
	}
}

func (e *Engine) ingestIntent(rec wire.EventLogRecord) {
	env := rec.Envelope
	payload, _ := env["payload"].(map[string]any)
	obj, _ := payload["exec_intent"].(map[string]any)
	if obj == nil {
		return
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return
	}
	var intent wire.ExecutionIntent
	if err := json.Unmarshal(b, &intent); err != nil {
		return
	}
	if intent.IntentID == "" || intent.Version != "v1" {
		return
	}
	_, _ = e.store.Append("intent_accepted", intent.IntentID, intent)
	e.applyIntentAccepted(intent)
	e.intentsAccepted.Add(1)

	// Publish initial plan immediately.
	e.mu.Lock()
	it := e.st.Intents[intent.IntentID]
	e.mu.Unlock()
	if it != nil {
		e.publishPlanLocked(it, true)
	}
}

func (e *Engine) applyIntentAccepted(intent wire.ExecutionIntent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	targetQty := 0.0
	if intent.Target.TargetQty != nil {
		targetQty = fixedToFloat(intent.Target.TargetQty.Value, intent.Target.TargetQty.Scale)
	} else if intent.Target.TargetNotional != nil {
		// Best-effort: convert notional to qty using arrival price or latest mid.
		notional := fixedToFloat(intent.Target.TargetNotional.Value, intent.Target.TargetNotional.Scale)
		px := 0.0
		if intent.Benchmark.ArrivalPrice != nil {
			px = fixedToFloat(intent.Benchmark.ArrivalPrice.Value, intent.Benchmark.ArrivalPrice.Scale)
		}
		if px <= 0 {
			md := e.lastMD[mdKey{Venue: intent.InstrumentID.Venue, Symbol: intent.InstrumentID.Symbol}]
			if md != nil {
				bid := parseDecimal(md["bid"])
				ask := parseDecimal(md["ask"])
				last := parseDecimal(md["last"])
				if bid > 0 && ask > 0 {
					px = (bid + ask) / 2
				} else if last > 0 {
					px = last
				}
			}
		}
		if notional > 0 && px > 0 {
			targetQty = notional / px
		}
	}

	// Arrival mid is captured at intent acceptance for real-time IS/slippage attribution.
	arrivalMid := 0.0
	if md := e.lastMD[mdKey{Venue: intent.InstrumentID.Venue, Symbol: intent.InstrumentID.Symbol}]; md != nil {
		bid := parseDecimal(md["bid"])
		ask := parseDecimal(md["ask"])
		last := parseDecimal(md["last"])
		if bid > 0 && ask > 0 {
			arrivalMid = (bid + ask) / 2
		} else if last > 0 {
			arrivalMid = last
		}
	}

	if it, ok := e.st.Intents[intent.IntentID]; ok {
		// Idempotent re-delivery / update: update intent fields, keep execution state.
		it.Intent = intent
		if targetQty > 0 {
			it.TargetQty = targetQty
		}
		if arrivalMid > 0 && it.ArrivalMid == 0 {
			it.ArrivalMid = arrivalMid
		}
		if it.Status == "" {
			it.Status = "running"
		}
		return
	}

	e.st.Intents[intent.IntentID] = &intentRuntime{
		Intent:     intent,
		Status:     "running",
		TargetQty:  targetQty,
		ArrivalMid: arrivalMid,
		Child:      map[string]*childOrder{},
	}
}

func (e *Engine) publishPlanLocked(it *intentRuntime, force bool) {
	// Build or refresh plan from current intent settings.
	plan := e.execFor(it).BuildPlan(it.Intent, time.Now().UnixNano(), e.cfg.SliceInterval)
	it.Plan = plan

	_, _ = e.store.Append("plan_published", it.Intent.IntentID, plan)
	e.applyPlanPublished(plan)
	e.plansPublished.Add(1)

	// Publish to gateway (best-effort).
	env := map[string]any{
		"event_id":   fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		"event_type": "EVENT_TYPE_UNSPECIFIED",
		"ts": map[string]any{
			"ts_event_ns": 0,
			"ts_recv_ns":  time.Now().UnixNano(),
			"ts_emit_ns":  time.Now().UnixNano(),
		},
		"run_id":       it.Intent.RunID,
		"code_version": e.cfg.CodeVersion,
		"account_id":   it.Intent.AccountScope.AccountID,
		"venue":        it.Intent.InstrumentID.Venue,
		"symbol":       it.Intent.InstrumentID.Symbol,
		"payload": map[string]any{
			"order_plan": plan,
		},
	}
	_ = e.gw.Publish(e.cfg.PlanTopic, wire.EventLogRecord{Type: "ORDER_PLAN_UPDATED", Envelope: env})
	_ = force // reserved for future: only publish on change unless forced
}

func (e *Engine) applyPlanPublished(plan wire.OrderPlan) {
	e.mu.Lock()
	defer e.mu.Unlock()
	it := e.st.Intents[plan.IntentID]
	if it == nil {
		return
	}
	it.Plan = plan
}

func (e *Engine) ingestMD(rec wire.EventLogRecord) {
	env := rec.Envelope
	payload, _ := env["payload"].(map[string]any)
	md, _ := payload["md_tick"].(map[string]any)
	if md == nil {
		return
	}
	symbol, _ := md["symbol"].(string)
	venue, _ := md["venue"].(string)
	if symbol == "" || venue == "" {
		return
	}
	vol := parseDecimal(md["volume"])
	now := time.Now().UnixNano()
	tsRecv := now
	if ts, ok := env["ts"].(map[string]any); ok {
		if v, ok := ts["ts_recv_ns"].(float64); ok {
			tsRecv = int64(v)
		}
	}
	e.mu.Lock()
	e.lastMD[mdKey{Venue: venue, Symbol: symbol}] = md
	e.lastMDTS[mdKey{Venue: venue, Symbol: symbol}] = tsRecv
	// Update per-intent volume tracking for online VWAP/POV control.
	for _, it := range e.st.Intents {
		if it.Intent.InstrumentID.Symbol != symbol || it.Intent.InstrumentID.Venue != venue {
			continue
		}
		if it.FirstMDTSNS == 0 {
			it.FirstMDTSNS = now
		}
		if vol > 0 {
			// Handle both cumulative and per-tick volume sources:
			// - If monotonic increasing, treat as cumulative and take deltas.
			// - If not monotonic, treat as per-tick and accumulate directly.
			delta := vol
			if vol >= it.LastMDVolume && it.LastMDVolume > 0 {
				delta = vol - it.LastMDVolume
			}
			if delta > 0 {
				it.CumMDVolume += delta
			}
			it.LastMDVolume = vol
		}
	}
	e.mu.Unlock()
}

func (e *Engine) ingestSnapshot(rec wire.EventLogRecord) {
	// Keep a position cache for reduce-only enforcement.
	env := rec.Envelope
	accountID, _ := env["account_id"].(string)
	if accountID == "" {
		return
	}
	payload, _ := env["payload"].(map[string]any)
	snap, _ := payload["snapshot"].(map[string]any)
	if snap == nil {
		return
	}
	positions, _ := snap["positions"].([]any)
	if len(positions) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, it := range positions {
		p, ok := it.(map[string]any)
		if !ok {
			continue
		}
		venue, _ := p["venue"].(string)
		symbol, _ := p["symbol"].(string)
		qty := parseDecimal(p["qty"])
		if venue == "" || symbol == "" {
			continue
		}
		e.pos[posKey{AccountID: accountID, Venue: venue, Symbol: symbol}] = qty
	}
	// Persist position snapshot in the event log (recovery without needing a full gateway replay).
	_, _ = e.store.Append("snapshot_pos", "", e.pos)
}

func (e *Engine) ingestExecReport(rec wire.EventLogRecord) {
	env := rec.Envelope
	payload, _ := env["payload"].(map[string]any)
	er, _ := payload["exec_report"].(map[string]any)
	if er == nil {
		return
	}
	_, _ = e.store.Append("exec_report_seen", "", er)
	e.applyExecReport(er)
}

func (e *Engine) applyExecReport(er map[string]any) {
	coid, _ := er["client_order_id"].(string)
	if coid == "" {
		return
	}
	reportType, _ := er["report_type"].(string)
	orderID, _ := er["order_id"].(string)
	filledQty := parseDecimal(er["filled_qty"])

	e.mu.Lock()
	defer e.mu.Unlock()

	// Find owning intent by scanning active child orders.
	for _, it := range e.st.Intents {
		ch := it.Child[coid]
		if ch == nil {
			continue
		}
		if orderID != "" {
			ch.OrderID = orderID
		}
		ch.LastEventNS = time.Now().UnixNano()
		switch reportType {
		case "EXEC_REPORT_TYPE_ACK":
			ch.Status = "ack"
			if ch.SentTSNS > 0 {
				lat := float64(ch.LastEventNS - ch.SentTSNS)
				e.recordMetric("mf.exec.order_ack_latency_ns", lat, it, ch.LastEventNS, nil)
				if e.gr != nil {
					e.gr.observeAckLatency(it.Intent.IntentID, lat)
				}
				if e.obs != nil {
					e.obs.RecordSpan(
						"intent:"+it.Intent.IntentID,
						fmt.Sprintf("span-ack-%d", ch.LastEventNS),
						"",
						"exec.order_ack",
						ch.SentTSNS,
						ch.LastEventNS,
						"OK",
						map[string]interface{}{"client_order_id": ch.ClientOrderID},
					)
				}
			}
		case "EXEC_REPORT_TYPE_PARTIAL":
			ch.Status = "partial"
			if filledQty > ch.FilledQty {
				delta := filledQty - ch.FilledQty
				ch.FilledQty = filledQty
				it.FilledQty += delta
			}
		case "EXEC_REPORT_TYPE_FILL":
			ch.Status = "filled"
			if filledQty > ch.FilledQty {
				delta := filledQty - ch.FilledQty
				ch.FilledQty = filledQty
				it.FilledQty += delta
			}
			// Real-time slippage / IS estimate vs arrival mid (best-effort).
			if it.ArrivalMid > 0 {
				fp := parseDecimal(er["fill_price"])
				if fp > 0 {
					slippageBps := math.Abs(fp-it.ArrivalMid) / it.ArrivalMid * 10000.0
					e.recordMetric("mf.exec.slippage_bps", slippageBps, it, ch.LastEventNS, map[string]interface{}{"fill_price": fp, "arrival_mid": it.ArrivalMid})
					if e.gr != nil {
						e.gr.observeSlippage(it.Intent.IntentID, slippageBps)
					}
					// Signed IS: buy wants negative, sell wants positive; store signed for analysis.
					isSigned := (fp - it.ArrivalMid) / it.ArrivalMid * 10000.0
					if it.Intent.Side == "sell" {
						// For sells, higher-than-arrival is good (positive IS).
						// Keep the signed value as-is; downstream can interpret.
					}
					e.recordMetric("mf.exec.implementation_shortfall_bps", isSigned, it, ch.LastEventNS, nil)
				}
			}
			if ch.SentTSNS > 0 {
				lat := float64(ch.LastEventNS - ch.SentTSNS)
				e.recordMetric("mf.exec.order_fill_latency_ns", lat, it, ch.LastEventNS, nil)
				if e.gr != nil {
					e.gr.observeFillLatency(it.Intent.IntentID, lat)
				}
				if e.obs != nil {
					e.obs.RecordSpan(
						"intent:"+it.Intent.IntentID,
						fmt.Sprintf("span-fill-%d", ch.LastEventNS),
						"",
						"exec.order_fill",
						ch.SentTSNS,
						ch.LastEventNS,
						"OK",
						map[string]interface{}{"client_order_id": ch.ClientOrderID},
					)
				}
			}
		case "EXEC_REPORT_TYPE_REJECT":
			ch.Status = "rejected"
			it.TotalRejects++
			it.ConsecutiveRejects++
			e.recordMetric("mf.exec.order_reject", 1, it, ch.LastEventNS, map[string]interface{}{"reason": er["reason"]})
			if e.gr != nil {
				e.gr.observeReject(it.Intent.IntentID)
			}
		case "EXEC_REPORT_TYPE_CANCEL":
			ch.Status = "canceled"
			e.recordMetric("mf.exec.order_cancel", 1, it, ch.LastEventNS, nil)
		default:
			// ignore
		}
		// Success resets consecutive rejects.
		if reportType == "EXEC_REPORT_TYPE_ACK" || reportType == "EXEC_REPORT_TYPE_PARTIAL" || reportType == "EXEC_REPORT_TYPE_FILL" {
			it.ConsecutiveRejects = 0
		}
		// Completion check.
		if it.TargetQty > 0 && it.FilledQty >= it.TargetQty-1e-9 {
			it.Status = "completed"
		}
		return
	}
}

func (e *Engine) ingestRiskEvent(rec wire.EventLogRecord) {
	// For now we only persist risk events indirectly via EXEC_REPORT reject reasons.
	// Keeping this hook allows future guardrails (auto-pause) without refactoring.
	_ = rec
}

func (e *Engine) stepAll() {
	now := time.Now().UnixNano()
	// Prefetch kill-switch modes without holding the engine lock.
	accountIDs := map[string]struct{}{}
	e.mu.Lock()
	for _, it := range e.st.Intents {
		if it.Status != "running" {
			continue
		}
		if it.Intent.AccountScope.AccountID != "" {
			accountIDs[it.Intent.AccountScope.AccountID] = struct{}{}
		}
	}
	e.mu.Unlock()

	modes := map[string]string{}
	for acct := range accountIDs {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		modes[acct] = e.ks.getMode(ctx, acct)
		cancel()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, it := range e.st.Intents {
		if it.Status != "running" {
			continue
		}
		// Time-window enforcement.
		if now < it.Intent.TimeWindow.StartTSNS {
			continue
		}
		if now > it.Intent.TimeWindow.EndTSNS {
			// Stop safely on expiry: cancel any live child orders before terminating.
			e.cancelAllLiveLocked(it)
			it.Status = "terminated"
			continue
		}

		// Fault tolerance gate.
		if it.Plan.FaultTolerance.MaxConsecutiveRejects > 0 && it.ConsecutiveRejects >= it.Plan.FaultTolerance.MaxConsecutiveRejects {
			e.cancelAllLiveLocked(it)
			it.Status = "paused"
			continue
		}
		if it.Plan.FaultTolerance.MaxTotalRejects > 0 && it.TotalRejects >= it.Plan.FaultTolerance.MaxTotalRejects {
			e.cancelAllLiveLocked(it)
			it.Status = "paused"
			continue
		}

		// Kill-switch mode linkage (control-plane safety).
		acct := it.Intent.AccountScope.AccountID
		mode := "ON"
		if acct != "" {
			if m, ok := modes[acct]; ok && m != "" {
				mode = m
			}
		}
		switch mode {
		case "OFF":
			e.cancelAllLiveLocked(it)
			it.Status = "paused"
			continue
		case "FLATTEN", "REDUCE_ONLY":
			// Execution-engine enforces reduce-only behavior using snapshot-derived positions.
			curPos := e.pos[posKey{AccountID: acct, Venue: it.Intent.InstrumentID.Venue, Symbol: it.Intent.InstrumentID.Symbol}]
			if !isReducingOrder(it.Intent.Side, curPos) {
				e.cancelAllLiveLocked(it)
				it.Status = "paused"
				continue
			}
		}

		// Ensure plan exists.
		if it.Plan.PlanID == "" {
			e.publishPlanLocked(it, true)
		}

		if it.TargetQty > 0 && it.FilledQty >= it.TargetQty-1e-9 {
			it.Status = "completed"
			continue
		}

		// Pick active slice based on plan schedule (time bucket).
		slice := nextSlice(it.Plan, now)
		if slice == nil {
			// No slice is active at this moment; wait.
			continue
		}

		// Marketdata staleness metric (best-effort).
		key := mdKey{Venue: it.Intent.InstrumentID.Venue, Symbol: it.Intent.InstrumentID.Symbol}
		if ts, ok := e.lastMDTS[key]; ok && ts > 0 {
			stale := float64(now - ts)
			e.recordMetric("mf.exec.md_staleness_ns", stale, it, now, nil)
			if e.gr != nil {
				e.gr.observeMDStaleness(it.Intent.IntentID, stale)
			}
		}

		// Compute desired cumulative schedule based on algo + observed market volume.
		desired := e.execFor(it).DesiredCumQty(it, now)
		if it.TargetQty > 0 {
			desired = math.Min(desired, it.TargetQty)
		}
		working := liveWorkingQty(it)
		catchUp := desired - it.FilledQty - working
		// If we have a live child order, we still run micro-decisions for cancel/replace.

		// Cap by current slice target (so we don't burst too much in a single bucket).
		sliceCap := fixedToFloat(slice.Qty.Value, slice.Qty.Scale)
		if sliceCap > 0 {
			catchUp = math.Min(catchUp, sliceCap)
		}

		qty := catchUp
		if it.Intent.Constraints.MaxOrderQty != nil {
			maxQ := fixedToFloat(it.Intent.Constraints.MaxOrderQty.Value, it.Intent.Constraints.MaxOrderQty.Scale)
			if maxQ > 0 {
				qty = math.Min(qty, maxQ)
			}
		}
		if it.Intent.Constraints.MinFillQty != nil {
			minQ := fixedToFloat(it.Intent.Constraints.MinFillQty.Value, it.Intent.Constraints.MinFillQty.Scale)
			if minQ > 0 && qty < minQ {
				// Do not emit undersized child orders.
				continue
			}
		}
		// When qty<=0, we still allow micro cancel/replace decisions on existing orders.
		e.stepMicroLocked(it, slice.SliceID, qty, now)
	}
}

func (e *Engine) stepMicroLocked(it *intentRuntime, sliceID string, desiredQty float64, nowNS int64) {
	// If micro sidecar is disabled, fall back to legacy path (computeLimitPrice + maybeCancelReplace).
	if e.micro == nil {
		if hasLiveChild(it) {
			e.maybeCancelReplaceLocked(it, nowNS)
			return
		}
		if desiredQty <= 0 {
			return
		}
		px, ok := e.computeLimitPrice(it.Intent, it.Plan, nowNS)
		if !ok {
			return
		}
		e.emitNewChildLocked(it, sliceID, desiredQty, px, nowNS)
		return
	}

	// Market data is required for micro decisions (it uses bid/ask/last).
	md := e.lastMD[mdKey{Venue: it.Intent.InstrumentID.Venue, Symbol: it.Intent.InstrumentID.Symbol}]
	if md == nil {
		return
	}
	bid := parseDecimal(md["bid"])
	ask := parseDecimal(md["ask"])
	last := parseDecimal(md["last"])
	mid := 0.0
	if bid > 0 && ask > 0 {
		mid = (bid + ask) / 2
	} else if last > 0 {
		mid = last
	}
	if mid <= 0 {
		return
	}

	// Determine current working order (single-live constraint).
	var cur *childOrder
	for _, ch := range it.Child {
		if ch.Status == "new" || ch.Status == "ack" || ch.Status == "partial" {
			cur = ch
			break
		}
	}
	if cur != nil {
		// If there is a working order, micro operates on its remaining qty.
		desiredQty = math.Max(0, cur.Qty-cur.FilledQty)
	}
	if desiredQty <= 0 && cur == nil {
		return
	}

	// Instrument precision config.
	inst := e.instruments.get(it.Intent.InstrumentID.Venue, it.Intent.InstrumentID.Symbol)

	// Dynamic max_slippage_bps injection:
	// max_slippage_bps = min(intent.constraints.max_slippage_bps, cost_model_v2.p95_estimate)
	maxSlip := 0.0
	if e.costModel != nil {
		p := it.Intent.Constraints.MaxParticipation
		if p <= 0 {
			p = 0.0
		}
		if v, ok := e.costModel.SuggestMaxSlippageBpsP95(it.Intent.InstrumentID.Symbol, it.Intent.InstrumentID.Venue, e.cfg.CostBucket, desiredQty, mid, p); ok {
			maxSlip = v
		}
	}
	if it.Intent.Constraints.MaxSlippageBps > 0 {
		if maxSlip <= 0 {
			maxSlip = it.Intent.Constraints.MaxSlippageBps
		} else {
			maxSlip = math.Min(maxSlip, it.Intent.Constraints.MaxSlippageBps)
		}
	}

	priceLimit := 0.0
	if it.Intent.Constraints.PriceLimit != nil {
		priceLimit = fixedToFloat(it.Intent.Constraints.PriceLimit.Value, it.Intent.Constraints.PriceLimit.Scale)
	}

	// Build MicroInput JSON (strings for decimals).
	input := map[string]any{
		"symbol":      it.Intent.InstrumentID.Symbol,
		"venue":       it.Intent.InstrumentID.Venue,
		"side":        it.Intent.Side,
		"desired_qty": f64s(desiredQty),
		"tick_size":   f64s(inst.TickSize),
		"lot_size":    f64s(inst.LotSize),
		"min_notional": func() any {
			if inst.MinNotional > 0 {
				return f64s(inst.MinNotional)
			}
			return nil
		}(),
		"bid":  func() any { if bid > 0 { return f64s(bid) }; return nil }(),
		"ask":  func() any { if ask > 0 { return f64s(ask) }; return nil }(),
		"last": func() any { if last > 0 { return f64s(last) }; return nil }(),
		"price_policy": map[string]any{
			"price_limit":           func() any { if priceLimit > 0 { return f64s(priceLimit) }; return nil }(),
			"limit_offset_bps":      f64s(it.Plan.PriceStrategy.LimitOffsetBps),
			"price_protection_bps":  f64s(it.Plan.PriceStrategy.PriceProtectionBps),
			"max_slippage_bps":      func() any { if maxSlip > 0 { return f64s(maxSlip) }; return nil }(),
			"peg_mode":              it.Plan.PriceStrategy.PegMode,
		},
		"cr_policy": map[string]any{
			"min_lifetime_ms":       it.Plan.CancelReplace.MinLifetimeMS,
			"replace_threshold_bps": f64s(it.Plan.CancelReplace.ReplaceThresholdBps),
			"max_replaces":          it.Plan.CancelReplace.MaxReplacesPerSlice,
		},
		"current": func() any {
			if cur == nil {
				return nil
			}
			return map[string]any{
				"client_order_id":      cur.ClientOrderID,
				"working_qty":          f64s(math.Max(0, cur.Qty-cur.FilledQty)),
				"working_price":        f64s(cur.LimitPrice),
				"last_update_ts_ns":    cur.LastEventNS,
				"replaces":             cur.Replaces,
			}
		}(),
		"now_ts_ns": nowNS,
	}

	dec, err := e.micro.decide(input)
	if err != nil {
		return
	}
	act, _ := dec["action"].(string)
	switch act {
	case "noop":
		return
	case "place":
		q, ok1 := dec["qty"].(string)
		p, ok2 := dec["price"].(string)
		if !ok1 || !ok2 {
			return
		}
		qf, _ := strconv.ParseFloat(q, 64)
		pf, _ := strconv.ParseFloat(p, 64)
		if qf <= 0 || pf <= 0 {
			return
		}
		e.emitNewChildLocked(it, sliceID, qf, pf, nowNS)
	case "cancel":
		if cur == nil {
			return
		}
		e.cancelChildLocked(it, *cur, nowNS, "micro_cancel")
	case "replace":
		if cur == nil {
			return
		}
		q, ok1 := dec["new_qty"].(string)
		p, ok2 := dec["new_price"].(string)
		if !ok1 || !ok2 {
			return
		}
		qf, _ := strconv.ParseFloat(q, 64)
		pf, _ := strconv.ParseFloat(p, 64)
		if qf <= 0 || pf <= 0 {
			return
		}
		// Cancel then place a new child order.
		e.cancelChildLocked(it, *cur, nowNS, "micro_replace")
		e.emitReplaceChildLocked(it, sliceID, qf, pf, nowNS)
	}
}

func (e *Engine) emitNewChildLocked(it *intentRuntime, sliceID string, qty float64, px float64, now int64) {
	rk := "ord:" + it.Intent.AccountScope.AccountID
	if !e.limiter.Allow(rk, it.Plan.RateLimit.OrdersPerSec, it.Plan.RateLimit.Burst) {
		return
	}
	coid := e.nextClientOrderIDLocked(it, sliceID)
	cmd := buildOrderCmdRecord(it.Intent, e.cfg.RunIDOrDefault(it.Intent.RunID), e.cfg.CodeVersion, coid, qty, px, it.Plan.PriceStrategy)
	if err := e.gw.Publish(e.cfg.OrderCmdTopic, cmd); err != nil {
		return
	}
	e.ordersEmitted.Add(1)
	e.orderSentTS[coid] = now
	if e.gr != nil {
		e.gr.observeOrder(it.Intent.IntentID)
	}
	e.publishAudit("execution-engine", "child_order_emitted", it.Intent, map[string]any{
		"intent_id":       it.Intent.IntentID,
		"client_order_id": coid,
		"slice_id":        sliceID,
		"algo":            it.Intent.ExecutionAlgo,
		"qty":             qty,
		"limit_price":     px,
	})
	e.recordMetric("mf.exec.order_emitted", 1, it, now, nil)
	ch := childOrder{
		ClientOrderID: coid,
		SliceID:       sliceID,
		AccountID:     it.Intent.AccountScope.AccountID,
		Venue:         it.Intent.InstrumentID.Venue,
		Symbol:        it.Intent.InstrumentID.Symbol,
		Side:          it.Intent.Side,
		Qty:           qty,
		LimitPrice:    px,
		SentTSNS:      now,
		Status:        "new",
		LastEventNS:   now,
	}
	_, _ = e.store.Append("child_order_emitted", it.Intent.IntentID, ch)
	cp := ch
	it.Child[ch.ClientOrderID] = &cp
	e.applyGuardrailsLocked(it)
}

func (e *Engine) emitReplaceChildLocked(it *intentRuntime, sliceID string, qty float64, px float64, now int64) {
	rk := "ord:" + it.Intent.AccountScope.AccountID
	if !e.limiter.Allow(rk, it.Plan.RateLimit.OrdersPerSec, it.Plan.RateLimit.Burst) {
		return
	}
	coid := e.nextReplaceClientOrderIDLocked(it, sliceID)
	cmd := buildOrderCmdRecord(it.Intent, e.cfg.RunIDOrDefault(it.Intent.RunID), e.cfg.CodeVersion, coid, qty, px, it.Plan.PriceStrategy)
	if err := e.gw.Publish(e.cfg.OrderCmdTopic, cmd); err != nil {
		return
	}
	e.ordersEmitted.Add(1)
	e.orderSentTS[coid] = now
	if e.gr != nil {
		e.gr.observeOrder(it.Intent.IntentID)
	}
	e.publishAudit("execution-engine", "child_order_replaced", it.Intent, map[string]any{
		"intent_id":       it.Intent.IntentID,
		"client_order_id": coid,
		"slice_id":        sliceID,
		"algo":            it.Intent.ExecutionAlgo,
		"qty":             qty,
		"limit_price":     px,
	})
	e.recordMetric("mf.exec.order_replaced", 1, it, now, nil)
	ch := childOrder{
		ClientOrderID: coid,
		SliceID:       sliceID,
		AccountID:     it.Intent.AccountScope.AccountID,
		Venue:         it.Intent.InstrumentID.Venue,
		Symbol:        it.Intent.InstrumentID.Symbol,
		Side:          it.Intent.Side,
		Qty:           qty,
		LimitPrice:    px,
		SentTSNS:      now,
		Status:        "new",
		LastEventNS:   now,
		Replaces:      1,
	}
	_, _ = e.store.Append("child_order_emitted", it.Intent.IntentID, ch)
	cp := ch
	it.Child[ch.ClientOrderID] = &cp
	e.applyGuardrailsLocked(it)
}

func (e *Engine) cancelChildLocked(it *intentRuntime, ch childOrder, now int64, reason string) {
	ck := "cxl:" + it.Intent.AccountScope.AccountID
	if !e.limiter.Allow(ck, it.Plan.RateLimit.CancelsPerSec, it.Plan.RateLimit.Burst) {
		return
	}
	cancelCmd := buildCancelOrderCmdRecord(it.Intent, e.cfg.RunIDOrDefault(it.Intent.RunID), e.cfg.CodeVersion, ch)
	if err := e.gw.Publish(e.cfg.OrderCmdTopic, cancelCmd); err != nil {
		return
	}
	e.ordersEmitted.Add(1)
	e.publishAudit("execution-engine", "child_order_cancel_sent", it.Intent, map[string]any{
		"client_order_id": ch.ClientOrderID,
		"reason":          reason,
	})
	e.recordMetric("mf.exec.order_cancel_sent", 1, it, now, map[string]interface{}{"reason": reason})
}

func (e *Engine) applyGuardrailsLocked(it *intentRuntime) {
	if e.gr == nil || it == nil {
		return
	}
	act := e.gr.evaluate(it.Intent.IntentID)
	if act == nil {
		return
	}

	switch act.Kind {
	case "degrade":
		old := it.Intent.ExecutionAlgo
		it.Intent.ExecutionAlgo = act.DegradeTo
		e.publishPlanLocked(it, true)
		e.publishAudit("execution-engine", "AUTO_DEGRADE", it.Intent, map[string]any{
			"from_algo": old,
			"to_algo":   act.DegradeTo,
			"reason":    act.Reason,
		})
	case "pause", "pause_cancel_all":
		e.cancelAllLiveLocked(it)
		it.Status = "paused"
		e.publishAudit("execution-engine", "AUTO_PAUSE", it.Intent, map[string]any{
			"reason": act.Reason,
		})
		if e.gr.shouldRollbackOnPause() {
			if rid, ok := it.Intent.Tags["release_id"]; ok && rid != "" {
				_ = e.gr.rollbackRelease(rid, "execution guardrails: "+act.Reason)
				e.publishAudit("execution-engine", "AUTO_ROLLBACK", it.Intent, map[string]any{
					"release_id": rid,
					"reason":     act.Reason,
				})
			}
		}
	}
}

func (e *Engine) maybeCancelReplaceLocked(it *intentRuntime, nowNS int64) {
	if it.Plan.CancelReplace.Mode != "cancel_replace" {
		return
	}
	// Find a single live child (engine emits at most one at a time).
	var live *childOrder
	for _, ch := range it.Child {
		if ch.Status == "new" || ch.Status == "ack" || ch.Status == "partial" {
			live = ch
			break
		}
	}
	if live == nil {
		return
	}
	ageMS := float64(nowNS-live.LastEventNS) / 1e6
	if ageMS < float64(it.Plan.CancelReplace.MinLifetimeMS) {
		return
	}
	if live.LimitPrice <= 0 {
		return
	}
	newPx, ok := e.computeLimitPrice(it.Intent, it.Plan, nowNS)
	if !ok || newPx <= 0 {
		return
	}
	bps := math.Abs(newPx-live.LimitPrice) / live.LimitPrice * 10000.0
	if bps < it.Plan.CancelReplace.ReplaceThresholdBps {
		return
	}
	if live.Replaces >= it.Plan.CancelReplace.MaxReplacesPerSlice {
		return
	}

	// Rate limit (cancels).
	ck := "cxl:" + it.Intent.AccountScope.AccountID
	if !e.limiter.Allow(ck, it.Plan.RateLimit.CancelsPerSec, it.Plan.RateLimit.Burst) {
		return
	}

	cancelCmd := buildCancelOrderCmdRecord(it.Intent, e.cfg.RunIDOrDefault(it.Intent.RunID), e.cfg.CodeVersion, *live)
	if err := e.gw.Publish(e.cfg.OrderCmdTopic, cancelCmd); err != nil {
		return
	}
	e.ordersEmitted.Add(1)
	live.Status = "cancel_sent"
	live.Replaces++
	_, _ = e.store.Append("child_order_cancel_sent", it.Intent.IntentID, live)
}

func (e *Engine) cancelAllLiveLocked(it *intentRuntime) {
	// Best-effort cancel: do not create new risk while transitioning to a safe state.
	for _, ch := range it.Child {
		if ch.Status != "new" && ch.Status != "ack" && ch.Status != "partial" {
			continue
		}
		if ch.Status == "cancel_sent" {
			continue
		}
		ck := "cxl:" + it.Intent.AccountScope.AccountID
		if !e.limiter.Allow(ck, it.Plan.RateLimit.CancelsPerSec, it.Plan.RateLimit.Burst) {
			continue
		}
		cancelCmd := buildCancelOrderCmdRecord(it.Intent, e.cfg.RunIDOrDefault(it.Intent.RunID), e.cfg.CodeVersion, *ch)
		if err := e.gw.Publish(e.cfg.OrderCmdTopic, cancelCmd); err != nil {
			continue
		}
		e.ordersEmitted.Add(1)
		ch.Status = "cancel_sent"
		_, _ = e.store.Append("child_order_cancel_sent", it.Intent.IntentID, ch)
	}
}

func liveWorkingQty(it *intentRuntime) float64 {
	sum := 0.0
	for _, ch := range it.Child {
		if ch.Status == "new" || ch.Status == "ack" || ch.Status == "partial" || ch.Status == "cancel_sent" {
			sum += math.Max(0, ch.Qty-ch.FilledQty)
		}
	}
	return sum
}

func (e *Engine) applyChildOrderEmitted(intentID string, ch childOrder) {
	e.mu.Lock()
	defer e.mu.Unlock()
	it := e.st.Intents[intentID]
	if it == nil {
		return
	}
	cp := ch
	it.Child[ch.ClientOrderID] = &cp
}

func (e *Engine) nextClientOrderIDLocked(it *intentRuntime, sliceID string) string {
	// Deterministic, traceable IDs:
	//   exec:{intent_id}:{slice_seq}:{replace_seq}
	it.NextSliceSeq++
	it.NextReplaceSeq = 0
	return fmt.Sprintf("exec:%s:%s:%d:r%d", it.Intent.IntentID, sliceID, it.NextSliceSeq, it.NextReplaceSeq)
}

func (e *Engine) nextReplaceClientOrderIDLocked(it *intentRuntime, sliceID string) string {
	// Replace IDs keep the same slice_seq and bump replace_seq.
	if it.NextSliceSeq <= 0 {
		it.NextSliceSeq = 1
	}
	it.NextReplaceSeq++
	return fmt.Sprintf("exec:%s:%s:%d:r%d", it.Intent.IntentID, sliceID, it.NextSliceSeq, it.NextReplaceSeq)
}

func (e *Engine) computeLimitPrice(intent wire.ExecutionIntent, plan wire.OrderPlan, nowNS int64) (float64, bool) {
	md := e.lastMD[mdKey{Venue: intent.InstrumentID.Venue, Symbol: intent.InstrumentID.Symbol}]
	if md == nil {
		return 0, false
	}
	bid := parseDecimal(md["bid"])
	ask := parseDecimal(md["ask"])
	last := parseDecimal(md["last"])
	mid := 0.0
	if bid > 0 && ask > 0 {
		mid = (bid + ask) / 2
	} else if last > 0 {
		mid = last
	}
	if mid <= 0 {
		return 0, false
	}

	base := mid
	switch plan.PriceStrategy.PegMode {
	case "bid":
		if bid > 0 {
			base = bid
		}
	case "ask":
		if ask > 0 {
			base = ask
		}
	case "mid":
		base = mid
	}

	// Urgency shifts base towards touch.
	if intent.Urgency == "urgent" || intent.Urgency == "high" {
		if intent.Side == "buy" && ask > 0 {
			base = ask
		}
		if intent.Side == "sell" && bid > 0 {
			base = bid
		}
	}

	off := plan.PriceStrategy.LimitOffsetBps / 10000.0
	px := base
	if intent.Side == "buy" {
		px = base * (1.0 - off)
	} else {
		px = base * (1.0 + off)
	}

	// Hard clamp by intent price_limit if present.
	if intent.Constraints.PriceLimit != nil {
		lim := fixedToFloat(intent.Constraints.PriceLimit.Value, intent.Constraints.PriceLimit.Scale)
		if lim > 0 {
			if intent.Side == "buy" {
				px = math.Min(px, lim)
			} else {
				px = math.Max(px, lim)
			}
		}
	}

	// Slippage protection: if px is too far from mid, clamp.
	if plan.PriceStrategy.PriceProtectionBps > 0 {
		maxMove := plan.PriceStrategy.PriceProtectionBps / 10000.0
		if intent.Side == "buy" {
			px = math.Min(px, mid*(1.0+maxMove))
		} else {
			px = math.Max(px, mid*(1.0-maxMove))
		}
	}

	_ = nowNS
	return px, true
}

func fixedToFloat(v int64, scale int32) float64 {
	if scale == 0 {
		return float64(v)
	}
	return float64(v) / math.Pow10(int(scale))
}

func isReducingOrder(side string, curPos float64) bool {
	// reduce-only requires the order to decrease absolute position.
	if curPos == 0 {
		return false
	}
	if curPos > 0 {
		return side == "sell"
	}
	return side == "buy"
}

func hasLiveChild(it *intentRuntime) bool {
	for _, ch := range it.Child {
		if ch.Status == "new" || ch.Status == "ack" || ch.Status == "partial" || ch.Status == "cancel_sent" {
			return true
		}
	}
	return false
}

func nextSlice(plan wire.OrderPlan, nowNS int64) *wire.PlanSlice {
	for i := range plan.Slices {
		s := &plan.Slices[i]
		if nowNS >= s.TimeWindow.StartTSNS && nowNS <= s.TimeWindow.EndTSNS {
			return s
		}
	}
	return nil
}

func buildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan {
	// Full v1 plan builder:
	// - Deterministic slices computed from the time window and configured slice interval.
	// - Applies a few safe defaults for cancel/replace + fault tolerance.
	start := intent.TimeWindow.StartTSNS
	end := intent.TimeWindow.EndTSNS
	if end <= start {
		end = start + int64(10*time.Second)
	}
	if sliceInterval <= 0 {
		sliceInterval = 1 * time.Second
	}
	n := int(math.Ceil(float64(end-start) / float64(sliceInterval.Nanoseconds())))
	if n < 1 {
		n = 1
	}
	if intent.Constraints.MaxChildOrders != nil && *intent.Constraints.MaxChildOrders > 0 {
		if n > *intent.Constraints.MaxChildOrders {
			n = *intent.Constraints.MaxChildOrders
		}
	}

	totalQty := int64(0)
	qScale := int32(0)
	if intent.Target.TargetQty != nil {
		totalQty = intent.Target.TargetQty.Value
		qScale = intent.Target.TargetQty.Scale
	}
	per := int64(0)
	if n > 0 {
		per = int64(math.Ceil(float64(totalQty) / float64(n)))
	}
	slices := make([]wire.PlanSlice, 0, n)
	for i := 0; i < n; i++ {
		s0 := start + int64(i)*sliceInterval.Nanoseconds()
		s1 := start + int64(i+1)*sliceInterval.Nanoseconds()
		if s1 > end {
			s1 = end
		}
		slices = append(slices, wire.PlanSlice{
			SliceID: fmt.Sprintf("s%03d", i),
			Seq:     i,
			TimeWindow: wire.TimeWindow{
				StartTSNS: s0,
				EndTSNS:   s1,
			},
			Qty: wire.Quantity{Value: per, Scale: qScale},
		})
	}

	plan := wire.OrderPlan{
		Version:       "v1",
		PlanID:        fmt.Sprintf("plan-%s-%d", intent.IntentID, nowNS),
		IntentID:      intent.IntentID,
		GeneratedTSNS: nowNS,
		Algo:          intent.ExecutionAlgo,
		AlgoParams:    intent.AlgoParams,
		Slices:        slices,
		PriceStrategy: wire.PriceStrategy{
			OrderType:          "limit",
			TimeInForce:        "day",
			LimitOffsetBps:     0,
			PegMode:            "mid",
			PriceProtectionBps: math.Max(5.0, intent.Constraints.MaxSlippageBps),
		},
		CancelReplace: wire.CancelReplace{
			Mode:                "none",
			MinLifetimeMS:       250,
			ReplaceThresholdBps: 3.0,
			MaxReplacesPerSlice: 3,
		},
		RateLimit: wire.RateLimit{
			OrdersPerSec:  10,
			CancelsPerSec: 10,
			Burst:         20,
		},
		FaultTolerance: wire.FaultTolerance{
			MaxConsecutiveRejects: 3,
			MaxTotalRejects:       10,
			OnExhausted:           "pause_intent",
		},
	}

	// Allow market orders only if explicitly allowed.
	if intent.Constraints.AllowMarketOrders {
		if intent.Urgency == "urgent" {
			plan.PriceStrategy.OrderType = "market"
		}
	}

	// Algo-specific defaults.
	switch intent.ExecutionAlgo {
	case "twap":
		plan.PriceStrategy.PegMode = "mid"
	case "vwap":
		plan.PriceStrategy.PegMode = "mid"
	case "pov":
		plan.PriceStrategy.PegMode = "ask"
		if intent.Side == "sell" {
			plan.PriceStrategy.PegMode = "bid"
		}
	case "is_min_v1":
		plan.PriceStrategy.PegMode = "ask"
		if intent.Side == "sell" {
			plan.PriceStrategy.PegMode = "bid"
		}
		plan.PriceStrategy.LimitOffsetBps = 1.0
		plan.CancelReplace.Mode = "cancel_replace"
	}
	return plan
}

func buildOrderCmdRecord(intent wire.ExecutionIntent, runID string, codeVersion string, clientOrderID string, qty float64, limitPx float64, ps wire.PriceStrategy) wire.EventLogRecord {
	now := time.Now().UnixNano()
	accountID := intent.AccountScope.AccountID
	if accountID == "" {
		// For account_group intents, publish an empty account_id and rely on allocator + post-risk gate.
		// This keeps the wire contract stable while allowing multi-account execution at higher layers.
		accountID = ""
	}
	orderType := "ORDER_TYPE_LIMIT"
	if ps.OrderType == "market" {
		orderType = "ORDER_TYPE_MARKET"
	}
	tif := "TIME_IN_FORCE_DAY"
	switch ps.TimeInForce {
	case "gtc":
		tif = "TIME_IN_FORCE_GTC"
	case "ioc":
		tif = "TIME_IN_FORCE_IOC"
	case "fok":
		tif = "TIME_IN_FORCE_FOK"
	}
	side := "SIDE_BUY"
	if intent.Side == "sell" {
		side = "SIDE_SELL"
	}

	q := map[string]any{"value": int64(math.Round(qty)), "scale": 0}
	price := map[string]any{"value": 0, "scale": 2, "currency": "USD"}
	if limitPx > 0 {
		price = map[string]any{"value": int64(math.Round(limitPx * 100)), "scale": 2, "currency": "USD"}
	}

	env := map[string]any{
		"event_id":   fmt.Sprintf("order-%d", now),
		"event_type": "EVENT_TYPE_ORDER_CMD",
		"ts": map[string]any{
			"ts_event_ns": 0,
			"ts_recv_ns":  now,
			"ts_emit_ns":  now,
		},
		"account_id":   accountID,
		"venue":        intent.InstrumentID.Venue,
		"symbol":       intent.InstrumentID.Symbol,
		"run_id":       runID,
		"code_version": codeVersion,
		"payload": map[string]any{
			"order_cmd": map[string]any{
				"client_order_id": clientOrderID,
				"account_id":      accountID,
				"symbol":          intent.InstrumentID.Symbol,
				"venue":           intent.InstrumentID.Venue,
				"side":            side,
				"type":            orderType,
				"qty":             q,
				"price":           price,
				"time_in_force":   tif,
				// Extension fields (not in proto) are allowed by Phase0 contracts and used by adapters.
				"intent_id": intent.IntentID,
			},
		},
	}
	return wire.EventLogRecord{Type: "ORDER_CMD", Envelope: env}
}

func buildCancelOrderCmdRecord(intent wire.ExecutionIntent, runID string, codeVersion string, live childOrder) wire.EventLogRecord {
	now := time.Now().UnixNano()
	accountID := intent.AccountScope.AccountID
	side := "SIDE_BUY"
	if intent.Side == "sell" {
		side = "SIDE_SELL"
	}
	// Cancel is encoded via Phase1 adapter conventions:
	// - qty <= 0, or
	// - client_order_id prefixed with CANCEL-, plus optional broker order_id when available.
	q := map[string]any{"value": int64(0), "scale": 0}
	price := map[string]any{"value": int64(0), "scale": 2, "currency": "USD"}

	cancelCOID := "CANCEL-" + live.ClientOrderID
	orderCmd := map[string]any{
		"client_order_id":              cancelCOID,
		"account_id":                   accountID,
		"symbol":                       intent.InstrumentID.Symbol,
		"venue":                        intent.InstrumentID.Venue,
		"side":                         side,
		"type":                         "ORDER_TYPE_LIMIT",
		"qty":                          q,
		"price":                        price,
		"time_in_force":                "TIME_IN_FORCE_DAY",
		"intent_id":                    intent.IntentID,
		"cancel_target_client_order_id": live.ClientOrderID,
	}
	if live.OrderID != "" {
		orderCmd["order_id"] = live.OrderID
	}

	env := map[string]any{
		"event_id":   fmt.Sprintf("order-cancel-%d", now),
		"event_type": "EVENT_TYPE_ORDER_CMD",
		"ts": map[string]any{
			"ts_event_ns": 0,
			"ts_recv_ns":  now,
			"ts_emit_ns":  now,
		},
		"account_id":   accountID,
		"venue":        intent.InstrumentID.Venue,
		"symbol":       intent.InstrumentID.Symbol,
		"run_id":       runID,
		"code_version": codeVersion,
		"payload": map[string]any{
			"order_cmd": orderCmd,
		},
	}
	return wire.EventLogRecord{Type: "ORDER_CMD", Envelope: env}
}

func (c Config) RunIDOrDefault(runID string) string {
	if runID != "" {
		return runID
	}
	return c.RunID
}

