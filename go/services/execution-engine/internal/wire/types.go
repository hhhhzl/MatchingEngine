package wire

import "encoding/json"

// GatewayMessage is the outer frame used by the Event Gateway TCP protocol.
// Example:
//   {"topic":"v1.order.cmd.request","msg":{...EventLogRecord...}}
type GatewayMessage struct {
	Topic string          `json:"topic"`
	Msg   json.RawMessage `json:"msg"`
}

// EventLogRecord is the inner append-only NDJSON record that all services share.
// We keep this generic (map payload) to avoid hard-coupling services to generated protobufs.
type EventLogRecord struct {
	Type     string                 `json:"type"`
	Envelope map[string]interface{} `json:"envelope"`
}

// Quantity matches proto marginforest.v1.Quantity JSON shape.
// It uses fixed-point integer representation: value * 10^-scale.
type Quantity struct {
	Value int64 `json:"value"`
	Scale int32 `json:"scale"`
}

// Money matches proto marginforest.v1.Money JSON shape.
type Money struct {
	Value    int64  `json:"value"`
	Scale    int32  `json:"scale"`
	Currency string `json:"currency"`
}

// ExecutionIntent is the strongly-typed version of specs/execution_intent.jsonschema.
// This struct is used internally by the execution-engine service.
type ExecutionIntent struct {
	Version    string `json:"version"`
	IntentID   string `json:"intent_id"`
	RunID      string `json:"run_id"`
	StrategyID string `json:"strategy_id"`

	AccountScope   AccountScope       `json:"account_scope"`
	InstrumentID   InstrumentID       `json:"instrument_id"`
	Side           string             `json:"side"` // buy/sell
	Target         Target             `json:"target"`
	TimeWindow     TimeWindow         `json:"time_window"`
	Urgency        string             `json:"urgency"`        // low/normal/high/urgent
	ExecutionAlgo  string             `json:"execution_algo"` // twap/vwap/pov/is_min_v1
	AlgoParams     map[string]any     `json:"algo_params,omitempty"`
	Constraints    Constraints        `json:"constraints"`
	Benchmark      Benchmark          `json:"benchmark"`
	Tags           map[string]string  `json:"tags,omitempty"`
}

type InstrumentID struct {
	Symbol     string `json:"symbol"`
	Venue      string `json:"venue"`
	AssetClass string `json:"asset_class,omitempty"`
}

type AccountScope struct {
	Mode           string `json:"mode"` // single_account/account_group
	AccountID      string `json:"account_id,omitempty"`
	AccountGroupID string `json:"account_group_id,omitempty"`
}

type Target struct {
	TargetQty      *Quantity `json:"target_qty,omitempty"`
	TargetNotional *Money    `json:"target_notional,omitempty"`
}

type TimeWindow struct {
	StartTSNS int64 `json:"start_ts_ns"`
	EndTSNS   int64 `json:"end_ts_ns"`
}

type Constraints struct {
	MaxParticipation  float64   `json:"max_participation"`
	PriceLimit        *Money    `json:"price_limit,omitempty"`
	MaxSlippageBps    float64   `json:"max_slippage_bps"`
	MinFillQty        *Quantity `json:"min_fill_qty,omitempty"`
	MaxOrderQty       *Quantity `json:"max_order_qty,omitempty"`
	MaxChildOrders    *int      `json:"max_child_orders,omitempty"`
	AllowMarketOrders bool      `json:"allow_market_orders"`
	ReduceOnly        bool      `json:"reduce_only"`
}

type Benchmark struct {
	Type         string `json:"type"` // arrival_price/vwap/twap
	ArrivalPrice *Money `json:"arrival_price,omitempty"`
}

// OrderPlan is the strongly-typed version of specs/order_plan.jsonschema.
type OrderPlan struct {
	Version        string         `json:"version"`
	PlanID         string         `json:"plan_id"`
	IntentID       string         `json:"intent_id"`
	GeneratedTSNS  int64          `json:"generated_ts_ns"`
	Algo           string         `json:"algo"`
	AlgoParams     map[string]any `json:"algo_params,omitempty"`
	Slices         []PlanSlice    `json:"slices"`
	PriceStrategy  PriceStrategy  `json:"price_strategy"`
	CancelReplace  CancelReplace  `json:"cancel_replace"`
	RateLimit      RateLimit      `json:"rate_limit"`
	FaultTolerance FaultTolerance `json:"fault_tolerance"`
}

type PlanSlice struct {
	SliceID    string     `json:"slice_id"`
	Seq        int        `json:"seq"`
	TimeWindow TimeWindow `json:"time_window"`
	Qty        Quantity   `json:"qty"`
	MinFillQty *Quantity  `json:"min_fill_qty,omitempty"`
	PriceLimit *Money     `json:"price_limit,omitempty"`
}

type PriceStrategy struct {
	OrderType          string  `json:"order_type"` // limit/market
	TimeInForce        string  `json:"time_in_force"`
	LimitOffsetBps     float64 `json:"limit_offset_bps"`
	PegMode            string  `json:"peg_mode"`
	PriceProtectionBps float64 `json:"price_protection_bps"`
}

type CancelReplace struct {
	Mode                string  `json:"mode"` // none/cancel_replace
	MinLifetimeMS       int64   `json:"min_lifetime_ms"`
	ReplaceThresholdBps float64 `json:"replace_threshold_bps"`
	MaxReplacesPerSlice int     `json:"max_replaces_per_slice"`
}

type RateLimit struct {
	OrdersPerSec  float64 `json:"orders_per_sec"`
	CancelsPerSec float64 `json:"cancels_per_sec"`
	Burst         float64 `json:"burst"`
}

type FaultTolerance struct {
	MaxConsecutiveRejects int    `json:"max_consecutive_rejects"`
	MaxTotalRejects       int    `json:"max_total_rejects"`
	OnExhausted           string `json:"on_exhausted"` // pause_intent/cancel_all/terminate_intent
}

