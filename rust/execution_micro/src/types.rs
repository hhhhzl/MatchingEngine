use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Side is the trading direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

/// PegMode selects the reference price used for limit placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PegMode {
    None,
    Mid,
    Bid,
    Ask,
}

/// PricePolicy controls how a candidate price is generated and clamped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PricePolicy {
    /// If present, clamp the resulting price to this hard limit:
    /// - Buy: price <= limit
    /// - Sell: price >= limit
    pub price_limit: Option<Decimal>,

    /// Offset in bps applied away from the chosen peg reference.
    /// For Buy: price = ref * (1 - bps/1e4)
    /// For Sell: price = ref * (1 + bps/1e4)
    pub limit_offset_bps: Decimal,

    /// Price protection in bps relative to mid.
    /// This prevents placing orders too far away from the current market.
    pub price_protection_bps: Decimal,

    /// If present, an additional slippage cap (bps) relative to mid.
    /// This is intended to be set dynamically using an online cost model.
    #[serde(default)]
    pub max_slippage_bps: Option<Decimal>,

    /// Peg selection: bid/ask/mid/none.
    pub peg_mode: PegMode,
}

impl Default for PricePolicy {
    fn default() -> Self {
        Self {
            price_limit: None,
            limit_offset_bps: Decimal::ZERO,
            price_protection_bps: Decimal::new(5, 0), // 5 bps default
            max_slippage_bps: None,
            peg_mode: PegMode::Mid,
        }
    }
}

/// CancelReplacePolicy controls when a working order should be canceled/replaced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelReplacePolicy {
    /// Minimum age of a working order before we consider cancel/replace.
    pub min_lifetime_ms: u64,

    /// Replace threshold in bps between current working price and desired price.
    pub replace_threshold_bps: Decimal,

    /// Hard cap on replacements per slice (caller tracks per-slice count).
    pub max_replaces: u32,
}

impl Default for CancelReplacePolicy {
    fn default() -> Self {
        Self {
            min_lifetime_ms: 250,
            replace_threshold_bps: Decimal::new(3, 0), // 3 bps
            max_replaces: 3,
        }
    }
}

/// MicroOrder describes the currently working (live) order state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MicroOrder {
    /// Client order id (used for traceability).
    pub client_order_id: String,
    /// Remaining working quantity (shares/contracts).
    pub working_qty: Decimal,
    /// Current working price (limit).
    pub working_price: Decimal,
    /// Timestamp when the order was last updated/acknowledged (ns).
    pub last_update_ts_ns: i64,
    /// Replacement counter already applied for the current slice.
    pub replaces: u32,
}

/// MicroInput is the full decision context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MicroInput {
    /// Instrument identifiers (for logs/traceability; not used in math).
    pub symbol: String,
    pub venue: String,

    pub side: Side,

    /// Desired quantity to add (positive).
    pub desired_qty: Decimal,

    /// Tick size and lot size constraints (both > 0).
    pub tick_size: Decimal,
    pub lot_size: Decimal,

    /// If present, enforce min notional: qty * mid >= min_notional.
    pub min_notional: Option<Decimal>,

    /// Best bid/ask/last. Missing fields are treated as unavailable.
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub last: Option<Decimal>,

    /// Policy for price generation and safety clamps.
    pub price_policy: PricePolicy,

    /// Cancel/replace policy.
    pub cr_policy: CancelReplacePolicy,

    /// Current working order (if any).
    pub current: Option<MicroOrder>,

    /// Current timestamp in nanoseconds.
    pub now_ts_ns: i64,
}

/// MicroDecision is a deterministic action proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum MicroDecision {
    /// Do nothing on this tick.
    Noop { reason: String },

    /// Place a new limit order.
    Place {
        qty: Decimal,
        price: Decimal,
        reason: String,
    },

    /// Cancel the current working order (client order id must match input.current).
    Cancel {
        client_order_id: String,
        reason: String,
    },

    /// Cancel and replace with a new price/qty (caller emits cancel + new order).
    Replace {
        cancel_client_order_id: String,
        new_qty: Decimal,
        new_price: Decimal,
        reason: String,
    },
}

