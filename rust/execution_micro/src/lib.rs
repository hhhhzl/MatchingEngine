//! Execution micro-decision library (fast-path).
//!
//! This crate provides deterministic, low-latency decision helpers used on the hot path:
//! - Tick/lot/min-notional alignment (fixed-point safe).
//! - Price protection and limit clamping.
//! - Cancel/replace decisions given current working order and latest market data.
//!
//! Design constraints:
//! - Deterministic: same input -> same output.
//! - No network I/O; pure computation.
//! - Explicit rounding rules to avoid off-by-one tick issues.

mod math;
mod model;
mod types;

pub use model::{decide, DecisionError};
pub use types::{
    CancelReplacePolicy, MicroDecision, MicroInput, MicroOrder, PegMode, PricePolicy, Side,
};

