//! Matching Engine Library
//!
//! A high-performance order matching engine implementing price-time priority matching.
//! Supports Limit and Market orders with partial fills.
//!
//! Features:
//! - L2 matching: Price-level aggregation (fast, efficient)
//! - L3 matching: Individual order queue positions (precise, advanced)
//! - Advanced order types: Post-only, Hidden, Stop, Stop-Limit
//! - Market rules engine: Risk controls, circuit breakers, trading halts

pub mod types;
pub mod orderbook;
pub mod orderbook_l3;
pub mod order_types;
pub mod rules_engine;
pub mod matching;
pub mod engine_l2;
pub mod engine_l3;
pub mod error;
mod ffi;

// L2 exports
pub use types::{
    Order, Trade, Side, OrderType, TimeInForce, OrderStatus,
    EngineEvent, EngineEventKind, SeqNum, Liquidity, DoneReason,
    BookLevel, BookSnapshot, BookDeltaReason,
    MatchPriceRule, MarketStatus,
};
pub use orderbook::OrderBook;
pub use engine_l2::MatchingEngine;

// L3 exports
pub use orderbook_l3::OrderBookL3;
pub use order_types::{PostOnlyOrder, HiddenOrder, StopOrder, StopLimitOrder};
pub use rules_engine::{MarketRulesEngine, Rule, RuleAction, RuleType};
pub use engine_l3::MatchingEngineL3;

// Common exports
pub use error::{MatchingError, Result};
