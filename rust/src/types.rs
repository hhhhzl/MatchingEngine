//! Core types for the matching engine

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Sequence number for deterministic event streams.
pub type SeqNum = u64;

/// Order side: Buy or Sell
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy order
    Buy,
    /// Sell order
    Sell,
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order - executes at best available price
    Market,
    /// Limit order - executes at specified price or better
    Limit,
    /// Stop order - triggers when price reaches stop level
    Stop,
    /// Stop limit order - combines stop and limit
    StopLimit,
}

/// Time in force for orders
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good Till Cancel - order remains active until cancelled
    GTC,
    /// Day order - expires at end of trading day
    Day,
    /// Immediate or Cancel - execute immediately or cancel
    IOC,
    /// Fill or Kill - execute completely or cancel
    FOK,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    /// New order, not yet acknowledged
    New,
    /// Order acknowledged by the exchange
    Ack,
    /// Order partially filled
    Partial,
    /// Order fully filled
    Filled,
    /// Order cancelled
    Canceled,
    /// Order rejected
    Rejected,
}

/// Represents an order in the matching engine
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Order {
    /// Unique order identifier assigned by the engine
    pub order_id: String,
    /// Client-provided order identifier
    pub client_order_id: String,
    /// Trading symbol
    pub symbol: String,
    /// Order side (Buy or Sell)
    pub side: Side,
    /// Order type (Market, Limit, etc.)
    pub order_type: OrderType,
    /// Limit price (required for Limit orders, ignored for Market orders)
    pub price: Decimal,
    /// Original order quantity
    pub qty: Decimal,
    /// Cumulative filled quantity
    pub cum_qty: Decimal,
    /// Remaining quantity to be filled
    pub leaves_qty: Decimal,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Order timestamp in nanoseconds
    pub timestamp_ns: i64,
    /// Current order status
    pub status: OrderStatus,
}

impl Order {
    /// Create a new order
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_order_id: String,
        symbol: String,
        side: Side,
        order_type: OrderType,
        price: Decimal,
        qty: Decimal,
        time_in_force: TimeInForce,
        timestamp_ns: i64,
    ) -> Self {
        Self {
            order_id: String::new(),
            client_order_id,
            symbol,
            side,
            order_type,
            price,
            qty,
            cum_qty: Decimal::ZERO,
            leaves_qty: qty,
            time_in_force,
            timestamp_ns,
            status: OrderStatus::New,
        }
    }
}

/// Represents a trade (fill) resulting from order matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Unique trade identifier
    pub trade_id: String,
    /// Order identifier of the incoming order
    pub order_id: String,
    /// Client order identifier of the incoming order
    pub client_order_id: String,
    /// Order identifier of the contra order (if available)
    pub contra_order_id: Option<String>,
    /// Trading symbol
    pub symbol: String,
    /// Trade side (Buy or Sell)
    pub side: Side,
    /// Trade execution price
    pub price: Decimal,
    /// Trade quantity
    pub qty: Decimal,
    /// Trade timestamp in nanoseconds
    pub timestamp_ns: i64,
}

/// Liquidity side of a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Liquidity {
    /// The resting order (provided liquidity).
    Maker,
    /// The incoming order (took liquidity).
    Taker,
    /// Auction uncrossing fill (no maker/taker distinction).
    Auction,
}

/// Trade price determination rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchPriceRule {
    /// Trade at the resting (maker) price.
    Maker,
    /// Trade at the incoming (taker) limit price when available, otherwise maker.
    Taker,
    /// Trade at midpoint of best bid/ask when available, otherwise maker.
    Midpoint,
}

/// Market state for a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketStatus {
    /// Accept orders, do not match until auction.
    PreOpen,
    /// Continuous trading.
    Open,
    /// Trading halted (reject new orders; allow cancels).
    Halted,
    /// Market closed (reject new orders; allow cancels).
    Closed,
}

/// Terminal reason for an order lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DoneReason {
    /// The order was fully filled.
    Filled,
    /// The order was canceled by user action or time-in-force rules.
    Canceled,
    /// The order was rejected by validation or rules.
    Rejected,
    /// The order expired (e.g., Day order at end-of-day).
    Expired,
}

/// One aggregated book level (price, total visible quantity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Decimal,
    pub qty: Decimal,
}

/// Full book snapshot (aggregated L2 levels).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

/// Reason for a book delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookDeltaReason {
    Add,
    Cancel,
    Fill,
    Replace,
    Auction,
}

/// Execution-style event stream for all engine actions.
///
/// This is the stable output contract intended for replay, auditing, and bindings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineEvent {
    /// Monotonic per-engine sequence number.
    pub seq: SeqNum,
    /// Event timestamp in nanoseconds.
    pub timestamp_ns: i64,
    /// Trading symbol.
    pub symbol: String,
    /// Event payload.
    pub kind: EngineEventKind,
}

/// Event payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEventKind {
    /// Order acknowledged (accepted and either rested or immediately eligible to match).
    Ack {
        order_id: String,
        client_order_id: String,
        status: OrderStatus,
        leaves_qty: Decimal,
        cum_qty: Decimal,
    },
    /// Order rejected.
    Reject {
        order_id: String,
        client_order_id: String,
        reason: String,
    },
    /// Cancel acknowledged.
    CancelAck {
        order_id: String,
        client_order_id: String,
        leaves_qty: Decimal,
        cum_qty: Decimal,
    },
    /// Replace/amend acknowledged.
    ReplaceAck {
        order_id: String,
        client_order_id: String,
        new_price: Decimal,
        new_qty: Decimal,
        leaves_qty: Decimal,
        cum_qty: Decimal,
    },
    /// Fill event for one side of a trade.
    Fill {
        trade_id: String,
        order_id: String,
        client_order_id: String,
        contra_order_id: String,
        side: Side,
        liquidity: Liquidity,
        price: Decimal,
        qty: Decimal,
        leaves_qty: Decimal,
        cum_qty: Decimal,
    },
    /// Terminal lifecycle event for an order.
    Done {
        order_id: String,
        client_order_id: String,
        reason: DoneReason,
    },
    /// Aggregated book delta for one price level.
    ///
    /// Applying these deltas in `seq` order reconstructs the L2 book state when
    /// combined with an initial `BookSnapshot`.
    BookDelta {
        side: Side,
        price: Decimal,
        /// Signed quantity change at this level (positive for add, negative for remove).
        delta_qty: Decimal,
        /// New total quantity at this level after applying the delta.
        new_qty: Decimal,
        reason: BookDeltaReason,
    },
    /// Aggregated book snapshot (top N levels).
    BookSnapshot {
        depth: usize,
        snapshot: BookSnapshot,
    },
    /// Market status transition.
    MarketStatus {
        symbol: String,
        status: MarketStatus,
    },
}

/// Wrapper for Order to enable price-time priority comparison
///
/// Priority rules:
/// 1. Price priority: Buy orders with higher prices have priority, Sell orders with lower prices have priority
/// 2. Time priority: Among orders with the same price, earlier timestamps have priority
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct OrderComparable {
    /// The order being compared
    pub order: Order,
}

impl Ord for OrderComparable {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.order.side != other.order.side {
            panic!("can only compare same side order");
        }

        // Price-time priority:
        // 1. Price priority (buy: higher better, sell: lower better)
        // 2. Time priority (earlier timestamp better)
        // Returns Ordering::Greater if self has higher priority than other
        let price_cmp = match self.order.side {
            Side::Buy => {
                // Buy: higher price is better (greater priority)
                // If self.price > other.price, self has higher priority (Greater)
                self.order.price.cmp(&other.order.price)
            }
            Side::Sell => {
                // Sell: lower price is better (greater priority)
                // If self.price < other.price, self has higher priority (Greater)
                other.order.price.cmp(&self.order.price)
            }
        };

        match price_cmp {
            Ordering::Equal => {
                // Same price: earlier timestamp is better (greater priority)
                // If self.timestamp < other.timestamp, self has higher priority (Greater)
                other.order.timestamp_ns.cmp(&self.order.timestamp_ns)
            }
            _ => price_cmp,
        }
    }
}

impl PartialOrd for OrderComparable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Market data snapshot from an order book
#[derive(Debug, Clone)]
pub struct MarketData {
    /// Symbol
    pub symbol: String,
    /// Best bid price
    pub best_bid: Option<Decimal>,
    /// Best bid quantity
    pub best_bid_qty: Option<Decimal>,
    /// Best ask price
    pub best_ask: Option<Decimal>,
    /// Best ask quantity
    pub best_ask_qty: Option<Decimal>,
    /// Last trade price
    pub last_trade_price: Option<Decimal>,
}
