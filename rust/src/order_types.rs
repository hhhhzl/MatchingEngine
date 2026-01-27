//! Advanced order types for L3 matching engine

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::types::{Order, Side, OrderType};

/// Post-only order: Only adds liquidity, never takes
///
/// If the order would immediately match, it is rejected instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostOnlyOrder {
    /// Base order
    pub order: Order,
    /// Whether order was rejected due to immediate match
    pub rejected: bool,
}

impl PostOnlyOrder {
    /// Create a new post-only order
    pub fn new(order: Order) -> Self {
        Self {
            order,
            rejected: false,
        }
    }
}

/// Hidden order: Not visible in order book
///
/// Hidden orders participate in matching but are not displayed
/// in the order book. Used for large orders to avoid market impact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HiddenOrder {
    /// Base order
    pub order: Order,
    /// Display quantity (if any) - shows smaller qty in book
    pub display_qty: Option<Decimal>,
}

impl HiddenOrder {
    /// Create a new hidden order
    pub fn new(order: Order, display_qty: Option<Decimal>) -> Self {
        Self {
            order,
            display_qty,
        }
    }
    
    /// Get the visible quantity in the order book
    pub fn visible_qty(&self) -> Decimal {
        self.display_qty.unwrap_or(Decimal::ZERO)
    }
    
    /// Get the hidden quantity (total - visible)
    pub fn hidden_qty(&self) -> Decimal {
        self.order.qty - self.visible_qty()
    }
}

/// Stop order: Triggers when price reaches stop level
///
/// Once triggered, becomes a market order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopOrder {
    /// Base order (becomes market order when triggered)
    pub order: Order,
    /// Stop price level
    pub stop_price: Decimal,
    /// Whether stop has been triggered
    pub triggered: bool,
}

impl StopOrder {
    /// Create a new stop order
    pub fn new(order: Order, stop_price: Decimal) -> Self {
        Self {
            order,
            stop_price,
            triggered: false,
        }
    }
    
    /// Check if stop should be triggered
    pub fn check_trigger(&mut self, current_price: Decimal) -> bool {
        if self.triggered {
            return true;
        }
        
        match self.order.side {
            Side::Buy => {
                // Buy stop: trigger when price rises above stop_price
                if current_price >= self.stop_price {
                    self.triggered = true;
                    self.order.order_type = OrderType::Market;
                    return true;
                }
            }
            Side::Sell => {
                // Sell stop: trigger when price falls below stop_price
                if current_price <= self.stop_price {
                    self.triggered = true;
                    self.order.order_type = OrderType::Market;
                    return true;
                }
            }
        }
        
        false
    }
}

/// Stop-limit order: Combines stop and limit
///
/// When stop is triggered, becomes a limit order at specified price.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopLimitOrder {
    /// Base order (becomes limit order when triggered)
    pub order: Order,
    /// Stop price level
    pub stop_price: Decimal,
    /// Limit price (used after trigger)
    pub limit_price: Decimal,
    /// Whether stop has been triggered
    pub triggered: bool,
}

impl StopLimitOrder {
    /// Create a new stop-limit order
    pub fn new(order: Order, stop_price: Decimal, limit_price: Decimal) -> Self {
        Self {
            order,
            stop_price,
            limit_price,
            triggered: false,
        }
    }
    
    /// Check if stop should be triggered
    pub fn check_trigger(&mut self, current_price: Decimal) -> bool {
        if self.triggered {
            return true;
        }
        
        match self.order.side {
            Side::Buy => {
                // Buy stop: trigger when price rises above stop_price
                if current_price >= self.stop_price {
                    self.triggered = true;
                    self.order.order_type = OrderType::Limit;
                    self.order.price = self.limit_price;
                    return true;
                }
            }
            Side::Sell => {
                // Sell stop: trigger when price falls below stop_price
                if current_price <= self.stop_price {
                    self.triggered = true;
                    self.order.order_type = OrderType::Limit;
                    self.order.price = self.limit_price;
                    return true;
                }
            }
        }
        
        false
    }
}
