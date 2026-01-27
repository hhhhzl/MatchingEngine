//! L3 Matching Engine
//!
//! Enhanced matching engine with L3 order book, advanced order types,
//! and market rules engine.

use std::collections::HashMap;
use rust_decimal::Decimal;

use crate::types::{Order, Trade};
use crate::error::{Result, MatchingError};
use crate::orderbook_l3::OrderBookL3;
use crate::order_types::{PostOnlyOrder, HiddenOrder, StopOrder, StopLimitOrder};
use crate::rules_engine::{MarketRulesEngine, Rule};

/// L3 Matching Engine with queue position tracking
pub struct MatchingEngineL3 {
    /// L3 order books by symbol
    orderbooks: HashMap<String, OrderBookL3>,
    /// Market rules engine
    rules_engine: MarketRulesEngine,
    /// Current prices by symbol (for stop orders)
    current_prices: HashMap<String, Decimal>,
    /// Order ID counter
    order_id_counter: u64,
}

impl MatchingEngineL3 {
    /// Create a new L3 matching engine
    pub fn new(symbols: Vec<String>) -> Self {
        let orderbooks = symbols
            .into_iter()
            .map(|symbol| (symbol.clone(), OrderBookL3::new(symbol)))
            .collect();

        Self {
            orderbooks,
            rules_engine: MarketRulesEngine::new(),
            current_prices: HashMap::new(),
            order_id_counter: 0,
        }
    }

    /// Add a symbol
    pub fn add_symbol(&mut self, symbol: String) {
        if !self.orderbooks.contains_key(&symbol) {
            self.orderbooks.insert(symbol.clone(), OrderBookL3::new(symbol));
        }
    }

    /// Add a market rule
    pub fn add_rule(&mut self, symbol: Option<String>, rule: Rule) {
        if let Some(symbol) = symbol {
            self.rules_engine.add_symbol_rule(symbol, rule);
        } else {
            self.rules_engine.add_global_rule(rule);
        }
    }

    /// Submit a regular order
    pub fn submit_order(&mut self, mut order: Order) -> Result<Vec<Trade>> {
        // Generate order_id if not provided
        if order.order_id.is_empty() {
            use uuid::Uuid;
            self.order_id_counter += 1;
            order.order_id = format!("ORDER_{}_{}", self.order_id_counter, Uuid::new_v4());
        }

        // Get or create orderbook
        if !self.orderbooks.contains_key(&order.symbol) {
            self.add_symbol(order.symbol.clone());
        }

        // Check market rules
        let current_price = self.current_prices.get(&order.symbol).copied();
        self.rules_engine.check_order(&order, current_price)?;

        // Get orderbook
        let orderbook = self.orderbooks
            .get_mut(&order.symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(order.symbol.clone()))?;

        // Match the order
        let trades = orderbook.match_order(order);

        // Update current price if trade occurred
        if let Some(last_trade) = trades.last() {
            self.current_prices.insert(last_trade.symbol.clone(), last_trade.price);
            self.rules_engine.update_last_price(&last_trade.symbol, last_trade.price);
        }

        Ok(trades)
    }

    /// Submit a post-only order
    pub fn submit_post_only(&mut self, post_only: PostOnlyOrder) -> Result<Vec<Trade>> {
        let order = post_only.order.clone();
        
        // Check rules
        let current_price = self.current_prices.get(&order.symbol).copied();
        self.rules_engine.check_order(&order, current_price)?;

        // Get orderbook
        let orderbook = self.orderbooks
            .get_mut(&order.symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(order.symbol.clone()))?;

        // Add as post-only (will reject if would immediately match)
        orderbook.add_post_only_order(post_only)?;

        // Post-only orders don't match immediately
        Ok(Vec::new())
    }

    /// Submit a hidden order
    pub fn submit_hidden(&mut self, hidden: HiddenOrder) -> Result<Vec<Trade>> {
        let order = hidden.order.clone();
        
        // Check rules
        let current_price = self.current_prices.get(&order.symbol).copied();
        self.rules_engine.check_order(&order, current_price)?;

        // Get orderbook
        let orderbook = self.orderbooks
            .get_mut(&order.symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(order.symbol.clone()))?;

        // Match as a hidden order (any unfilled remainder rests as hidden)
        let trades = orderbook.match_hidden_order(hidden);

        // Update current price if trade occurred
        if let Some(last_trade) = trades.last() {
            self.current_prices.insert(last_trade.symbol.clone(), last_trade.price);
            self.rules_engine.update_last_price(&last_trade.symbol, last_trade.price);
        }

        Ok(trades)
    }

    /// Submit a stop order
    pub fn submit_stop(&mut self, mut stop: StopOrder) -> Result<Vec<Trade>> {
        // Check if stop should be triggered
        if let Some(current_price) = self.current_prices.get(&stop.order.symbol) {
            stop.check_trigger(*current_price);
        }

        if stop.triggered {
            // Stop triggered, submit as market order
            self.submit_order(stop.order)
        } else {
            // Store stop order for later checking (simplified - would need stop order book)
            Ok(Vec::new())
        }
    }

    /// Submit a stop-limit order
    pub fn submit_stop_limit(&mut self, mut stop_limit: StopLimitOrder) -> Result<Vec<Trade>> {
        // Check if stop should be triggered
        if let Some(current_price) = self.current_prices.get(&stop_limit.order.symbol) {
            stop_limit.check_trigger(*current_price);
        }

        if stop_limit.triggered {
            // Stop triggered, submit as limit order
            self.submit_order(stop_limit.order)
        } else {
            // Store for later checking
            Ok(Vec::new())
        }
    }

    /// Cancel an order
    pub fn cancel_order(&mut self, symbol: &str, order_id: &str) -> Result<Order> {
        let orderbook = self.orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;

        orderbook.cancel_order(order_id)
    }

    /// Get queue position for an order
    pub fn get_queue_position(&self, symbol: &str, order_id: &str) -> Option<usize> {
        self.orderbooks
            .get(symbol)?
            .get_queue_position(order_id)
    }

    /// Get orderbook
    pub fn get_orderbook(&self, symbol: &str) -> Option<&OrderBookL3> {
        self.orderbooks.get(symbol)
    }

    /// Update current price (for stop order checking)
    pub fn update_price(&mut self, symbol: &str, price: Decimal) {
        self.current_prices.insert(symbol.to_string(), price);
        self.rules_engine.update_last_price(symbol, price);
    }
}
