//! Market Rules Engine
//!
//! Enforces market rules such as:
//! - Price limits (circuit breakers)
//! - Position limits
//! - Order size limits
//! - Trading halts
//! - Risk controls

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{Order, Side, OrderType};
use crate::error::{Result, MatchingError};

/// Rule action to take when rule is violated
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleAction {
    /// Reject the order
    Reject,
    /// Allow but log warning
    Warn,
    /// Allow but throttle (delay)
    Throttle,
    /// Halt trading for symbol
    Halt,
}

/// Market rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Rule identifier
    pub rule_id: String,
    /// Rule name
    pub name: String,
    /// Rule type
    pub rule_type: RuleType,
    /// Action to take when violated
    pub action: RuleAction,
    /// Rule parameters
    pub parameters: HashMap<String, String>,
    /// Whether rule is enabled
    pub enabled: bool,
}

/// Rule type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleType {
    /// Price limit (circuit breaker)
    PriceLimit,
    /// Position limit
    PositionLimit,
    /// Order size limit
    OrderSizeLimit,
    /// Trading halt
    TradingHalt,
    /// Price change limit (percentage)
    PriceChangeLimit,
    /// Volume limit
    VolumeLimit,
}

/// Market rules engine
pub struct MarketRulesEngine {
    /// Rules by symbol
    rules_by_symbol: HashMap<String, Vec<Rule>>,
    /// Global rules (apply to all symbols)
    global_rules: Vec<Rule>,
    /// Current positions by account
    positions: HashMap<String, HashMap<String, Decimal>>, // account_id -> symbol -> position
    /// Trading halt status by symbol
    halted_symbols: std::collections::HashSet<String>,
    /// Last prices for price change calculation
    last_prices: HashMap<String, Decimal>,
}

impl MarketRulesEngine {
    /// Create a new rules engine
    pub fn new() -> Self {
        Self {
            rules_by_symbol: HashMap::new(),
            global_rules: Vec::new(),
            positions: HashMap::new(),
            halted_symbols: std::collections::HashSet::new(),
            last_prices: HashMap::new(),
        }
    }

    /// Add a rule for a specific symbol
    pub fn add_symbol_rule(&mut self, symbol: String, rule: Rule) {
        self.rules_by_symbol.entry(symbol).or_default().push(rule);
    }

    /// Add a global rule
    pub fn add_global_rule(&mut self, rule: Rule) {
        self.global_rules.push(rule);
    }

    /// Check if an order violates any rules
    ///
    /// Returns Ok(()) if order passes, Err with reason if rejected.
    pub fn check_order(&mut self, order: &Order, current_price: Option<Decimal>) -> Result<()> {
        // Check if symbol is halted
        if self.halted_symbols.contains(&order.symbol) {
            return Err(MatchingError::InvalidOrder(
                format!("Trading halted for symbol: {}", order.symbol),
            ));
        }

        // Get rules for this symbol
        let symbol_rules = self.rules_by_symbol.get(&order.symbol).cloned().unwrap_or_default();
        let all_rules: Vec<Rule> = self.global_rules.iter()
            .chain(symbol_rules.iter())
            .cloned()
            .collect();

        // Check each rule
        for rule in all_rules {
            if !rule.enabled {
                continue;
            }

            if self._check_rule(&rule, order, current_price)? {
                // Rule violated
                match rule.action {
                    RuleAction::Reject => {
                        return Err(MatchingError::InvalidOrder(
                            format!("Rule violated: {} ({})", rule.name, rule.rule_id),
                        ));
                    }
                    RuleAction::Halt => {
                        self.halted_symbols.insert(order.symbol.clone());
                        return Err(MatchingError::InvalidOrder(
                            format!("Trading halted due to rule: {} ({})", rule.name, rule.rule_id),
                        ));
                    }
                    RuleAction::Warn => {
                        // Log warning but allow
                        eprintln!("Warning: Rule {} violated for order {}", rule.rule_id, order.order_id);
                    }
                    RuleAction::Throttle => {
                        // Would implement throttling (delay) here
                        eprintln!("Throttle: Rule {} violated for order {}", rule.rule_id, order.order_id);
                    }
                }
            }
        }

        Ok(())
    }

    /// Update position for an account
    pub fn update_position(&mut self, account_id: &str, symbol: &str, position: Decimal) {
        self.positions
            .entry(account_id.to_string())
            .or_default()
            .insert(symbol.to_string(), position);
    }

    /// Update last price for price change calculations
    pub fn update_last_price(&mut self, symbol: &str, price: Decimal) {
        self.last_prices.insert(symbol.to_string(), price);
    }

    /// Resume trading for a symbol
    pub fn resume_trading(&mut self, symbol: &str) {
        self.halted_symbols.remove(symbol);
    }

    /// Check if a rule is violated
    fn _check_rule(&self, rule: &Rule, order: &Order, current_price: Option<Decimal>) -> Result<bool> {
        match rule.rule_type {
            RuleType::PriceLimit => {
                // Check if the effective price is within limits.
                //
                // - For limit orders, the relevant price is the order's limit price.
                // - For market orders, use the current market price (if available).
                //
                // This matches typical exchange behavior: price bands constrain the order price
                // for limit orders and constrain execution context for market orders.
                let effective_price = match order.order_type {
                    OrderType::Limit => Some(order.price),
                    OrderType::Market => current_price,
                    _ => Some(order.price),
                };

                if let Some(price) = effective_price {
                    let min_price = rule.parameters.get("min_price")
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);
                    let max_price = rule.parameters.get("max_price")
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::MAX);

                    if price < min_price || price > max_price {
                        return Ok(true);
                    }
                }

                Ok(false)
            }
            RuleType::OrderSizeLimit => {
                // Check if order size exceeds limit
                let max_size = rule.parameters.get("max_size")
                    .and_then(|s| s.parse::<Decimal>().ok())
                    .unwrap_or(Decimal::MAX);

                if order.qty > max_size {
                    return Ok(true);
                }
                Ok(false)
            }
            RuleType::PositionLimit => {
                // Check if position would exceed limit
                let account_id = rule.parameters.get("account_id");
                if let Some(account_id) = account_id {
                    if let Some(account_positions) = self.positions.get(account_id) {
                        let current_position = account_positions.get(&order.symbol)
                            .copied()
                            .unwrap_or(Decimal::ZERO);
                        
                        let max_position = rule.parameters.get("max_position")
                            .and_then(|s| s.parse::<Decimal>().ok())
                            .unwrap_or(Decimal::MAX);

                        let new_position = match order.side {
                            Side::Buy => current_position + order.qty,
                            Side::Sell => current_position - order.qty,
                        };

                        if new_position.abs() > max_position {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            RuleType::PriceChangeLimit => {
                // Check if price change exceeds limit
                if let Some(price) = current_price {
                    if let Some(last_price) = self.last_prices.get(&order.symbol) {
                        let change_pct = ((price - last_price) / *last_price).abs();
                        let max_change = rule.parameters.get("max_change_pct")
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(1.0);

                        // Convert Decimal to f64 for comparison
                        let change_pct_f64: f64 = change_pct.to_string().parse().unwrap_or(0.0);
                        if change_pct_f64 > max_change {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            RuleType::TradingHalt => {
                // Check if trading is halted
                Ok(self.halted_symbols.contains(&order.symbol))
            }
            RuleType::VolumeLimit => {
                // Would need to track volume - simplified for now
                Ok(false)
            }
        }
    }
}

impl Default for MarketRulesEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_limit_rule() {
        let mut engine = MarketRulesEngine::new();
        
        let rule = Rule {
            rule_id: "price_limit_1".to_string(),
            name: "Price Limit".to_string(),
            rule_type: RuleType::PriceLimit,
            action: RuleAction::Reject,
            parameters: {
                let mut params = HashMap::new();
                params.insert("min_price".to_string(), "90.0".to_string());
                params.insert("max_price".to_string(), "110.0".to_string());
                params
            },
            enabled: true,
        };
        
        engine.add_global_rule(rule);
        
        let order = Order::new_with_account(
            "client_1".to_string(),
            "acct".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(12000, 2), // 120.00
            Decimal::new(100, 0),
            crate::types::TimeInForce::GTC,
            1000,
        );
        
        assert!(engine.check_order(&order, Some(Decimal::new(12000, 2))).is_err());
    }
}
