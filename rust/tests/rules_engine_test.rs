//! Market Rules Engine Tests
//!
//! Tests for market rules including:
//! - Price limits
//! - Order size limits
//! - Position limits
//! - Price change limits
//! - Trading halts

use matching_engine::{
    MatchingEngineL3, MarketRulesEngine, Order, Side, OrderType, TimeInForce,
    Rule, RuleAction, RuleType,
};
use rust_decimal::Decimal;
use std::collections::HashMap;

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
    
    // Order within limits - should pass
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2), // 100.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order1, Some(Decimal::new(10000, 2))).is_ok());
    
    // Order above limit - should reject
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(12000, 2), // 120.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, Some(Decimal::new(12000, 2))).is_err());
}

#[test]
fn test_order_size_limit_rule() {
    let mut engine = MarketRulesEngine::new();
    
    let rule = Rule {
        rule_id: "size_limit_1".to_string(),
        name: "Order Size Limit".to_string(),
        rule_type: RuleType::OrderSizeLimit,
        action: RuleAction::Reject,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "1000.0".to_string());
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    // Order within limit - should pass
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(500, 0),
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order1, None).is_ok());
    
    // Order exceeds limit - should reject
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(2000, 0),
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, None).is_err());
}

#[test]
fn test_position_limit_rule() {
    let mut engine = MarketRulesEngine::new();
    
    // Set current position
    engine.update_position("account_1", "AAPL", Decimal::new(500, 0));
    
    let rule = Rule {
        rule_id: "position_limit_1".to_string(),
        name: "Position Limit".to_string(),
        rule_type: RuleType::PositionLimit,
        action: RuleAction::Reject,
        parameters: {
            let mut params = HashMap::new();
            params.insert("account_id".to_string(), "account_1".to_string());
            params.insert("max_position".to_string(), "1000.0".to_string());
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    // Order within limit - should pass
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(400, 0), // New position: 500 + 400 = 900 < 1000
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order1, None).is_ok());
    
    // Order exceeds limit - should reject
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(600, 0), // New position: 500 + 600 = 1100 > 1000
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, None).is_err());
}

#[test]
fn test_price_change_limit_rule() {
    let mut engine = MarketRulesEngine::new();
    
    // Set last price
    engine.update_last_price("AAPL", Decimal::new(10000, 2));
    
    let rule = Rule {
        rule_id: "price_change_1".to_string(),
        name: "Price Change Limit".to_string(),
        rule_type: RuleType::PriceChangeLimit,
        action: RuleAction::Reject,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_change_pct".to_string(), "0.1".to_string()); // 10%
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    // Small price change - should pass
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10500, 2), // 5% change
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order1, Some(Decimal::new(10500, 2))).is_ok());
    
    // Large price change - should reject
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(12000, 2), // 20% change
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, Some(Decimal::new(12000, 2))).is_err());
}

#[test]
fn test_trading_halt_rule() {
    let mut engine = MarketRulesEngine::new();
    
    // TradingHalt rule type checks if symbol is already halted
    // To test halting, we use a rule with Halt action
    let rule = Rule {
        rule_id: "halt_1".to_string(),
        name: "Trading Halt".to_string(),
        rule_type: RuleType::OrderSizeLimit, // Any rule type
        action: RuleAction::Halt, // This will halt trading
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "50.0".to_string()); // Small limit
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0), // Exceeds limit, triggers Halt action
        TimeInForce::GTC,
        1000,
    );
    
    // First order triggers halt - should reject and halt
    assert!(engine.check_order(&order, None).is_err());
    
    // Subsequent orders should be rejected because symbol is halted
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(10, 0), // Small order, but symbol is halted
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, None).is_err());
    
    // Resume trading
    engine.resume_trading("AAPL");
    
    // Now should pass
    assert!(engine.check_order(&order2, None).is_ok());
}

#[test]
fn test_symbol_specific_rule() {
    let mut engine = MarketRulesEngine::new();
    
    // Add symbol-specific rule
    let rule = Rule {
        rule_id: "symbol_limit_1".to_string(),
        name: "AAPL Size Limit".to_string(),
        rule_type: RuleType::OrderSizeLimit,
        action: RuleAction::Reject,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "500.0".to_string());
            params
        },
        enabled: true,
    };
    
    engine.add_symbol_rule("AAPL".to_string(), rule);
    
    // Order for AAPL - should be limited
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(600, 0), // Exceeds limit
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order1, None).is_err());
    
    // Order for TSLA - should not be limited
    let order2 = Order::new(
        "client_2".to_string(),
        "TSLA".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(600, 0),
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&order2, None).is_ok());
}

#[test]
fn test_rule_action_warn() {
    let mut engine = MarketRulesEngine::new();
    
    let rule = Rule {
        rule_id: "warn_1".to_string(),
        name: "Warning Rule".to_string(),
        rule_type: RuleType::OrderSizeLimit,
        action: RuleAction::Warn,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "1000.0".to_string());
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    // Order exceeds limit but action is Warn - should pass with warning
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(2000, 0),
        TimeInForce::GTC,
        1000,
    );
    // Warn action allows the order but logs warning
    assert!(engine.check_order(&order, None).is_ok());
}

#[test]
fn test_disabled_rule() {
    let mut engine = MarketRulesEngine::new();
    
    let rule = Rule {
        rule_id: "disabled_1".to_string(),
        name: "Disabled Rule".to_string(),
        rule_type: RuleType::OrderSizeLimit,
        action: RuleAction::Reject,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "1000.0".to_string());
            params
        },
        enabled: false, // Disabled
    };
    
    engine.add_global_rule(rule);
    
    // Order exceeds limit but rule is disabled - should pass
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(2000, 0),
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.check_order(&order, None).is_ok());
}

#[test]
fn test_rules_engine_integration() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add price limit rule
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
    
    engine.add_rule(None, rule);
    
    // Set current price
    engine.update_price("AAPL", Decimal::new(10000, 2));
    
    // Order within limits - should pass
    let order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    assert!(engine.submit_order(order1).is_ok());
    
    // Order outside limits - should reject
    let order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(12000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.submit_order(order2).is_err());
}

#[test]
fn test_resume_trading() {
    let mut engine = MarketRulesEngine::new();
    
    // Halt trading using a rule with Halt action
    let rule = Rule {
        rule_id: "halt_1".to_string(),
        name: "Halt Rule".to_string(),
        rule_type: RuleType::OrderSizeLimit,
        action: RuleAction::Halt,
        parameters: {
            let mut params = HashMap::new();
            params.insert("max_size".to_string(), "50.0".to_string());
            params
        },
        enabled: true,
    };
    
    engine.add_global_rule(rule);
    
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0), // Exceeds limit, triggers halt
        TimeInForce::GTC,
        1000,
    );
    
    // Should halt
    assert!(engine.check_order(&order, None).is_err());
    
    // Resume trading
    engine.resume_trading("AAPL");
    
    // Should pass after resume (even with same order, but we need small order to pass rule)
    let small_order = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(10, 0), // Small enough to pass rule
        TimeInForce::GTC,
        2000,
    );
    assert!(engine.check_order(&small_order, None).is_ok());
}
