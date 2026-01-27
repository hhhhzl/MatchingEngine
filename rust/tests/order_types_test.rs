//! Advanced Order Types Tests
//!
//! Tests for advanced order types:
//! - PostOnlyOrder
//! - HiddenOrder
//! - StopOrder
//! - StopLimitOrder

use matching_engine::{
    MatchingEngineL3, Order, Side, OrderType, TimeInForce,
    PostOnlyOrder, HiddenOrder, StopOrder, StopLimitOrder,
};
use rust_decimal::Decimal;

#[test]
fn test_post_only_order_accept() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add sell order at 100.00
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Post-only buy at 99.00 (won't match)
    let buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(9900, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    let post_only = PostOnlyOrder::new(buy_order);
    let trades = engine.submit_post_only(post_only).unwrap();
    assert_eq!(trades.len(), 0);
}

#[test]
fn test_post_only_order_reject() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add sell order at 100.00
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Post-only buy at 100.00 (would match)
    let buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    let post_only = PostOnlyOrder::new(buy_order);
    let result = engine.submit_post_only(post_only);
    assert!(result.is_err());
}

#[test]
fn test_hidden_order_visible_qty() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(1000, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let hidden = HiddenOrder::new(order.clone(), Some(Decimal::new(100, 0)));
    assert_eq!(hidden.visible_qty(), Decimal::new(100, 0));
    assert_eq!(hidden.hidden_qty(), Decimal::new(900, 0));
}

#[test]
fn test_hidden_order_fully_hidden() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(1000, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let hidden = HiddenOrder::new(order.clone(), None);
    assert_eq!(hidden.visible_qty(), Decimal::ZERO);
    assert_eq!(hidden.hidden_qty(), Decimal::new(1000, 0));
}

#[test]
fn test_stop_order_buy_trigger() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Market,
        Decimal::ZERO,
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop = StopOrder::new(order, Decimal::new(10500, 2)); // Stop at 105.00
    
    // Price below stop - not triggered
    assert!(!stop.check_trigger(Decimal::new(10400, 2)));
    assert!(!stop.triggered);
    
    // Price at stop - triggered
    assert!(stop.check_trigger(Decimal::new(10500, 2)));
    assert!(stop.triggered);
    assert_eq!(stop.order.order_type, OrderType::Market);
}

#[test]
fn test_stop_order_sell_trigger() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Market,
        Decimal::ZERO,
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop = StopOrder::new(order, Decimal::new(9500, 2)); // Stop at 95.00
    
    // Price above stop - not triggered
    assert!(!stop.check_trigger(Decimal::new(9600, 2)));
    assert!(!stop.triggered);
    
    // Price at stop - triggered
    assert!(stop.check_trigger(Decimal::new(9500, 2)));
    assert!(stop.triggered);
    assert_eq!(stop.order.order_type, OrderType::Market);
}

#[test]
fn test_stop_limit_order_buy_trigger() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop_limit = StopLimitOrder::new(
        order,
        Decimal::new(10500, 2), // Stop at 105.00
        Decimal::new(10400, 2), // Limit at 104.00
    );
    
    // Price below stop - not triggered
    assert!(!stop_limit.check_trigger(Decimal::new(10400, 2)));
    assert!(!stop_limit.triggered);
    
    // Price at stop - triggered
    assert!(stop_limit.check_trigger(Decimal::new(10500, 2)));
    assert!(stop_limit.triggered);
    assert_eq!(stop_limit.order.order_type, OrderType::Limit);
    assert_eq!(stop_limit.order.price, Decimal::new(10400, 2)); // Limit price
}

#[test]
fn test_stop_limit_order_sell_trigger() {
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop_limit = StopLimitOrder::new(
        order,
        Decimal::new(9500, 2), // Stop at 95.00
        Decimal::new(9600, 2), // Limit at 96.00
    );
    
    // Price above stop - not triggered
    assert!(!stop_limit.check_trigger(Decimal::new(9600, 2)));
    assert!(!stop_limit.triggered);
    
    // Price at stop - triggered
    assert!(stop_limit.check_trigger(Decimal::new(9500, 2)));
    assert!(stop_limit.triggered);
    assert_eq!(stop_limit.order.order_type, OrderType::Limit);
    assert_eq!(stop_limit.order.price, Decimal::new(9600, 2)); // Limit price
}

#[test]
fn test_stop_order_integration() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Set initial price
    engine.update_price("AAPL", Decimal::new(10000, 2));
    
    // Create stop order (buy stop at 105.00)
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Market,
        Decimal::ZERO,
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop = StopOrder::new(order, Decimal::new(10500, 2));
    
    // Price below stop - not triggered
    engine.update_price("AAPL", Decimal::new(10400, 2));
    let trades = engine.submit_stop(stop.clone()).unwrap();
    assert_eq!(trades.len(), 0);
    
    // Price at stop - triggered and matched
    engine.update_price("AAPL", Decimal::new(10500, 2));
    
    // Add matching sell order
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10500, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Now submit stop (should trigger and match)
    stop.check_trigger(Decimal::new(10500, 2));
    let trades = engine.submit_stop(stop).unwrap();
    assert_eq!(trades.len(), 1);
}

#[test]
fn test_stop_limit_order_integration() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Set initial price
    engine.update_price("AAPL", Decimal::new(10000, 2));
    
    // Create stop-limit order (buy stop at 105.00, limit at 104.00)
    let order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let mut stop_limit = StopLimitOrder::new(
        order,
        Decimal::new(10500, 2),
        Decimal::new(10400, 2),
    );
    
    // Price below stop - not triggered
    engine.update_price("AAPL", Decimal::new(10400, 2));
    let trades = engine.submit_stop_limit(stop_limit.clone()).unwrap();
    assert_eq!(trades.len(), 0);
    
    // Price at stop - triggered
    engine.update_price("AAPL", Decimal::new(10500, 2));
    stop_limit.check_trigger(Decimal::new(10500, 2));
    
    // Add matching sell order at limit price
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10400, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Submit stop-limit (should trigger and match)
    let trades = engine.submit_stop_limit(stop_limit).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].price, Decimal::new(10400, 2));
}

#[test]
fn test_hidden_order_matching() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add visible sell order
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Submit hidden buy order (should match)
    let buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    let hidden = HiddenOrder::new(buy_order, Some(Decimal::new(50, 0)));
    let trades = engine.submit_hidden(hidden).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].qty, Decimal::new(100, 0)); // Full match
}
