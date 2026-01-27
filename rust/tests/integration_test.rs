//! Integration tests for the matching engine

use matching_engine::{MatchingEngine, Order, Side, OrderType, TimeInForce, OrderStatus};
use rust_decimal::Decimal;

#[test]
fn test_basic_matching() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    // Add buy order
    let buy_order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2), // 100.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    // Add sell order
    let sell_order = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(9900, 2), // 99.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    // Submit buy order (should not match yet)
    let trades1 = engine.submit_order(buy_order).unwrap();
    assert_eq!(trades1.len(), 0);
    
    // Submit sell order (should match)
    let trades2 = engine.submit_order(sell_order).unwrap();
    assert_eq!(trades2.len(), 1);
    assert_eq!(trades2[0].qty, Decimal::new(100, 0));
    assert_eq!(trades2[0].price, Decimal::new(10000, 2)); // Use buy price (better)
}

#[test]
fn test_market_order() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    // Add limit sell order
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2), // 100.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Submit market buy order
    let market_buy = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Market,
        Decimal::ZERO, // Market orders don't need price
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    let trades = engine.submit_order(market_buy).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].price, Decimal::new(10000, 2)); // Use limit price
}

#[test]
fn test_ioc_order() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    // Submit IOC buy order with no matching orders
    let ioc_order = Order::new(
        "client_ioc".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::IOC,
        1000,
    );
    
    let trades = engine.submit_order(ioc_order).unwrap();
    assert_eq!(trades.len(), 0);
    
    // Order should be cancelled (not in book)
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    assert_eq!(orderbook.get_best_bid(), None);
}

#[test]
fn test_fok_order() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    // Add sell order for 50 shares
    let sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(50, 0),
        TimeInForce::GTC,
        1000,
    );
    engine.submit_order(sell_order).unwrap();
    
    // Submit FOK buy order for 100 shares (cannot fully fill)
    let fok_order = Order::new(
        "client_fok".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10100, 2),
        Decimal::new(100, 0),
        TimeInForce::FOK,
        2000,
    );
    
    let trades = engine.submit_order(fok_order).unwrap();
    assert_eq!(trades.len(), 0); // FOK requires complete fill, so no trades
    
    // Order should be cancelled
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    assert_eq!(orderbook.get_best_bid(), None);
}

#[test]
fn test_cancel_order() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
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
    
    let trades = engine.submit_order(order).unwrap();
    assert_eq!(trades.len(), 0);
    
    // Get order ID
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    let order_id = orderbook.get_best_bid().unwrap().order_id.clone();
    
    // Cancel order
    let canceled = engine.cancel_order("AAPL", &order_id).unwrap();
    assert_eq!(canceled.status, OrderStatus::Canceled);
    
    // Order should be removed from book
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    assert_eq!(orderbook.get_best_bid(), None);
}
