//! Edge Cases and Error Handling Tests
//!
//! Tests for edge cases and error conditions:
//! - Invalid orders
//! - Duplicate orders
//! - Zero quantities
//! - Negative prices
//! - Order state transitions
//! - Cancellation edge cases

use matching_engine::{
    MatchingEngine, OrderBookL3, Order, Side, OrderType, TimeInForce, OrderStatus,
    MatchingError,
};
use rust_decimal::Decimal;

#[test]
fn test_zero_quantity_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let mut order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::ZERO, // Zero quantity
        TimeInForce::GTC,
        1000,
    );
    order.order_id = "order_1".to_string();
    
    let result = book.add_order(order);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), MatchingError::InvalidQuantity);
}

#[test]
fn test_negative_price_limit_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let mut order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(-1000, 2), // Negative price
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    order.order_id = "order_1".to_string();
    
    let result = book.add_order(order);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), MatchingError::InvalidPrice);
}

#[test]
fn test_duplicate_order_id() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let mut order1 = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    order1.order_id = "order_1".to_string();
    
    let mut order2 = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    order2.order_id = "order_1".to_string(); // Same ID
    
    book.add_order(order1).unwrap();
    let result = book.add_order(order2);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MatchingError::OrderExists(_)));
}

#[test]
fn test_cancel_nonexistent_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let result = book.cancel_order("nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MatchingError::OrderNotFound(_)));
}

#[test]
fn test_cancel_already_filled_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add sell order
    let mut sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    sell_order.order_id = "sell_1".to_string();
    book.add_order(sell_order).unwrap();
    
    // Match with buy order (sells all)
    let mut buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    buy_order.order_id = "buy_1".to_string();
    book.match_order(buy_order);
    
    // Try to cancel filled order
    let result = book.cancel_order("sell_1");
    assert!(result.is_err()); // Order already removed from book
}

#[test]
fn test_market_order_with_empty_book() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    let market_order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Market,
        Decimal::ZERO,
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let trades = engine.submit_order(market_order).unwrap();
    assert_eq!(trades.len(), 0); // No match, order should be rejected or not added
}

#[test]
fn test_ioc_order_no_match() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    let ioc_order = Order::new(
        "client_1".to_string(),
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
    
    // IOC order should not be in book
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    assert_eq!(orderbook.get_best_bid(), None);
}

#[test]
fn test_fok_order_partial_fill() {
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
    
    // FOK buy order for 100 shares (cannot fully fill)
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
    assert_eq!(trades.len(), 0); // FOK requires complete fill
    
    // Order should not be in book
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    assert_eq!(orderbook.get_best_bid(), None);
}

#[test]
fn test_fok_order_full_fill() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    
    // Add sell order for 100 shares
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
    
    // FOK buy order for 100 shares (can fully fill)
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
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].qty, Decimal::new(100, 0));
}

#[test]
fn test_multiple_partial_fills() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add multiple sell orders
    for i in 0..3 {
        let mut order = Order::new(
            format!("client_sell_{}", i),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(50, 0),
            TimeInForce::GTC,
            (i * 1000) as i64,
        );
        order.order_id = format!("sell_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Match with large buy order
    let mut buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(200, 0), // More than available
        TimeInForce::GTC,
        4000,
    );
    buy_order.order_id = "buy_1".to_string();
    
    let trades = book.match_order(buy_order);
    assert_eq!(trades.len(), 3); // Three partial fills
    assert_eq!(trades.iter().map(|t| t.qty).sum::<Decimal>(), Decimal::new(150, 0));
    
    // Remaining buy order should be in book
    let remaining = book.get_order("buy_1").unwrap();
    assert_eq!(remaining.leaves_qty, Decimal::new(50, 0));
    assert_eq!(remaining.status, OrderStatus::Partial);
}

#[test]
fn test_cancel_from_middle_of_queue() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add 5 orders
    for i in 0..5 {
        let mut order = Order::new(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            (i * 1000) as i64,
        );
        order.order_id = format!("order_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Cancel order in middle (order_2)
    book.cancel_order("order_2").unwrap();
    
    // Verify queue positions updated correctly
    assert_eq!(book.get_queue_position("order_0"), Some(0));
    assert_eq!(book.get_queue_position("order_1"), Some(1));
    assert_eq!(book.get_queue_position("order_2"), None); // Removed
    assert_eq!(book.get_queue_position("order_3"), Some(2)); // Moved up
    assert_eq!(book.get_queue_position("order_4"), Some(3)); // Moved up
}

#[test]
fn test_order_not_found_after_fill() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add sell order
    let mut sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    sell_order.order_id = "sell_1".to_string();
    book.add_order(sell_order).unwrap();
    
    // Match completely
    let mut buy_order = Order::new(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    buy_order.order_id = "buy_1".to_string();
    book.match_order(buy_order);
    
    // Order should be removed
    assert_eq!(book.get_order("sell_1"), None);
}

#[test]
fn test_empty_orderbook_operations() {
    let book = OrderBookL3::new("AAPL".to_string());
    
    assert_eq!(book.get_best_bid(), None);
    assert_eq!(book.get_best_ask(), None);
    
    let (bids, asks) = book.get_visible_orderbook(10);
    assert_eq!(bids.len(), 0);
    assert_eq!(asks.len(), 0);
}

#[test]
fn test_very_small_quantities() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Test with very small quantity (but > 0)
    let mut order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(1, 6), // 0.000001
        TimeInForce::GTC,
        1000,
    );
    order.order_id = "order_1".to_string();
    
    let result = book.add_order(order);
    assert!(result.is_ok()); // Should accept very small but positive quantities
}

#[test]
fn test_very_high_price() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let mut order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(100000000, 2), // Very high price
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    order.order_id = "order_1".to_string();
    
    let result = book.add_order(order);
    assert!(result.is_ok()); // Should accept high prices
}

#[test]
fn test_same_price_different_times() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add orders at same price, different times
    for i in 0..10 {
        let mut order = Order::new(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            (i * 100) as i64, // Different timestamps
        );
        order.order_id = format!("order_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Verify they're ordered by time
    for i in 0..10 {
        assert_eq!(book.get_queue_position(&format!("order_{}", i)), Some(i));
    }
}

#[test]
fn test_cancel_best_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add multiple orders
    for i in 0..3 {
        let mut order = Order::new(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            (i * 1000) as i64,
        );
        order.order_id = format!("order_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Cancel best order (order_0)
    book.cancel_order("order_0").unwrap();
    
    // Next order should become best
    let best_bid = book.get_best_bid().unwrap();
    assert_eq!(best_bid.order_id, "order_1");
}
