//! L3 Order Book and Matching Engine Tests
//!
//! Tests for L3 functionality including:
//! - Queue position tracking
//! - Price-time priority ordering
//! - Hidden orders
//! - Post-only orders
//! - MatchingEngineL3 integration

use matching_engine::{
    MatchingEngineL3, OrderBookL3, Order, Side, OrderType, TimeInForce, OrderStatus,
    HiddenOrder, PostOnlyOrder,
};
use rust_decimal::Decimal;

#[test]
fn test_l3_queue_position_tracking() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Create orders with different prices and timestamps
    let mut order1 = Order::new_with_account(
        "client_1".to_string(),
        "acct".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2), // 100.00
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000, // Earlier timestamp
    );
    order1.order_id = "order_1".to_string();
    
    let mut order2 = Order::new_with_account(
        "client_2".to_string(),
        "acct".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10100, 2), // 101.00 - higher price
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000, // Later timestamp
    );
    order2.order_id = "order_2".to_string();
    
    let mut order3 = Order::new_with_account(
        "client_3".to_string(),
        "acct".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2), // 100.00 - same price as order1
        Decimal::new(100, 0),
        TimeInForce::GTC,
        3000, // Later timestamp
    );
    order3.order_id = "order_3".to_string();
    
    // Add orders
    let pos1 = book.add_order(order1).unwrap();
    let pos2 = book.add_order(order2).unwrap();
    let pos3 = book.add_order(order3).unwrap();
    
    // order2 (higher price) should be at position 0
    assert_eq!(pos2, 0);
    // order1 is added first, so its insertion position is 0 at the time of insertion.
    // After adding order2 (higher price), order1's *current* queue position becomes 1.
    assert_eq!(pos1, 0);
    // order3 (same price, later time) should be at position 2
    assert_eq!(pos3, 2);
    
    // Verify queue positions
    assert_eq!(book.get_queue_position("order_2"), Some(0));
    assert_eq!(book.get_queue_position("order_1"), Some(1));
    assert_eq!(book.get_queue_position("order_3"), Some(2));
}

#[test]
fn test_l3_price_time_priority() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add multiple buy orders at same price, different times
    for i in 0..5 {
        let mut order = Order::new_with_account(
            format!("client_{}", i),
            "acct".to_string(),
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
    
    // Verify they're in time order (earliest first)
    for i in 0..5 {
        assert_eq!(book.get_queue_position(&format!("order_{}", i)), Some(i));
    }
    
    // Best bid should be the first one (earliest at same price)
    let best_bid = book.get_best_bid().unwrap();
    assert_eq!(best_bid.order_id, "order_0");
}

#[test]
fn test_l3_hidden_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    let order = Order::new_with_account(
        "client_1".to_string(),
        "acct".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(1000, 0), // Large quantity
        TimeInForce::GTC,
        1000,
    );
    
    // Create hidden order with display quantity
    let hidden = HiddenOrder::new(order, Some(Decimal::new(100, 0)));
    let _pos = book.add_hidden_order(hidden).unwrap();
    
    // Get visible orderbook
    let (bids, _asks) = book.get_visible_orderbook(10);
    
    // Should only show display_qty (100), not full qty (1000)
    assert_eq!(bids.len(), 1);
    assert_eq!(bids[0].0, Decimal::new(10000, 2)); // Price
    assert_eq!(bids[0].1, Decimal::new(100, 0)); // Visible quantity
}

#[test]
fn test_l3_fully_hidden_order() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
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
    
    // Fully hidden order (no display_qty)
    let hidden = HiddenOrder::new(order, None);
    book.add_hidden_order(hidden).unwrap();
    
    // Get visible orderbook - should not show hidden order
    let (bids, _asks) = book.get_visible_orderbook(10);
    assert_eq!(bids.len(), 0);
}

#[test]
fn test_l3_post_only_order_success() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add a sell order at 100.00
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
    
    // Post-only buy order at 99.00 (won't match)
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
    let result = book.add_post_only_order(post_only);
    assert!(result.is_ok());
}

#[test]
fn test_l3_post_only_order_reject() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add a sell order at 100.00
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
    
    // Post-only buy order at 100.00 (would match immediately)
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
    let result = book.add_post_only_order(post_only);
    assert!(result.is_err());
}

#[test]
fn test_l3_cancel_updates_queue_positions() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add 3 orders
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
    
    // Cancel middle order (order_1)
    let canceled = book.cancel_order("order_1").unwrap();
    assert_eq!(canceled.status, OrderStatus::Canceled);
    
    // Verify queue positions updated
    assert_eq!(book.get_queue_position("order_0"), Some(0));
    assert_eq!(book.get_queue_position("order_1"), None); // Removed
    assert_eq!(book.get_queue_position("order_2"), Some(1)); // Moved up
}

#[test]
fn test_l3_matching_engine_basic() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    let buy_order = Order::new(
        "client_1".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        1000,
    );
    
    let sell_order = Order::new(
        "client_2".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(9900, 2),
        Decimal::new(100, 0),
        TimeInForce::GTC,
        2000,
    );
    
    // Submit buy order (should not match)
    let trades1 = engine.submit_order(buy_order).unwrap();
    assert_eq!(trades1.len(), 0);
    
    // Submit sell order (should match)
    let trades2 = engine.submit_order(sell_order).unwrap();
    assert_eq!(trades2.len(), 1);
    assert_eq!(trades2[0].qty, Decimal::new(100, 0));
    assert_eq!(trades2[0].price, Decimal::new(10000, 2)); // Use buy price (better)
}

#[test]
fn test_l3_matching_engine_queue_position() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add multiple buy orders
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
        engine.submit_order(order).unwrap();
    }
    
    // Get order IDs from orderbook
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    let order_id_0 = orderbook.get_best_bid().unwrap().order_id.clone();
    
    // Verify queue positions
    assert_eq!(engine.get_queue_position("AAPL", &order_id_0), Some(0));
}

#[test]
fn test_l3_matching_engine_hidden_order() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
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
    
    let hidden = HiddenOrder::new(order, Some(Decimal::new(50, 0)));
    let trades = engine.submit_hidden(hidden).unwrap();
    assert_eq!(trades.len(), 0); // No matching orders
    
    // Verify order is in book but hidden
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    let (bids, _) = orderbook.get_visible_orderbook(10);
    assert_eq!(bids.len(), 1);
    assert_eq!(bids[0].1, Decimal::new(50, 0)); // Display quantity
}

#[test]
fn test_l3_matching_engine_post_only() {
    let mut engine = MatchingEngineL3::new(vec!["AAPL".to_string()]);
    
    // Add sell order
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
    
    // Post-only buy order that won't match
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
    assert_eq!(trades.len(), 0); // No immediate match
}

#[test]
fn test_l3_partial_fill_queue_position() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add sell order for 50 shares
    let mut sell_order = Order::new(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(50, 0),
        TimeInForce::GTC,
        1000,
    );
    sell_order.order_id = "sell_1".to_string();
    book.add_order(sell_order).unwrap();
    
    // Match with buy order for 100 shares (partial fill)
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
    
    let trades = book.match_order(buy_order);
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].qty, Decimal::new(50, 0));
    
    // Remaining buy order should be in book
    let remaining = book.get_order("buy_1").unwrap();
    assert_eq!(remaining.leaves_qty, Decimal::new(50, 0));
    assert_eq!(remaining.status, OrderStatus::Partial);
}

#[test]
fn test_l3_multiple_price_levels() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add orders at different price levels
    let prices = [10100, 10000, 9900, 9800];
    for (i, &price) in prices.iter().enumerate() {
        let mut order = Order::new(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(price, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            (i * 1000) as i64,
        );
        order.order_id = format!("order_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Verify best bid is highest price
    let best_bid = book.get_best_bid().unwrap();
    assert_eq!(best_bid.price, Decimal::new(10100, 2));
    assert_eq!(best_bid.order_id, "order_0");
}

#[test]
fn test_l3_visible_orderbook_aggregation() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add multiple orders at same price
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
    
    // Get visible orderbook - should aggregate quantities
    let (bids, _asks) = book.get_visible_orderbook(10);
    assert_eq!(bids.len(), 1);
    assert_eq!(bids[0].0, Decimal::new(10000, 2)); // Price
    assert_eq!(bids[0].1, Decimal::new(300, 0)); // Total quantity
}
