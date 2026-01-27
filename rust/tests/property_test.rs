//! Property-Based Tests
//!
//! Tests that verify invariants hold across many random inputs:
//! - Price-time priority invariants
//! - Order quantity conservation
//! - Trade price validity
//! - Queue position consistency

use matching_engine::{
    OrderBookL3, Order, Side, OrderType, TimeInForce, OrderStatus,
};
use rust_decimal::Decimal;
use proptest::prelude::*;

// Helper to create valid order
fn valid_order(
    client_id: String,
    symbol: String,
    side: Side,
    price: i64,
    qty: i64,
    timestamp: i64,
) -> Order {
    Order::new(
        client_id,
        symbol,
        side,
        OrderType::Limit,
        Decimal::new(price.max(1), 2), // Ensure positive
        Decimal::new(qty.max(1), 0), // Ensure positive
        TimeInForce::GTC,
        timestamp.max(0),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    
    #[test]
    fn test_price_time_priority_invariant(
        prices in 1000i64..100000i64,
        timestamps in 0i64..1000000i64,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        
        // Create two orders at same price, different times
        let mut order1 = valid_order(
            "client_1".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            prices,
            100,
            timestamps,
        );
        order1.order_id = "order_1".to_string();
        
        let mut order2 = valid_order(
            "client_2".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            prices,
            100,
            timestamps + 1000, // Later
        );
        order2.order_id = "order_2".to_string();
        
        let pos1 = book.add_order(order1).unwrap();
        let pos2 = book.add_order(order2).unwrap();
        
        // Earlier order should have better (lower index) position
        prop_assert!(pos1 < pos2);
    }
    
    #[test]
    fn test_quantity_conservation(
        buy_qty in 1i64..1000i64,
        sell_qty in 1i64..1000i64,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        
        // Add sell order
        let mut sell_order = valid_order(
            "client_sell".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            10000,
            sell_qty,
            1000,
        );
        sell_order.order_id = "sell_1".to_string();
        book.add_order(sell_order).unwrap();
        
        // Match with buy order
        let mut buy_order = valid_order(
            "client_buy".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            buy_qty,
            2000,
        );
        buy_order.order_id = "buy_1".to_string();
        
        let trades = book.match_order(buy_order);
        
        // Total traded quantity should not exceed either order
        let total_traded: Decimal = trades.iter().map(|t| t.qty).sum();
        prop_assert!(total_traded <= Decimal::new(buy_qty, 0));
        prop_assert!(total_traded <= Decimal::new(sell_qty, 0));
        
        // If both orders exist after matching, verify quantities
        if let Some(buy) = book.get_order("buy_1") {
            prop_assert!(buy.leaves_qty >= Decimal::ZERO);
            prop_assert!(buy.cum_qty + buy.leaves_qty == buy.qty);
        }
    }
    
    #[test]
    fn test_trade_price_validity(
        buy_price in 1000i64..20000i64,
        sell_price in 1000i64..20000i64,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        
        // Add sell order
        let mut sell_order = valid_order(
            "client_sell".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            sell_price,
            100,
            1000,
        );
        sell_order.order_id = "sell_1".to_string();
        book.add_order(sell_order).unwrap();
        
        // Match with buy order
        let mut buy_order = valid_order(
            "client_buy".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            buy_price,
            100,
            2000,
        );
        buy_order.order_id = "buy_1".to_string();
        
        let trades = book.match_order(buy_order);
        
        // If trades occurred, price should be between buy and sell prices
        for trade in trades {
            let min_price = Decimal::new(buy_price.min(sell_price), 2);
            let max_price = Decimal::new(buy_price.max(sell_price), 2);
            
            // Trade price should be between buy and sell (or equal to one)
            prop_assert!(
                trade.price >= min_price &&
                trade.price <= max_price
            );
        }
    }
    
    #[test]
    fn test_queue_position_consistency(
        num_orders in 1usize..20usize,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        let mut order_ids = Vec::new();
        
        // Add orders
        for i in 0..num_orders {
            let mut order = valid_order(
                format!("client_{}", i),
                "AAPL".to_string(),
                Side::Buy,
                10000,
                100,
                (i * 1000) as i64,
            );
            order.order_id = format!("order_{}", i);
            order_ids.push(order.order_id.clone());
            book.add_order(order).unwrap();
        }
        
        // Verify all orders have valid queue positions
        for (i, order_id) in order_ids.iter().enumerate() {
            if let Some(pos) = book.get_queue_position(order_id) {
                prop_assert_eq!(pos, i);
            }
        }
    }
    
    #[test]
    fn test_no_negative_quantities(
        initial_qty in 1i64..1000i64,
        match_qty in 1i64..1000i64,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        
        // Add sell order
        let mut sell_order = valid_order(
            "client_sell".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            10000,
            initial_qty,
            1000,
        );
        sell_order.order_id = "sell_1".to_string();
        book.add_order(sell_order).unwrap();
        
        // Match
        let mut buy_order = valid_order(
            "client_buy".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            match_qty,
            2000,
        );
        buy_order.order_id = "buy_1".to_string();
        
        let trades = book.match_order(buy_order);
        
        // Verify no negative quantities
        for trade in &trades {
            prop_assert!(trade.qty > Decimal::ZERO);
            prop_assert!(trade.price > Decimal::ZERO);
        }
        
        // Verify remaining quantities are non-negative
        if let Some(order) = book.get_order("sell_1") {
            prop_assert!(order.leaves_qty >= Decimal::ZERO);
            prop_assert!(order.cum_qty >= Decimal::ZERO);
        }
        
        if let Some(order) = book.get_order("buy_1") {
            prop_assert!(order.leaves_qty >= Decimal::ZERO);
            prop_assert!(order.cum_qty >= Decimal::ZERO);
        }
    }
    
    #[test]
    fn test_order_status_transitions(
        qty in 1i64..1000i64,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        
        // Add order
        let mut order = valid_order(
            "client_1".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            qty,
            1000,
        );
        order.order_id = "order_1".to_string();
        book.add_order(order).unwrap();
        
        let order = book.get_order("order_1").unwrap();
        prop_assert_eq!(order.status, OrderStatus::New);
        
        // Add matching sell order
        let mut sell_order = valid_order(
            "client_sell".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            10000,
            qty / 2, // Partial fill
            2000,
        );
        sell_order.order_id = "sell_1".to_string();
        book.add_order(sell_order).unwrap();
        
        // Match
        let mut buy_order = valid_order(
            "client_1".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            qty,
            3000,
        );
        buy_order.order_id = "order_2".to_string();
        book.match_order(buy_order);
        
        // Verify status
        if let Some(order) = book.get_order("order_2") {
            if order.leaves_qty > Decimal::ZERO {
                prop_assert!(
                    order.status == OrderStatus::Partial ||
                    order.status == OrderStatus::New
                );
            }
        }
    }
}

#[test]
fn test_deterministic_matching() {
    // Test that same input produces same output
    let mut book1 = OrderBookL3::new("AAPL".to_string());
    let mut book2 = OrderBookL3::new("AAPL".to_string());
    
    // Add same orders to both books
    for i in 0..5 {
        let mut order1 = valid_order(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            100,
            (i * 1000) as i64,
        );
        order1.order_id = format!("order_{}", i);
        
        let mut order2 = order1.clone();
        order2.order_id = format!("order_{}", i);
        
        book1.add_order(order1).unwrap();
        book2.add_order(order2).unwrap();
    }
    
    // Match with same order
    let mut buy_order1 = valid_order(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        10000,
        100,
        6000,
    );
    buy_order1.order_id = "buy_1".to_string();
    
    let mut buy_order2 = buy_order1.clone();
    buy_order2.order_id = "buy_1".to_string();
    
    let trades1 = book1.match_order(buy_order1);
    let trades2 = book2.match_order(buy_order2);
    
    // Should produce same number of trades
    assert_eq!(trades1.len(), trades2.len());
}

#[test]
fn test_price_improvement() {
    // Test that matching always uses better price
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add sell order at 100.00
    let mut sell_order = valid_order(
        "client_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        10000,
        100,
        1000,
    );
    sell_order.order_id = "sell_1".to_string();
    book.add_order(sell_order).unwrap();
    
    // Match with buy order at 101.00 (better price)
    let mut buy_order = valid_order(
        "client_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        10100,
        100,
        2000,
    );
    buy_order.order_id = "buy_1".to_string();
    
    let trades = book.match_order(buy_order);
    assert_eq!(trades.len(), 1);
    // Should use sell price (100.00) - the better price for the seller
    assert_eq!(trades[0].price, Decimal::new(10000, 2));
}

#[test]
fn test_time_priority_same_price() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    
    // Add orders at same price, different times
    let timestamps = [5000, 3000, 4000, 1000, 2000];
    for (i, &ts) in timestamps.iter().enumerate() {
        let mut order = valid_order(
            format!("client_{}", i),
            "AAPL".to_string(),
            Side::Buy,
            10000,
            100,
            ts,
        );
        order.order_id = format!("order_{}", i);
        book.add_order(order).unwrap();
    }
    
    // Best bid should be earliest (1000)
    let best_bid = book.get_best_bid().unwrap();
    assert_eq!(best_bid.timestamp_ns, 1000);
    
    // Verify queue positions reflect time priority
    assert_eq!(book.get_queue_position("order_3"), Some(0)); // ts=1000
    assert_eq!(book.get_queue_position("order_4"), Some(1)); // ts=2000
    assert_eq!(book.get_queue_position("order_1"), Some(2)); // ts=3000
    assert_eq!(book.get_queue_position("order_2"), Some(3)); // ts=4000
    assert_eq!(book.get_queue_position("order_0"), Some(4)); // ts=5000
}
