//! Regressions for L3 bugs fixed in M0:
//! - FOK that could not fully fill still decremented resting orders.
//! - A market order's unfilled remainder rested on the book at price 0.

use matching_engine::{Order, OrderBookL3, OrderType, Side, TimeInForce};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn order(id: &str, side: Side, ot: OrderType, price: i64, qty: i64, tif: TimeInForce, ts: i64) -> Order {
    let mut o = Order::new_with_account(
        format!("c_{id}"),
        "acct".to_string(),
        "AAPL".to_string(),
        side,
        ot,
        Decimal::new(price, 2),
        Decimal::new(qty, 0),
        tif,
        ts,
    );
    o.order_id = id.to_string();
    o
}

/// (order_id, price, leaves_qty) for both sides, front of queue first.
fn book_state(book: &OrderBookL3) -> Vec<(String, Decimal, Decimal)> {
    let (bids, asks) = book.get_l3_snapshot(usize::MAX);
    bids.into_iter()
        .chain(asks)
        .map(|v| (v.order_id, v.price, v.leaves_qty))
        .collect()
}

#[test]
fn fok_that_cannot_fill_leaves_book_untouched() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    book.add_order(order("a1", Side::Sell, OrderType::Limit, 10000, 30, TimeInForce::GTC, 1)).unwrap();
    book.add_order(order("a2", Side::Sell, OrderType::Limit, 10100, 30, TimeInForce::GTC, 2)).unwrap();
    let before = book_state(&book);

    // 50 wanted at <= 100.00, only 30 available there.
    let trades = book.match_order(order("b", Side::Buy, OrderType::Limit, 10000, 50, TimeInForce::FOK, 3));

    assert!(trades.is_empty());
    assert_eq!(book_state(&book), before);
}

#[test]
fn fok_that_can_fill_trades_fully() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    book.add_order(order("a1", Side::Sell, OrderType::Limit, 10000, 30, TimeInForce::GTC, 1)).unwrap();
    book.add_order(order("a2", Side::Sell, OrderType::Limit, 10100, 30, TimeInForce::GTC, 2)).unwrap();

    let trades = book.match_order(order("b", Side::Buy, OrderType::Limit, 10100, 50, TimeInForce::FOK, 3));

    let filled: Decimal = trades.iter().map(|t| t.qty).sum();
    assert_eq!(filled, Decimal::new(50, 0));
    assert_eq!(book_state(&book), vec![("a2".to_string(), Decimal::new(10100, 2), Decimal::new(10, 0))]);
}

#[test]
fn market_order_on_empty_book_does_not_rest() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    let trades = book.match_order(order("m", Side::Buy, OrderType::Market, 0, 10, TimeInForce::GTC, 1));
    assert!(trades.is_empty());
    assert!(book_state(&book).is_empty());
}

#[test]
fn market_order_remainder_is_canceled() {
    let mut book = OrderBookL3::new("AAPL".to_string());
    book.add_order(order("b1", Side::Buy, OrderType::Limit, 9900, 4, TimeInForce::GTC, 1)).unwrap();

    let trades = book.match_order(order("m", Side::Sell, OrderType::Market, 0, 10, TimeInForce::Day, 2));

    assert_eq!(trades.iter().map(|t| t.qty).sum::<Decimal>(), Decimal::new(4, 0));
    assert!(book_state(&book).is_empty(), "remainder must not rest");
}

proptest! {
    /// FOK is all-or-nothing: either exactly the order qty trades, or the
    /// book is exactly as it was.
    #[test]
    fn fok_all_or_nothing(
        asks in prop::collection::vec((9_990i64..10_010, 1i64..50), 0..12),
        limit in 9_985i64..10_015,
        qty in 1i64..200,
    ) {
        let mut book = OrderBookL3::new("AAPL".to_string());
        for (i, (p, q)) in asks.iter().enumerate() {
            book.add_order(order(&format!("a{i}"), Side::Sell, OrderType::Limit, *p, *q, TimeInForce::GTC, i as i64)).unwrap();
        }
        let before = book_state(&book);
        let trades = book.match_order(order("b", Side::Buy, OrderType::Limit, limit, qty, TimeInForce::FOK, 1_000));
        let filled: Decimal = trades.iter().map(|t| t.qty).sum();
        if trades.is_empty() {
            prop_assert_eq!(book_state(&book), before);
        } else {
            prop_assert_eq!(filled, Decimal::new(qty, 0));
        }
    }
}
