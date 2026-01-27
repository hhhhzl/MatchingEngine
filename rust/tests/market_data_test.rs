//! Market data (snapshot + delta) and auction tests.

use matching_engine::{
    BookDeltaReason, EngineEventKind, MatchingEngine, MatchPriceRule, MarketStatus, Order, OrderType,
    Side, TimeInForce,
};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Default)]
struct BookReplayer {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl BookReplayer {
    fn from_snapshot(snapshot: matching_engine::BookSnapshot) -> Self {
        let mut r = Self::default();
        for lvl in snapshot.bids {
            r.bids.insert(lvl.price, lvl.qty);
        }
        for lvl in snapshot.asks {
            r.asks.insert(lvl.price, lvl.qty);
        }
        r
    }

    fn apply_delta(&mut self, side: Side, price: Decimal, new_qty: Decimal) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if new_qty == Decimal::ZERO {
            book.remove(&price);
        } else {
            book.insert(price, new_qty);
        }
    }
}

#[test]
fn test_snapshot_plus_deltas_reconstructs_book() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
    engine.set_price_rule("AAPL", MatchPriceRule::Maker).unwrap();

    // Seed with some orders.
    let ev1 = engine
        .submit_order_events(Order::new(
            "b1".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(10, 0),
            TimeInForce::GTC,
            1,
        ))
        .unwrap();
    let ev2 = engine
        .submit_order_events(Order::new(
            "s1".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10100, 2),
            Decimal::new(7, 0),
            TimeInForce::GTC,
            2,
        ))
        .unwrap();

    // Take a snapshot.
    let snap_ev = engine.book_snapshot_event("AAPL", 10, 3).unwrap();
    let snapshot = match &snap_ev[0].kind {
        EngineEventKind::BookSnapshot { snapshot, .. } => snapshot.clone(),
        other => panic!("expected snapshot event, got {:?}", other),
    };
    let mut replay = BookReplayer::from_snapshot(snapshot);

    // Apply deltas from additional activity.
    let ev3 = engine
        .submit_order_events(Order::new(
            "b2".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10050, 2),
            Decimal::new(3, 0),
            TimeInForce::GTC,
            4,
        ))
        .unwrap();

    // Cancel an existing order (best bid at 100.50? Actually 100.50 is best; cancel b2).
    let orderbook = engine.get_orderbook("AAPL").unwrap();
    let oid = orderbook.get_best_bid().unwrap().order_id.clone();
    let ev4 = engine.cancel_order_events("AAPL", &oid, 5).unwrap();

    for e in ev1.into_iter().chain(ev2).chain(ev3).chain(ev4) {
        if let EngineEventKind::BookDelta {
            side,
            price,
            new_qty,
            ..
        } = e.kind
        {
            replay.apply_delta(side, price, new_qty);
        }
    }

    let expected = engine.get_book_snapshot("AAPL", 10).unwrap();
    let replayed = matching_engine::BookSnapshot {
        bids: replay
            .bids
            .iter()
            .rev()
            .map(|(p, q)| matching_engine::BookLevel { price: *p, qty: *q })
            .collect(),
        asks: replay
            .asks
            .iter()
            .map(|(p, q)| matching_engine::BookLevel { price: *p, qty: *q })
            .collect(),
    };

    assert_eq!(expected, replayed);
}

#[test]
fn test_open_auction_uncrosses_and_is_deterministic() {
    let mut e1 = MatchingEngine::new(vec!["AAPL".to_string()]);
    let mut e2 = MatchingEngine::new(vec!["AAPL".to_string()]);

    // Move both engines to PreOpen.
    let _ = e1.set_market_status_events("AAPL", MarketStatus::PreOpen, 1).unwrap();
    let _ = e2.set_market_status_events("AAPL", MarketStatus::PreOpen, 1).unwrap();

    // Add crossing interest.
    let orders = vec![
        Order::new(
            "b1".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10200, 2),
            Decimal::new(10, 0),
            TimeInForce::GTC,
            2,
        ),
        Order::new(
            "b2".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10100, 2),
            Decimal::new(5, 0),
            TimeInForce::GTC,
            3,
        ),
        Order::new(
            "s1".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10050, 2),
            Decimal::new(7, 0),
            TimeInForce::GTC,
            4,
        ),
        Order::new(
            "s2".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10150, 2),
            Decimal::new(20, 0),
            TimeInForce::GTC,
            5,
        ),
    ];

    for o in &orders {
        e1.submit_order_events(o.clone()).unwrap();
        e2.submit_order_events(o.clone()).unwrap();
    }

    // Run the auction.
    let ev1 = e1.open_auction_events("AAPL", 10, None).unwrap();
    let ev2 = e2.open_auction_events("AAPL", 10, None).unwrap();

    // Deterministic: same sequence of Fill/Done events.
    let f1: Vec<_> = ev1
        .iter()
        .filter(|e| matches!(e.kind, EngineEventKind::Fill { .. } | EngineEventKind::Done { .. }))
        .map(|e| format!("{:?}", e.kind))
        .collect();
    let f2: Vec<_> = ev2
        .iter()
        .filter(|e| matches!(e.kind, EngineEventKind::Fill { .. } | EngineEventKind::Done { .. }))
        .map(|e| format!("{:?}", e.kind))
        .collect();
    assert_eq!(f1, f2);

    // Auction should emit deltas with Auction reason.
    assert!(ev1.iter().any(|e| matches!(e.kind, EngineEventKind::BookDelta { reason: BookDeltaReason::Auction, .. })));
}

#[test]
fn test_oco_cancels_sibling_on_fill() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);

    let mut buy = Order::new(
        "oco_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(10, 0),
        TimeInForce::GTC,
        1,
    );
    buy.order_id = "OCO_BUY".to_string();

    let mut sell = Order::new(
        "oco_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(11000, 2),
        Decimal::new(10, 0),
        TimeInForce::GTC,
        2,
    );
    sell.order_id = "OCO_SELL".to_string();

    let _ = engine
        .submit_oco_events(buy, sell, "G1".to_string())
        .unwrap();

    // Fill the buy order by crossing with a sell.
    let mut taker_sell = Order::new(
        "taker".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(10, 0),
        TimeInForce::GTC,
        3,
    );
    taker_sell.order_id = "TAKER_SELL".to_string();
    let ev = engine.submit_order_events(taker_sell).unwrap();

    // Sibling should be canceled.
    assert!(
        ev.iter().any(|e| matches!(&e.kind, EngineEventKind::CancelAck { order_id, .. } if order_id == "OCO_SELL"))
    );
    let book = engine.get_orderbook("AAPL").unwrap();
    assert!(book.get_order("OCO_SELL").is_none());
}

#[test]
fn test_iceberg_refreshes_slices_until_filled() {
    let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);

    // Seed ask liquidity at the same price to fully fill the iceberg.
    let mut sell = Order::new(
        "maker_sell".to_string(),
        "AAPL".to_string(),
        Side::Sell,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(25, 0),
        TimeInForce::GTC,
        1,
    );
    sell.order_id = "SELL".to_string();
    engine.submit_order_events(sell).unwrap();

    // Submit iceberg buy: total 25, display 10.
    let iceberg = Order::new(
        "iceberg_buy".to_string(),
        "AAPL".to_string(),
        Side::Buy,
        OrderType::Limit,
        Decimal::new(10000, 2),
        Decimal::new(25, 0),
        TimeInForce::GTC,
        2,
    );

    let ev = engine
        .submit_iceberg_events(iceberg, Decimal::new(10, 0))
        .unwrap();

    let ack_count = ev
        .iter()
        .filter(|e| matches!(e.kind, EngineEventKind::Ack { .. }))
        .count();
    let done_filled_count = ev
        .iter()
        .filter(|e| matches!(e.kind, EngineEventKind::Done { reason: matching_engine::DoneReason::Filled, .. }))
        .count();
    let total_filled: Decimal = ev
        .iter()
        .filter_map(|e| match &e.kind {
            EngineEventKind::Fill { qty, .. } => Some(*qty),
            _ => None,
        })
        .sum();

    // Expect 3 child slices (10, 10, 5) => 3 acks, 3 filled dones.
    assert_eq!(ack_count, 3);
    assert_eq!(done_filled_count, 4); // includes maker sell Done(Filled) as well
    assert_eq!(total_filled, Decimal::new(50, 0)); // both sides emit Fill events
}

