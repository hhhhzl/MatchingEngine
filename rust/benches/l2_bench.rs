use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use matching_engine::{MatchingEngine, Order, OrderType, Side, TimeInForce};
use rust_decimal::Decimal;

fn bench_l2_submit_and_match(c: &mut Criterion) {
    c.bench_function("l2_submit_match_simple", |b| {
        b.iter_batched(
            || {
                let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
                // Seed with a resting sell order.
                let sell = Order::new(
                    "maker_sell".to_string(),
                    "AAPL".to_string(),
                    Side::Sell,
                    OrderType::Limit,
                    Decimal::new(10000, 2),
                    Decimal::new(1000, 0),
                    TimeInForce::GTC,
                    1,
                );
                let _ = engine.submit_order(sell).unwrap();
                engine
            },
            |mut engine| {
                // Submit a crossing buy order.
                let buy = Order::new(
                    "taker_buy".to_string(),
                    "AAPL".to_string(),
                    Side::Buy,
                    OrderType::Limit,
                    Decimal::new(10100, 2),
                    Decimal::new(100, 0),
                    TimeInForce::GTC,
                    2,
                );
                let _ = engine.submit_order(buy).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_l2_cancel(c: &mut Criterion) {
    c.bench_function("l2_cancel_o1", |b| {
        b.iter_batched(
            || {
                let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
                // Add many resting buy orders across a few price levels.
                for i in 0..10_000u64 {
                    let mut o = Order::new(
                        format!("c{}", i),
                        "AAPL".to_string(),
                        Side::Buy,
                        OrderType::Limit,
                        Decimal::new(10000 + (i as i64 % 10), 2),
                        Decimal::new(1, 0),
                        TimeInForce::GTC,
                        i as i64,
                    );
                    o.order_id = format!("OID_{}", i);
                    let _ = engine.submit_order(o).unwrap();
                }
                // Cancel something from the middle.
                ("AAPL".to_string(), "OID_5000".to_string(), engine)
            },
            |(symbol, oid, mut engine)| {
                let _ = engine.cancel_order(&symbol, &oid).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_l2_submit_and_match, bench_l2_cancel);
criterion_main!(benches);

