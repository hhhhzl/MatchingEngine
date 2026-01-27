# matching-engine (Rust)

This is the Rust core for MatchingEngine (L2/L3 matching + deterministic events + market data + C ABI).

For the full project overview, quickstart, and bindings docs, see:
- `../README.md`

## Quick usage (recommended: event stream)

```rust
use matching_engine::{MatchingEngine, Order, Side, OrderType, TimeInForce};
use rust_decimal::Decimal;

let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);

let order = Order::new(
    "c1".to_string(),
    "AAPL".to_string(),
    Side::Buy,
    OrderType::Limit,
    Decimal::new(10100, 2),
    Decimal::new(10, 0),
    TimeInForce::GTC,
    1,
);

let events = engine.submit_order_events(order).unwrap();
println!("{:?}", events);
```

## Build / test

```bash
cargo test
cargo clippy -- -D warnings
```

## Benchmarks

```bash
cargo bench
```
