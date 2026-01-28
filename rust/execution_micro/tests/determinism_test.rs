use execution_micro::{decide, CancelReplacePolicy, MicroInput, MicroOrder, PegMode, PricePolicy, Side};
use rust_decimal::Decimal;

#[test]
fn decision_is_deterministic() {
    let inp = MicroInput {
        symbol: "AAPL".to_string(),
        venue: "NASDAQ".to_string(),
        side: Side::Buy,
        desired_qty: Decimal::new(100, 0),
        tick_size: Decimal::new(1, 2), // 0.01
        lot_size: Decimal::new(1, 0),
        min_notional: Some(Decimal::new(100, 0)),
        bid: Some(Decimal::new(10000, 2)),
        ask: Some(Decimal::new(10002, 2)),
        last: Some(Decimal::new(10001, 2)),
        price_policy: PricePolicy {
            price_limit: Some(Decimal::new(10005, 2)),
            limit_offset_bps: Decimal::new(5, 0),
            price_protection_bps: Decimal::new(10, 0),
            max_slippage_bps: None,
            peg_mode: PegMode::Mid,
        },
        cr_policy: CancelReplacePolicy::default(),
        current: Some(MicroOrder {
            client_order_id: "c1".to_string(),
            working_qty: Decimal::new(100, 0),
            working_price: Decimal::new(9990, 2),
            last_update_ts_ns: 0,
            replaces: 0,
        }),
        now_ts_ns: 2_000_000_000,
    };

    let a = decide(&inp).unwrap();
    let b = decide(&inp).unwrap();
    assert_eq!(a, b);
}

