use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::types::{PegMode, Side};

/// Convert bps to a multiplicative factor: \(1 \pm bps/10000\).
pub fn bps_factor(bps: Decimal) -> Decimal {
    // bps is typically small; keep as Decimal to avoid float drift.
    Decimal::ONE + (bps / Decimal::new(10_000, 0))
}

/// Compute mid price using bid/ask/last fallbacks.
pub fn mid(bid: Option<Decimal>, ask: Option<Decimal>, last: Option<Decimal>) -> Option<Decimal> {
    match (bid, ask) {
        (Some(b), Some(a)) if b > Decimal::ZERO && a > Decimal::ZERO => Some((b + a) / Decimal::new(2, 0)),
        _ => last.filter(|p| *p > Decimal::ZERO),
    }
}

/// Choose reference price based on peg mode.
pub fn peg_price(peg: PegMode, bid: Option<Decimal>, ask: Option<Decimal>, last: Option<Decimal>) -> Option<Decimal> {
    match peg {
        PegMode::None => mid(bid, ask, last),
        PegMode::Mid => mid(bid, ask, last),
        PegMode::Bid => bid.filter(|p| *p > Decimal::ZERO).or_else(|| mid(bid, ask, last)),
        PegMode::Ask => ask.filter(|p| *p > Decimal::ZERO).or_else(|| mid(bid, ask, last)),
    }
}

/// Quantize a price to the tick grid with a side-aware rule:
/// - Buy: round down (floor) to avoid crossing higher than intended.
/// - Sell: round up (ceil) to avoid crossing lower than intended.
pub fn quantize_price(price: Decimal, tick: Decimal, side: Side) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let q = (price / tick).to_f64().unwrap_or(0.0);
    let k = match side {
        Side::Buy => q.floor(),
        Side::Sell => q.ceil(),
    };
    let k_dec = Decimal::from_f64_retain(k).unwrap_or(Decimal::ZERO);
    k_dec * tick
}

/// Quantize quantity to lot size (always round down).
pub fn quantize_qty(qty: Decimal, lot: Decimal) -> Decimal {
    if lot <= Decimal::ZERO {
        return qty;
    }
    let q = (qty / lot).to_f64().unwrap_or(0.0);
    let k = q.floor();
    let k_dec = Decimal::from_f64_retain(k).unwrap_or(Decimal::ZERO);
    k_dec * lot
}

/// Compute absolute difference in bps between a and b: |a-b|/a * 10000.
pub fn diff_bps(a: Decimal, b: Decimal) -> Option<Decimal> {
    if a <= Decimal::ZERO {
        return None;
    }
    Some(((a - b).abs() / a) * Decimal::new(10_000, 0))
}

