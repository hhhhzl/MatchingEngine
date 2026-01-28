use rust_decimal::Decimal;
use thiserror::Error;

use crate::math::{bps_factor, diff_bps, mid, peg_price, quantize_price, quantize_qty};
use crate::types::{MicroDecision, MicroInput, PegMode, Side};

#[derive(Debug, Error)]
pub enum DecisionError {
    #[error("invalid tick_size (must be > 0)")]
    InvalidTickSize,
    #[error("invalid lot_size (must be > 0)")]
    InvalidLotSize,
    #[error("desired_qty must be > 0")]
    InvalidDesiredQty,
    #[error("no usable market price (bid/ask/last missing or invalid)")]
    NoMarketPrice,
}

/// Decide is the top-level micro-decision entry point.
///
/// The caller is responsible for:
/// - Translating system-specific fixed-point types into Decimals.
/// - Applying the decision (placing/canceling/replacing orders).
pub fn decide(input: &MicroInput) -> Result<MicroDecision, DecisionError> {
    if input.tick_size <= Decimal::ZERO {
        return Err(DecisionError::InvalidTickSize);
    }
    if input.lot_size <= Decimal::ZERO {
        return Err(DecisionError::InvalidLotSize);
    }
    if input.desired_qty <= Decimal::ZERO {
        return Err(DecisionError::InvalidDesiredQty);
    }

    let ref_px = peg_price(
        input.price_policy.peg_mode,
        input.bid,
        input.ask,
        input.last,
    )
    .ok_or(DecisionError::NoMarketPrice)?;

    let mid_px = mid(input.bid, input.ask, input.last).ok_or(DecisionError::NoMarketPrice)?;

    // Price construction:
    // 1) Start with peg reference.
    // 2) Apply offset away from reference.
    // 3) Clamp to hard limit and protection bands.
    // 4) Quantize to tick grid with side-aware rounding.
    let mut px = match input.side {
        Side::Buy => ref_px * bps_factor(-input.price_policy.limit_offset_bps),
        Side::Sell => ref_px * bps_factor(input.price_policy.limit_offset_bps),
    };

    // Mid-based protection clamp (static protection + optional dynamic max_slippage).
    let mut protection_bps = input.price_policy.price_protection_bps;
    if let Some(ms) = input.price_policy.max_slippage_bps {
        protection_bps = protection_bps.min(ms);
    }
    if protection_bps > Decimal::ZERO {
        let max_move = protection_bps / Decimal::new(10_000, 0);
        match input.side {
            Side::Buy => {
                // Do not bid too high above mid.
                px = px.min(mid_px * (Decimal::ONE + max_move));
            }
            Side::Sell => {
                // Do not offer too low below mid.
                px = px.max(mid_px * (Decimal::ONE - max_move));
            }
        }
    }

    // Hard price limit clamp.
    if let Some(limit) = input.price_policy.price_limit {
        match input.side {
            Side::Buy => px = px.min(limit),
            Side::Sell => px = px.max(limit),
        }
    }

    // If peg_mode is "none", we still treat it as mid-based default.
    if input.price_policy.peg_mode == PegMode::None {
        // No-op: peg_price already falls back to mid.
    }

    px = quantize_price(px, input.tick_size, input.side);
    if px <= Decimal::ZERO {
        return Err(DecisionError::NoMarketPrice);
    }

    // Quantity construction:
    // - Enforce lot size (round down).
    // - Enforce min notional if provided.
    let qty = quantize_qty(input.desired_qty, input.lot_size);
    if qty <= Decimal::ZERO {
        return Ok(MicroDecision::Noop {
            reason: "desired_qty below lot_size".to_string(),
        });
    }
    if let Some(min_notional) = input.min_notional {
        let notion = qty * mid_px;
        if notion < min_notional {
            return Ok(MicroDecision::Noop {
                reason: "below min_notional".to_string(),
            });
        }
    }

    // If there is no working order, propose a placement.
    let Some(cur) = &input.current else {
        return Ok(MicroDecision::Place {
            qty,
            price: px,
            reason: "no working order".to_string(),
        });
    };

    // If the working order is already effectively correct, do nothing.
    if cur.working_qty == qty && cur.working_price == px {
        return Ok(MicroDecision::Noop {
            reason: "already at desired qty/price".to_string(),
        });
    }

    // Respect minimum lifetime before replacing.
    let age_ms = ((input.now_ts_ns - cur.last_update_ts_ns).max(0) as u128) / 1_000_000;
    if age_ms < input.cr_policy.min_lifetime_ms as u128 {
        return Ok(MicroDecision::Noop {
            reason: "min_lifetime_ms not reached".to_string(),
        });
    }

    // Replacement budget.
    if cur.replaces >= input.cr_policy.max_replaces {
        return Ok(MicroDecision::Noop {
            reason: "max_replaces reached".to_string(),
        });
    }

    // Replace threshold in bps.
    let bps = diff_bps(cur.working_price, px).unwrap_or(Decimal::ZERO);
    if bps < input.cr_policy.replace_threshold_bps {
        return Ok(MicroDecision::Noop {
            reason: "price diff below replace_threshold_bps".to_string(),
        });
    }

    Ok(MicroDecision::Replace {
        cancel_client_order_id: cur.client_order_id.clone(),
        new_qty: qty,
        new_price: px,
        reason: format!("replace: diff_bps={bps}"),
    })
}

