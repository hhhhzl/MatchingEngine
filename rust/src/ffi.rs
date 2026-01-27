//! C ABI bindings for the matching engine.
//!
//! Design goals:
//! - Stable, language-agnostic interface (C ABI)
//! - Deterministic, replayable outputs via sequenced `EngineEvent` streams
//! - High performance transport using MessagePack encoding (rmp-serde)
//!
//! Memory management:
//! - All buffers returned by this API must be freed with `mf_buffer_free`.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;

use rust_decimal::Decimal;

use crate::engine_l2::MatchingEngine;
use crate::types::{EngineEvent, MatchPriceRule, MarketStatus, Order, OrderType, Side, TimeInForce};

const MF_OK: c_int = 0;
const MF_ERR_NULL: c_int = 1;
const MF_ERR_INVALID: c_int = 2;
const MF_ERR_INTERNAL: c_int = 3;

/// Fixed scale for price/qty inputs in FFI.
///
/// All numeric values are provided as scaled integers:
/// - Decimal = value / MF_DECIMAL_SCALE_FACTOR
///
/// Note: `rust_decimal` uses "scale" to mean *decimal places* (0..=28), so we keep both
/// the factor and the decimal-place count here.
#[allow(dead_code)]
pub const MF_DECIMAL_SCALE_FACTOR: i64 = 1_000_000_000;
pub const MF_DECIMAL_SCALE_PLACES: u32 = 9;

#[repr(C)]
pub struct MFBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct MFOrder {
    /// Optional. If NULL, engine assigns an order id.
    pub order_id: *const c_char,
    pub client_order_id: *const c_char,
    pub symbol: *const c_char,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub price: i64,
    pub qty: i64,
    pub timestamp_ns: i64,
}

#[repr(C)]
pub struct MFEngine {
    inner: MatchingEngine,
}

fn cstr_to_string(p: *const c_char) -> Result<String, c_int> {
    if p.is_null() {
        return Err(MF_ERR_NULL);
    }
    let s = unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| MF_ERR_INVALID)?;
    Ok(s.to_string())
}

fn decimal_from_scaled(v: i64) -> Decimal {
    // i64 -> Decimal with fixed scale; use i128 to avoid overflow on intermediate conversions.
    Decimal::from_i128_with_scale(v as i128, MF_DECIMAL_SCALE_PLACES)
}

fn side_from_u8(v: u8) -> Result<Side, c_int> {
    match v {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        _ => Err(MF_ERR_INVALID),
    }
}

fn order_type_from_u8(v: u8) -> Result<OrderType, c_int> {
    match v {
        0 => Ok(OrderType::Market),
        1 => Ok(OrderType::Limit),
        _ => Err(MF_ERR_INVALID),
    }
}

fn tif_from_u8(v: u8) -> Result<TimeInForce, c_int> {
    match v {
        0 => Ok(TimeInForce::GTC),
        1 => Ok(TimeInForce::Day),
        2 => Ok(TimeInForce::IOC),
        3 => Ok(TimeInForce::FOK),
        _ => Err(MF_ERR_INVALID),
    }
}

fn price_rule_from_u8(v: u8) -> Result<MatchPriceRule, c_int> {
    match v {
        0 => Ok(MatchPriceRule::Maker),
        1 => Ok(MatchPriceRule::Taker),
        2 => Ok(MatchPriceRule::Midpoint),
        _ => Err(MF_ERR_INVALID),
    }
}

fn market_status_from_u8(v: u8) -> Result<MarketStatus, c_int> {
    match v {
        0 => Ok(MarketStatus::PreOpen),
        1 => Ok(MarketStatus::Open),
        2 => Ok(MarketStatus::Halted),
        3 => Ok(MarketStatus::Closed),
        _ => Err(MF_ERR_INVALID),
    }
}

fn encode_msgpack<T: serde::Serialize>(value: &T) -> Result<MFBuffer, c_int> {
    let mut buf = Vec::<u8>::new();
    rmp_serde::encode::write(&mut buf, value).map_err(|_| MF_ERR_INTERNAL)?;
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    Ok(MFBuffer { ptr, len })
}

unsafe fn write_out(out: *mut MFBuffer, buf: MFBuffer) -> Result<(), c_int> {
    if out.is_null() {
        return Err(MF_ERR_NULL);
    }
    ptr::write(out, buf);
    Ok(())
}

/// Free a buffer returned by this API.
#[no_mangle]
pub extern "C" fn mf_buffer_free(buf: MFBuffer) {
    if buf.ptr.is_null() || buf.len == 0 {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(buf.ptr, buf.len, buf.len);
    }
}

/// Create an engine with `n` symbols.
///
/// `symbols` is an array of null-terminated C strings.
#[no_mangle]
pub extern "C" fn mf_engine_new(symbols: *const *const c_char, n: usize) -> *mut MFEngine {
    if symbols.is_null() {
        return ptr::null_mut();
    }
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let p = unsafe { *symbols.add(i) };
        if let Ok(s) = cstr_to_string(p) {
            v.push(s);
        } else {
            return ptr::null_mut();
        }
    }
    let engine = MFEngine {
        inner: MatchingEngine::new(v),
    };
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn mf_engine_free(engine: *mut MFEngine) {
    if engine.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(engine));
    }
}

fn order_from_ffi(o: &MFOrder) -> Result<Order, c_int> {
    let client_order_id = cstr_to_string(o.client_order_id)?;
    let symbol = cstr_to_string(o.symbol)?;
    let side = side_from_u8(o.side)?;
    let order_type = order_type_from_u8(o.order_type)?;
    let tif = tif_from_u8(o.time_in_force)?;
    let price = decimal_from_scaled(o.price);
    let qty = decimal_from_scaled(o.qty);

    let mut order = Order::new(
        client_order_id,
        symbol,
        side,
        order_type,
        price,
        qty,
        tif,
        o.timestamp_ns,
    );

    if !o.order_id.is_null() {
        let oid = cstr_to_string(o.order_id)?;
        if !oid.is_empty() {
            order.order_id = oid;
        }
    }
    Ok(order)
}

/// Submit an order and return MessagePack-encoded `Vec<EngineEvent>` in `out`.
#[no_mangle]
pub extern "C" fn mf_engine_submit_order_events(
    engine: *mut MFEngine,
    order: *const MFOrder,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() || order.is_null() {
        return MF_ERR_NULL;
    }
    let engine = unsafe { &mut *engine };
    let order = unsafe { &*order };
    let o = match order_from_ffi(order) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let events: Vec<EngineEvent> = match engine.inner.submit_order_events(o) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&events) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_cancel_order_events(
    engine: *mut MFEngine,
    symbol: *const c_char,
    order_id: *const c_char,
    timestamp_ns: i64,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let oid = match cstr_to_string(order_id) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let engine = unsafe { &mut *engine };
    let events = match engine.inner.cancel_order_events(&sym, &oid, timestamp_ns) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&events) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_replace_order_events(
    engine: *mut MFEngine,
    symbol: *const c_char,
    order_id: *const c_char,
    new_price: i64,
    new_qty: i64,
    timestamp_ns: i64,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let oid = match cstr_to_string(order_id) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let engine = unsafe { &mut *engine };
    let events = match engine.inner.replace_order_events(
        &sym,
        &oid,
        decimal_from_scaled(new_price),
        decimal_from_scaled(new_qty),
        timestamp_ns,
    ) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&events) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_set_market_status_events(
    engine: *mut MFEngine,
    symbol: *const c_char,
    status: u8,
    timestamp_ns: i64,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let status = match market_status_from_u8(status) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let engine = unsafe { &mut *engine };
    let events = match engine.inner.set_market_status_events(&sym, status, timestamp_ns) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&events) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_set_price_rule(engine: *mut MFEngine, symbol: *const c_char, rule: u8) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let rule = match price_rule_from_u8(rule) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let engine = unsafe { &mut *engine };
    if engine.inner.set_price_rule(&sym, rule).is_err() {
        return MF_ERR_INTERNAL;
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_open_auction_events(
    engine: *mut MFEngine,
    symbol: *const c_char,
    timestamp_ns: i64,
    reference_price: i64,
    reference_price_is_set: u8,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let ref_price = if reference_price_is_set != 0 {
        Some(decimal_from_scaled(reference_price))
    } else {
        None
    };
    let engine = unsafe { &mut *engine };
    let events = match engine.inner.open_auction_events(&sym, timestamp_ns, ref_price) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&events) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

#[no_mangle]
pub extern "C" fn mf_engine_book_snapshot(
    engine: *mut MFEngine,
    symbol: *const c_char,
    depth: usize,
    out: *mut MFBuffer,
) -> c_int {
    if engine.is_null() {
        return MF_ERR_NULL;
    }
    let sym = match cstr_to_string(symbol) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let engine = unsafe { &mut *engine };
    let snapshot = match engine.inner.get_book_snapshot(&sym, depth) {
        Ok(v) => v,
        Err(_) => return MF_ERR_INTERNAL,
    };
    let buf = match encode_msgpack(&snapshot) {
        Ok(b) => b,
        Err(e) => return e,
    };
    unsafe {
        if let Err(e) = write_out(out, buf) {
            return e;
        }
    }
    MF_OK
}

