//! Matching logic utilities
//!
//! This module contains helper functions for order matching.
//! The core matching logic is implemented in OrderBook::match_order.

use crate::types::{Order, Trade};
use crate::orderbook::OrderBook;

/// Match an order against an order book
///
/// This is a convenience function that wraps OrderBook::match_order.
/// The actual matching logic is implemented in the OrderBook.
pub fn match_order(orderbook: &mut OrderBook, order: Order) -> Vec<Trade> {
    orderbook.match_order(order)
}
