//! L2 Matching Engine for managing multiple order books

use std::collections::HashMap;

use crate::orderbook::OrderBook;
use crate::types::{BookSnapshot, EngineEvent, EngineEventKind, MatchPriceRule, MarketStatus, Order, Trade};
use crate::error::{Result, MatchingError};
use rust_decimal::Decimal;
use std::collections::VecDeque;

/// L2 Matching Engine managing multiple order books (one per symbol)
///
/// Provides a high-level interface for order submission, cancellation, and matching
/// across multiple trading symbols using L2 order book (price-level aggregation).
pub struct MatchingEngine {
    /// Map of symbol to order book
    orderbooks: HashMap<String, OrderBook>,
    /// Order ID counter for generating unique order IDs
    order_id_counter: u64,
    /// Monotonic event sequence counter (per engine instance).
    seq: u64,
    /// Per-symbol market status.
    market_status: HashMap<String, MarketStatus>,
    /// Per-symbol trade price rule.
    price_rule: HashMap<String, MatchPriceRule>,
    /// OCO group -> (order_id_a, order_id_b).
    oco_groups: HashMap<String, (String, String)>,
    /// order_id -> group_id.
    oco_by_order: HashMap<String, String>,
    /// Iceberg id counter.
    iceberg_counter: u64,
    /// iceberg_id -> iceberg state.
    icebergs: HashMap<String, IcebergState>,
    /// active slice order_id -> iceberg_id.
    iceberg_by_order: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct IcebergState {
    symbol: String,
    account_id: String,
    side: crate::types::Side,
    price: Decimal,
    display_qty: Decimal,
    remaining_qty: Decimal,
    client_order_id: String,
    time_in_force: crate::types::TimeInForce,
}

impl MatchingEngine {
    /// Create a new matching engine with order books for the given symbols
    pub fn new(symbols: Vec<String>) -> Self {
        let orderbooks: HashMap<String, OrderBook> = symbols
            .into_iter()
            .map(|symbol| (symbol.clone(), OrderBook::new(symbol)))
            .collect();

        let mut market_status = HashMap::new();
        let mut price_rule = HashMap::new();
        for sym in orderbooks.keys() {
            market_status.insert(sym.clone(), MarketStatus::Open);
            price_rule.insert(sym.clone(), MatchPriceRule::Maker);
        }

        Self {
            orderbooks,
            order_id_counter: 0,
            seq: 0,
            market_status,
            price_rule,
            oco_groups: HashMap::new(),
            oco_by_order: HashMap::new(),
            iceberg_counter: 0,
            icebergs: HashMap::new(),
            iceberg_by_order: HashMap::new(),
        }
    }

    /// Add a new symbol to the engine
    ///
    /// Creates a new order book for the symbol if it doesn't already exist.
    pub fn add_symbol(&mut self, symbol: String) {
        if !self.orderbooks.contains_key(&symbol) {
            self.orderbooks
                .insert(symbol.clone(), OrderBook::new(symbol.clone()));
            self.market_status.insert(symbol.clone(), MarketStatus::Open);
            self.price_rule.insert(symbol, MatchPriceRule::Maker);
        }
    }

    fn next_order_id(&mut self) -> String {
        self.order_id_counter += 1;
        format!("ORDER_{}", self.order_id_counter)
    }

    fn wrap_events(&mut self, symbol: &str, timestamp_ns: i64, kinds: Vec<EngineEventKind>) -> Vec<EngineEvent> {
        kinds
            .into_iter()
            .map(|kind| {
                self.seq += 1;
                EngineEvent {
                    seq: self.seq,
                    timestamp_ns,
                    symbol: symbol.to_string(),
                    kind,
                }
            })
            .collect()
    }

    fn expand_side_effects(
        &mut self,
        symbol: &str,
        timestamp_ns: i64,
        initial: Vec<EngineEventKind>,
    ) -> Result<Vec<EngineEventKind>> {
        let mut q: VecDeque<EngineEventKind> = initial.into();
        let mut out: Vec<EngineEventKind> = Vec::new();

        while let Some(kind) = q.pop_front() {
            // Triggers are evaluated based on the event being appended.
            let mut followups: Vec<EngineEventKind> = Vec::new();

            match &kind {
                EngineEventKind::Done { order_id, reason, .. } => {
                    // OCO trigger: fill or cancel terminates sibling.
                    if (*reason == crate::types::DoneReason::Filled || *reason == crate::types::DoneReason::Canceled)
                        && self.oco_by_order.contains_key(order_id)
                    {
                        if let Some(mut k) = self.oco_cancel_sibling_kinds(symbol, order_id)? {
                            followups.append(&mut k);
                        }
                    }

                    // Iceberg trigger: when a slice fully fills, submit the next slice.
                    if *reason == crate::types::DoneReason::Filled && self.iceberg_by_order.contains_key(order_id) {
                        if let Some(mut k) = self.iceberg_submit_next_slice_kinds(symbol, timestamp_ns, order_id)? {
                            followups.append(&mut k);
                        }
                    }
                }
                EngineEventKind::CancelAck { order_id, .. } => {
                    // OCO trigger: manual cancel cancels sibling.
                    if self.oco_by_order.contains_key(order_id) {
                        if let Some(mut k) = self.oco_cancel_sibling_kinds(symbol, order_id)? {
                            followups.append(&mut k);
                        }
                    }
                    // Iceberg trigger: cancel stops the iceberg.
                    if self.iceberg_by_order.contains_key(order_id) {
                        self.iceberg_stop(order_id);
                    }
                }
                _ => {}
            }

            out.push(kind);

            if !followups.is_empty() {
                // Insert followups immediately after the triggering event.
                for k in followups.into_iter().rev() {
                    q.push_front(k);
                }
            }
        }

        Ok(out)
    }

    fn oco_cancel_sibling_kinds(
        &mut self,
        symbol: &str,
        trigger_order_id: &str,
    ) -> Result<Option<Vec<EngineEventKind>>> {
        let Some(group_id) = self.oco_by_order.remove(trigger_order_id) else { return Ok(None) };
        let Some((a, b)) = self.oco_groups.remove(&group_id) else { return Ok(None) };

        let sibling = if a == trigger_order_id { b } else { a };
        let _ = self.oco_by_order.remove(&sibling);

        let orderbook = self
            .orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;

        match orderbook.cancel_order_kinds(&sibling) {
            Ok(kinds) => Ok(Some(kinds)),
            Err(_) => Ok(None),
        }
    }

    fn iceberg_stop(&mut self, order_id: &str) {
        if let Some(iceberg_id) = self.iceberg_by_order.remove(order_id) {
            self.icebergs.remove(&iceberg_id);
        }
    }

    fn iceberg_submit_next_slice_kinds(
        &mut self,
        symbol: &str,
        timestamp_ns: i64,
        filled_slice_order_id: &str,
    ) -> Result<Option<Vec<EngineEventKind>>> {
        let Some(iceberg_id) = self.iceberg_by_order.remove(filled_slice_order_id) else {
            return Ok(None);
        };
        let (client_order_id, account_id, sym, side, price, tif, remaining_after, slice_qty) = {
            let Some(state) = self.icebergs.get_mut(&iceberg_id) else {
                return Ok(None);
            };

            if state.remaining_qty <= Decimal::ZERO {
                // No remaining quantity.
                return Ok(None);
            }

            let slice_qty = state.display_qty.min(state.remaining_qty);
            state.remaining_qty -= slice_qty;
            (
                state.client_order_id.clone(),
                state.account_id.clone(),
                state.symbol.clone(),
                state.side,
                state.price,
                state.time_in_force,
                state.remaining_qty,
                slice_qty,
            )
        };

        // If we exhausted the iceberg after taking this slice, we'll clean up after submission.
        let slice_order_id = self.next_order_id();

        let mut slice = Order::new_with_account(
            client_order_id.clone(),
            account_id.clone(),
            sym.clone(),
            side,
            crate::types::OrderType::Limit,
            price,
            slice_qty,
            tif,
            timestamp_ns,
        );
        slice.order_id = slice_order_id.clone();

        // Track the new active slice.
        self.iceberg_by_order
            .insert(slice_order_id.clone(), iceberg_id.clone());

        // Execute the slice like a normal order in the current market state.
        let kinds = {
            let orderbook = self
                .orderbooks
                .get_mut(symbol)
                .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;
            let status = *self.market_status.get(symbol).unwrap_or(&MarketStatus::Open);
            let price_rule = *self.price_rule.get(symbol).unwrap_or(&MatchPriceRule::Maker);
            match status {
                MarketStatus::Open => orderbook.match_order_kinds_with_price_rule(slice, price_rule)?,
                MarketStatus::PreOpen => orderbook.add_order_kinds(slice)?,
                MarketStatus::Halted | MarketStatus::Closed => vec![
                    EngineEventKind::Reject {
                        order_id: slice_order_id.clone(),
                        client_order_id: client_order_id.clone(),
                        reason: "Market not open".to_string(),
                    },
                    EngineEventKind::Done {
                        order_id: slice_order_id.clone(),
                        client_order_id: client_order_id.clone(),
                        reason: crate::types::DoneReason::Rejected,
                    },
                ],
            }
        };

        // Clean up if this slice consumed the last remaining quantity.
        if remaining_after <= Decimal::ZERO {
            self.icebergs.remove(&iceberg_id);
        }

        Ok(Some(kinds))
    }

    /// Set the trade price rule for a symbol.
    pub fn set_price_rule(&mut self, symbol: &str, rule: MatchPriceRule) -> Result<()> {
        if !self.orderbooks.contains_key(symbol) {
            return Err(MatchingError::OrderbookNotFound(symbol.to_string()));
        }
        self.price_rule.insert(symbol.to_string(), rule);
        Ok(())
    }

    /// Transition a symbol market status and emit a status event.
    pub fn set_market_status_events(
        &mut self,
        symbol: &str,
        status: MarketStatus,
        timestamp_ns: i64,
    ) -> Result<Vec<EngineEvent>> {
        if !self.orderbooks.contains_key(symbol) {
            return Err(MatchingError::OrderbookNotFound(symbol.to_string()));
        }
        self.market_status.insert(symbol.to_string(), status);
        Ok(self.wrap_events(
            symbol,
            timestamp_ns,
            vec![EngineEventKind::MarketStatus {
                symbol: symbol.to_string(),
                status,
            }],
        ))
    }

    /// Run an auction uncrossing pass and transition the market to `Open`.
    pub fn open_auction_events(
        &mut self,
        symbol: &str,
        timestamp_ns: i64,
        reference_price: Option<rust_decimal::Decimal>,
    ) -> Result<Vec<EngineEvent>> {
        let status = *self
            .market_status
            .get(symbol)
            .unwrap_or(&MarketStatus::Open);
        if status != MarketStatus::PreOpen {
            return Err(MatchingError::InvalidOrder(
                "Auction can only be run from PreOpen".to_string(),
            ));
        }

        let orderbook = self
            .orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;

        let mut kinds = Vec::new();
        kinds.push(EngineEventKind::MarketStatus {
            symbol: symbol.to_string(),
            status: MarketStatus::Open,
        });
        kinds.extend(orderbook.auction_uncross_kinds(reference_price)?);

        self.market_status.insert(symbol.to_string(), MarketStatus::Open);
        let kinds = self.expand_side_effects(symbol, timestamp_ns, kinds)?;
        Ok(self.wrap_events(symbol, timestamp_ns, kinds))
    }

    fn kinds_to_trades(&self, symbol: &str, timestamp_ns: i64, kinds: &[EngineEventKind]) -> Vec<Trade> {
        let mut trades = Vec::new();
        for k in kinds {
            if let EngineEventKind::Fill {
                trade_id,
                order_id,
                client_order_id,
                contra_order_id,
                side,
                liquidity,
                price,
                qty,
                ..
            } = k
            {
                if *liquidity == crate::types::Liquidity::Taker {
                    trades.push(Trade {
                        trade_id: trade_id.clone(),
                        order_id: order_id.clone(),
                        client_order_id: client_order_id.clone(),
                        contra_order_id: Some(contra_order_id.clone()),
                        symbol: symbol.to_string(),
                        side: *side,
                        price: *price,
                        qty: *qty,
                        timestamp_ns,
                    });
                }
            }
        }
        trades
    }

    /// Submit an order to the matching engine
    ///
    /// The order will be matched against existing orders in the order book,
    /// and any resulting trades will be returned.
    ///
    /// If the order cannot be fully matched, it will be added to the order book.
    pub fn submit_order(&mut self, mut order: Order) -> Result<Vec<Trade>> {
        let kinds = self.submit_order_kinds(&mut order)?;
        Ok(self.kinds_to_trades(&order.symbol, order.timestamp_ns, &kinds))
    }

    /// Submit an order and return a deterministic event stream.
    pub fn submit_order_events(&mut self, mut order: Order) -> Result<Vec<EngineEvent>> {
        let ts = order.timestamp_ns;
        let symbol = order.symbol.clone();
        let kinds = self.submit_order_kinds(&mut order)?;
        let kinds = self.expand_side_effects(&symbol, ts, kinds)?;
        Ok(self.wrap_events(&symbol, ts, kinds))
    }

    fn submit_order_kinds(&mut self, order: &mut Order) -> Result<Vec<EngineEventKind>> {
        // Generate order_id if not provided (fast monotonic IDs; deterministic).
        if order.order_id.is_empty() {
            order.order_id = self.next_order_id();
        }

        // Get or create orderbook.
        if !self.orderbooks.contains_key(&order.symbol) {
            self.add_symbol(order.symbol.clone());
        }

        let orderbook = self
            .orderbooks
            .get_mut(&order.symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(order.symbol.clone()))?;

        let status = *self
            .market_status
            .get(&order.symbol)
            .unwrap_or(&MarketStatus::Open);
        let price_rule = *self
            .price_rule
            .get(&order.symbol)
            .unwrap_or(&MatchPriceRule::Maker);

        let res = match status {
            MarketStatus::Open => orderbook.match_order_kinds_with_price_rule(order.clone(), price_rule),
            MarketStatus::PreOpen => orderbook.add_order_kinds(order.clone()),
            MarketStatus::Halted | MarketStatus::Closed => Err(MatchingError::InvalidOrder(format!(
                "Market not open for symbol {}",
                order.symbol
            ))),
        };

        match res {
            Ok(kinds) => Ok(kinds),
            Err(err) => Ok(vec![
                EngineEventKind::Reject {
                    order_id: order.order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    reason: err.to_string(),
                },
                EngineEventKind::Done {
                    order_id: order.order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    reason: crate::types::DoneReason::Rejected,
                },
            ]),
        }
    }

    /// Cancel an order
    ///
    /// Returns the cancelled order, or an error if the order is not found.
    pub fn cancel_order(&mut self, symbol: &str, order_id: &str) -> Result<Order> {
        let orderbook = self.orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;

        orderbook.cancel_order(order_id)
    }

    /// Cancel an order and return a deterministic event stream.
    pub fn cancel_order_events(&mut self, symbol: &str, order_id: &str, timestamp_ns: i64) -> Result<Vec<EngineEvent>> {
        let orderbook = self.orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;
        let kinds = orderbook.cancel_order_kinds(order_id)?;
        let kinds = self.expand_side_effects(symbol, timestamp_ns, kinds)?;
        Ok(self.wrap_events(symbol, timestamp_ns, kinds))
    }

    /// Submit a one-cancels-the-other (OCO) pair.
    ///
    /// When either order is filled or canceled, the sibling is canceled automatically (if resting).
    pub fn submit_oco_events(
        &mut self,
        mut a: Order,
        mut b: Order,
        group_id: String,
    ) -> Result<Vec<EngineEvent>> {
        if a.order_id.is_empty() {
            a.order_id = self.next_order_id();
        }
        if b.order_id.is_empty() {
            b.order_id = self.next_order_id();
        }

        self.oco_by_order.insert(a.order_id.clone(), group_id.clone());
        self.oco_by_order.insert(b.order_id.clone(), group_id.clone());
        self.oco_groups
            .insert(group_id, (a.order_id.clone(), b.order_id.clone()));

        let mut out = Vec::new();
        out.extend(self.submit_order_events(a)?);
        out.extend(self.submit_order_events(b)?);
        Ok(out)
    }

    /// Submit an iceberg order (L2 aggregated; implemented as sequential child slices).
    ///
    /// The engine submits visible slices of size `display_qty`. When a slice is fully filled,
    /// the next slice is submitted automatically, losing time priority (new order id).
    pub fn submit_iceberg_events(
        &mut self,
        order: Order,
        display_qty: Decimal,
    ) -> Result<Vec<EngineEvent>> {
        if display_qty <= Decimal::ZERO {
            return Err(MatchingError::InvalidQuantity);
        }
        if order.order_type != crate::types::OrderType::Limit {
            return Err(MatchingError::InvalidOrder(
                "Iceberg requires Limit order".to_string(),
            ));
        }
        if !matches!(order.time_in_force, crate::types::TimeInForce::GTC | crate::types::TimeInForce::Day) {
            return Err(MatchingError::InvalidOrder(
                "Iceberg requires GTC or Day".to_string(),
            ));
        }

        let iceberg_id = {
            self.iceberg_counter += 1;
            format!("ICEBERG_{}", self.iceberg_counter)
        };

        let slice_qty = display_qty.min(order.qty);
        let remaining = order.qty - slice_qty;

        if remaining <= Decimal::ZERO {
            // Degenerate iceberg: just submit as a normal order.
            return self.submit_order_events(order);
        }

        self.icebergs.insert(
            iceberg_id.clone(),
            IcebergState {
                symbol: order.symbol.clone(),
                account_id: order.account_id.clone(),
                side: order.side,
                price: order.price,
                display_qty,
                remaining_qty: remaining,
                client_order_id: order.client_order_id.clone(),
                time_in_force: order.time_in_force,
            },
        );

        let mut first_slice = Order::new_with_account(
            order.client_order_id.clone(),
            order.account_id.clone(),
            order.symbol.clone(),
            order.side,
            crate::types::OrderType::Limit,
            order.price,
            slice_qty,
            order.time_in_force,
            order.timestamp_ns,
        );
        first_slice.order_id = if order.order_id.is_empty() {
            self.next_order_id()
        } else {
            order.order_id
        };

        self.iceberg_by_order
            .insert(first_slice.order_id.clone(), iceberg_id);

        self.submit_order_events(first_slice)
    }

    /// Replace/amend a resting order and return sequenced events.
    pub fn replace_order_events(
        &mut self,
        symbol: &str,
        order_id: &str,
        new_price: rust_decimal::Decimal,
        new_qty: rust_decimal::Decimal,
        timestamp_ns: i64,
    ) -> Result<Vec<EngineEvent>> {
        let orderbook = self
            .orderbooks
            .get_mut(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;
        let kinds = orderbook.replace_order_kinds(order_id, new_price, new_qty, timestamp_ns)?;
        Ok(self.wrap_events(symbol, timestamp_ns, kinds))
    }

    /// Get an aggregated L2 book snapshot (top N levels).
    pub fn get_book_snapshot(&self, symbol: &str, depth: usize) -> Result<BookSnapshot> {
        let orderbook = self
            .orderbooks
            .get(symbol)
            .ok_or_else(|| MatchingError::OrderbookNotFound(symbol.to_string()))?;
        Ok(orderbook.snapshot(depth))
    }

    /// Emit a book snapshot as a sequenced event.
    pub fn book_snapshot_event(&mut self, symbol: &str, depth: usize, timestamp_ns: i64) -> Result<Vec<EngineEvent>> {
        let snapshot = self.get_book_snapshot(symbol, depth)?;
        let kinds = vec![EngineEventKind::BookSnapshot { depth, snapshot }];
        Ok(self.wrap_events(symbol, timestamp_ns, kinds))
    }

    /// Get a reference to an order book
    pub fn get_orderbook(&self, symbol: &str) -> Option<&OrderBook> {
        self.orderbooks.get(symbol)
    }

    /// Get a mutable reference to an order book
    pub fn get_orderbook_mut(&mut self, symbol: &str) -> Option<&mut OrderBook> {
        self.orderbooks.get_mut(symbol)
    }

    /// Get an order by symbol and order ID
    pub fn get_order(&self, symbol: &str, order_id: &str) -> Option<&Order> {
        self.orderbooks
            .get(symbol)?
            .get_order(order_id)
    }

    /// Get all symbols managed by this engine
    pub fn get_symbols(&self) -> Vec<String> {
        self.orderbooks.keys().cloned().collect()
    }
}
