//! L2 order book implementation with price-time priority.
//!
//! This implementation is optimized for:
//! - O(log M) best-price selection via BTreeMap (M = number of price levels)
//! - O(1) cancel by storing an index into an intrusive linked list per price level
//! - Deterministic behavior suitable for replay (no randomized IDs inside the book)

use std::collections::{BTreeMap, HashMap};

use rust_decimal::Decimal;

use crate::error::{MatchingError, Result};
use crate::types::{
    BookDeltaReason, BookLevel, BookSnapshot, DoneReason, EngineEventKind, Liquidity, MarketData,
    MatchPriceRule, Order, OrderStatus, OrderType, Side, TimeInForce, Trade,
};

/// Node in an intrusive doubly-linked list.
#[derive(Debug, Clone)]
struct Node {
    prev: Option<u32>,
    next: Option<u32>,
    order_id: String,
}

/// Simple index allocator for nodes.
#[derive(Debug, Default)]
struct NodeArena {
    nodes: Vec<Option<Node>>,
    free: Vec<u32>,
}

impl NodeArena {
    fn alloc(&mut self, node: Node) -> u32 {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx as usize] = Some(node);
            idx
        } else {
            let idx = self.nodes.len() as u32;
            self.nodes.push(Some(node));
            idx
        }
    }

    fn get(&self, idx: u32) -> &Node {
        self.nodes[idx as usize].as_ref().expect("node must exist")
    }

    fn get_mut(&mut self, idx: u32) -> &mut Node {
        self.nodes[idx as usize].as_mut().expect("node must exist")
    }

    fn free(&mut self, idx: u32) {
        self.nodes[idx as usize] = None;
        self.free.push(idx);
    }
}

/// Price level with FIFO queue.
#[derive(Debug, Default)]
struct PriceLevel {
    head: Option<u32>,
    tail: Option<u32>,
    total_qty: Decimal,
}

impl PriceLevel {
    fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    fn peek_front(&self) -> Option<u32> {
        self.head
    }

    fn push_back(&mut self, arena: &mut NodeArena, order_id: String) -> u32 {
        let idx = arena.alloc(Node {
            prev: self.tail,
            next: None,
            order_id,
        });
        match self.tail {
            None => {
                self.head = Some(idx);
                self.tail = Some(idx);
            }
            Some(tail) => {
                arena.get_mut(tail).next = Some(idx);
                self.tail = Some(idx);
            }
        }
        idx
    }

    fn remove(&mut self, arena: &mut NodeArena, idx: u32) {
        let (prev, next) = {
            let node = arena.get(idx);
            (node.prev, node.next)
        };

        match prev {
            None => {
                self.head = next;
            }
            Some(p) => {
                arena.get_mut(p).next = next;
            }
        }

        match next {
            None => {
                self.tail = prev;
            }
            Some(n) => {
                arena.get_mut(n).prev = prev;
            }
        }

        arena.free(idx);
    }
}

#[derive(Debug, Clone, Copy)]
struct OrderLoc {
    side: Side,
    price: Decimal,
    node_idx: u32,
}

#[derive(Debug, Clone)]
struct OrderEntry {
    order: Order,
    loc: OrderLoc,
}

/// L2 OrderBook for a single symbol.
pub struct OrderBook {
    symbol: String,
    bids: BTreeMap<Decimal, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    arena: NodeArena,
    orders: HashMap<String, OrderEntry>,
    last_trade_price: Option<Decimal>,
    order_id_counter: u64,
    trade_id_counter: u64,
}

impl OrderBook {
    /// Create a new order book for a symbol.
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            arena: NodeArena::default(),
            orders: HashMap::new(),
            last_trade_price: None,
            order_id_counter: 0,
            trade_id_counter: 0,
        }
    }

    fn next_order_id(&mut self) -> String {
        self.order_id_counter += 1;
        format!("ORDER_{}", self.order_id_counter)
    }

    fn next_trade_id(&mut self) -> String {
        self.trade_id_counter += 1;
        format!("TRADE_{}", self.trade_id_counter)
    }

    fn side_book_and_arena_mut(
        &mut self,
        side: Side,
    ) -> (&mut BTreeMap<Decimal, PriceLevel>, &mut NodeArena) {
        match side {
            Side::Buy => (&mut self.bids, &mut self.arena),
            Side::Sell => (&mut self.asks, &mut self.arena),
        }
    }

    fn crosses(&self, incoming: &Order, best_opp_price: Decimal) -> bool {
        match incoming.order_type {
            OrderType::Market => true,
            OrderType::Limit => match incoming.side {
                Side::Buy => incoming.price >= best_opp_price,
                Side::Sell => incoming.price <= best_opp_price,
            },
            _ => false,
        }
    }

    fn best_bid_ask(&self) -> (Option<Decimal>, Option<Decimal>) {
        (
            self.bids.keys().next_back().cloned(),
            self.asks.keys().next().cloned(),
        )
    }

    fn midpoint(&self) -> Option<Decimal> {
        let (bid, ask) = self.best_bid_ask();
        match (bid, ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
            _ => None,
        }
    }

    fn compute_trade_price(&self, rule: MatchPriceRule, incoming: &Order, maker_price: Decimal) -> Decimal {
        match rule {
            MatchPriceRule::Maker => maker_price,
            MatchPriceRule::Taker => {
                if incoming.order_type == OrderType::Limit && incoming.price > Decimal::ZERO {
                    incoming.price
                } else {
                    maker_price
                }
            }
            MatchPriceRule::Midpoint => self.midpoint().unwrap_or(maker_price),
        }
    }

    fn validate_order(order: &Order) -> Result<()> {
        if order.qty <= Decimal::ZERO {
            return Err(MatchingError::InvalidQuantity);
        }
        if matches!(order.order_type, OrderType::Limit) && order.price <= Decimal::ZERO {
            return Err(MatchingError::InvalidPrice);
        }
        Ok(())
    }

    fn level_qty(book: &BTreeMap<Decimal, PriceLevel>, price: Decimal) -> Decimal {
        book.get(&price).map(|l| l.total_qty).unwrap_or(Decimal::ZERO)
    }

    fn push_level_delta(
        out: &mut Vec<EngineEventKind>,
        side: Side,
        price: Decimal,
        delta_qty: Decimal,
        new_qty: Decimal,
        reason: BookDeltaReason,
    ) {
        out.push(EngineEventKind::BookDelta {
            side,
            price,
            delta_qty,
            new_qty,
            reason,
        });
    }

    /// Aggregated L2 book snapshot (top N levels).
    pub fn snapshot(&self, depth: usize) -> BookSnapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(p, l)| BookLevel {
                price: *p,
                qty: l.total_qty,
            })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(p, l)| BookLevel {
                price: *p,
                qty: l.total_qty,
            })
            .collect();
        BookSnapshot { bids, asks }
    }

    /// Add an order to the book without matching.
    ///
    /// This is mainly used by tests and advanced workflows.
    pub fn add_order(&mut self, mut order: Order) -> Result<()> {
        Self::validate_order(&order)?;

        if order.order_id.is_empty() {
            order.order_id = self.next_order_id();
        }

        if self.orders.contains_key(&order.order_id) {
            return Err(MatchingError::OrderExists(order.order_id));
        }

        order.status = OrderStatus::Ack;
        order.leaves_qty = order.qty - order.cum_qty;

        let (book, arena) = self.side_book_and_arena_mut(order.side);
        let level = book.entry(order.price).or_insert_with(PriceLevel::default);
        let node_idx = level.push_back(arena, order.order_id.clone());
        level.total_qty += order.leaves_qty;

        let loc = OrderLoc {
            side: order.side,
            price: order.price,
            node_idx,
        };
        self.orders.insert(order.order_id.clone(), OrderEntry { order, loc });
        Ok(())
    }

    /// Add an order to the book without matching and return event kinds.
    ///
    /// This is intended for pre-open / auction collection phases.
    pub fn add_order_kinds(&mut self, mut order: Order) -> Result<Vec<EngineEventKind>> {
        Self::validate_order(&order)?;

        if order.order_id.is_empty() {
            order.order_id = self.next_order_id();
        }
        if self.orders.contains_key(&order.order_id) {
            return Err(MatchingError::OrderExists(order.order_id));
        }

        // Only resting orders are supported here.
        if order.order_type == OrderType::Market {
            return Err(MatchingError::InvalidOrder(
                "Market orders cannot be rested".to_string(),
            ));
        }

        order.cum_qty = Decimal::ZERO.max(order.cum_qty);
        order.leaves_qty = order.qty - order.cum_qty;
        order.status = OrderStatus::Ack;

        let mut out = Vec::new();
        out.push(EngineEventKind::Ack {
            order_id: order.order_id.clone(),
            client_order_id: order.client_order_id.clone(),
            status: order.status,
            leaves_qty: order.leaves_qty,
            cum_qty: order.cum_qty,
        });

        let (book, arena) = self.side_book_and_arena_mut(order.side);
        let before = Self::level_qty(book, order.price);
        let level = book.entry(order.price).or_insert_with(PriceLevel::default);
        let node_idx = level.push_back(arena, order.order_id.clone());
        level.total_qty += order.leaves_qty;
        let after = level.total_qty;
        let _ = before;
        Self::push_level_delta(
            &mut out,
            order.side,
            order.price,
            order.leaves_qty,
            after,
            BookDeltaReason::Add,
        );

        let loc = OrderLoc {
            side: order.side,
            price: order.price,
            node_idx,
        };
        self.orders.insert(order.order_id.clone(), OrderEntry { order, loc });
        Ok(out)
    }

    /// Cancel an existing resting order.
    pub fn cancel_order(&mut self, order_id: &str) -> Result<Order> {
        let entry = self
            .orders
            .remove(order_id)
            .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?;

        let mut canceled = entry.order.clone();

        let (book, arena) = self.side_book_and_arena_mut(entry.loc.side);
        let level = book
            .get_mut(&entry.loc.price)
            .expect("price level must exist for resting order");

        level.total_qty -= canceled.leaves_qty;
        level.remove(arena, entry.loc.node_idx);
        if level.is_empty() {
            book.remove(&entry.loc.price);
        }

        canceled.status = OrderStatus::Canceled;
        Ok(canceled)
    }

    /// Cancel a resting order and return event kinds (including book delta).
    pub fn cancel_order_kinds(&mut self, order_id: &str) -> Result<Vec<EngineEventKind>> {
        // Extract client info and resting location before removal.
        let entry = self
            .orders
            .get(order_id)
            .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?
            .clone();

        let before = match entry.loc.side {
            Side::Buy => Self::level_qty(&self.bids, entry.loc.price),
            Side::Sell => Self::level_qty(&self.asks, entry.loc.price),
        };

        let canceled = self.cancel_order(order_id)?;

        let after = match entry.loc.side {
            Side::Buy => Self::level_qty(&self.bids, entry.loc.price),
            Side::Sell => Self::level_qty(&self.asks, entry.loc.price),
        };
        let delta = after - before;

        let mut out = Vec::new();
        out.push(EngineEventKind::CancelAck {
            order_id: canceled.order_id.clone(),
            client_order_id: canceled.client_order_id.clone(),
            leaves_qty: canceled.leaves_qty,
            cum_qty: canceled.cum_qty,
        });
        Self::push_level_delta(
            &mut out,
            entry.loc.side,
            entry.loc.price,
            if delta == Decimal::ZERO { -canceled.leaves_qty } else { delta },
            after,
            BookDeltaReason::Cancel,
        );
        out.push(EngineEventKind::Done {
            order_id: canceled.order_id.clone(),
            client_order_id: canceled.client_order_id.clone(),
            reason: DoneReason::Canceled,
        });
        Ok(out)
    }

    /// Replace/amend a resting order.
    ///
    /// Rule:
    /// - Price change: loses priority (moved to back of new price level).
    /// - Quantity increase: loses priority (moved to back at same price).
    /// - Quantity decrease at same price: keeps priority (stays in place).
    pub fn replace_order(
        &mut self,
        order_id: &str,
        new_price: Decimal,
        new_qty: Decimal,
        timestamp_ns: i64,
    ) -> Result<Order> {
        if new_qty <= Decimal::ZERO {
            return Err(MatchingError::InvalidQuantity);
        }
        if new_price <= Decimal::ZERO {
            return Err(MatchingError::InvalidPrice);
        }

        let mut entry = self
            .orders
            .remove(order_id)
            .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?;

        if new_qty < entry.order.cum_qty {
            return Err(MatchingError::InvalidOrder(
                "new_qty cannot be less than cum_qty".to_string(),
            ));
        }

        let qty_increased = new_qty > entry.order.qty;
        let price_changed = new_price != entry.order.price;
        let loses_priority = qty_increased || price_changed;

        // Remove from current level.
        {
            let (book, arena) = self.side_book_and_arena_mut(entry.loc.side);
            let level = book
                .get_mut(&entry.loc.price)
                .expect("price level must exist for resting order");
            level.total_qty -= entry.order.leaves_qty;
            level.remove(arena, entry.loc.node_idx);
            if level.is_empty() {
                book.remove(&entry.loc.price);
            }
        }

        entry.order.price = new_price;
        entry.order.qty = new_qty;
        entry.order.timestamp_ns = timestamp_ns;
        entry.order.leaves_qty = new_qty - entry.order.cum_qty;
        entry.order.status = if entry.order.cum_qty > Decimal::ZERO {
            OrderStatus::Partial
        } else {
            OrderStatus::Ack
        };

        // Reinsert.
        let (book, arena) = self.side_book_and_arena_mut(entry.loc.side);
        let level = book.entry(new_price).or_insert_with(PriceLevel::default);
        let node_idx = if loses_priority {
            level.push_back(arena, entry.order.order_id.clone())
        } else {
            // Keep priority: we insert at front by creating a new node and linking it ahead.
            // This is deterministic and preserves the order's relative position at the top.
            let idx = arena.alloc(Node {
                prev: None,
                next: level.head,
                order_id: entry.order.order_id.clone(),
            });
            if let Some(head) = level.head {
                arena.get_mut(head).prev = Some(idx);
            } else {
                level.tail = Some(idx);
            }
            level.head = Some(idx);
            idx
        };
        level.total_qty += entry.order.leaves_qty;

        entry.loc = OrderLoc {
            side: entry.loc.side,
            price: new_price,
            node_idx,
        };

        let out = entry.order.clone();
        self.orders.insert(out.order_id.clone(), entry);
        Ok(out)
    }

    /// Replace/amend a resting order and return event kinds (including book deltas).
    pub fn replace_order_kinds(
        &mut self,
        order_id: &str,
        new_price: Decimal,
        new_qty: Decimal,
        timestamp_ns: i64,
    ) -> Result<Vec<EngineEventKind>> {
        let existing = self
            .orders
            .get(order_id)
            .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?
            .order
            .clone();
        let side = existing.side;
        let old_price = existing.price;

        let before_old = match side {
            Side::Buy => Self::level_qty(&self.bids, old_price),
            Side::Sell => Self::level_qty(&self.asks, old_price),
        };
        let before_new = match side {
            Side::Buy => Self::level_qty(&self.bids, new_price),
            Side::Sell => Self::level_qty(&self.asks, new_price),
        };

        let replaced = self.replace_order(order_id, new_price, new_qty, timestamp_ns)?;

        let after_old = match side {
            Side::Buy => Self::level_qty(&self.bids, old_price),
            Side::Sell => Self::level_qty(&self.asks, old_price),
        };
        let after_new = match side {
            Side::Buy => Self::level_qty(&self.bids, new_price),
            Side::Sell => Self::level_qty(&self.asks, new_price),
        };

        let mut out = Vec::new();
        out.push(EngineEventKind::ReplaceAck {
            order_id: replaced.order_id.clone(),
            client_order_id: replaced.client_order_id.clone(),
            new_price,
            new_qty,
            leaves_qty: replaced.leaves_qty,
            cum_qty: replaced.cum_qty,
        });

        // Old level delta.
        let delta_old = after_old - before_old;
        if delta_old != Decimal::ZERO || old_price == new_price {
            Self::push_level_delta(
                &mut out,
                side,
                old_price,
                delta_old,
                if old_price == new_price { after_new } else { after_old },
                BookDeltaReason::Replace,
            );
        }
        // New level delta if price changed.
        if new_price != old_price {
            let delta_new = after_new - before_new;
            if delta_new != Decimal::ZERO {
                Self::push_level_delta(
                    &mut out,
                    side,
                    new_price,
                    delta_new,
                    after_new,
                    BookDeltaReason::Replace,
                );
            }
        }

        Ok(out)
    }

    fn can_fully_fill(&self, incoming: &Order) -> bool {
        let mut remaining = incoming.leaves_qty;
        if remaining <= Decimal::ZERO {
            return true;
        }

        let opp = match incoming.side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };

        match incoming.side {
            Side::Buy => {
                for (price, level) in opp.iter() {
                    if matches!(incoming.order_type, OrderType::Limit) && *price > incoming.price {
                        break;
                    }
                    remaining -= level.total_qty;
                    if remaining <= Decimal::ZERO {
                        return true;
                    }
                }
            }
            Side::Sell => {
                for (price, level) in opp.iter().rev() {
                    if matches!(incoming.order_type, OrderType::Limit) && *price < incoming.price {
                        break;
                    }
                    remaining -= level.total_qty;
                    if remaining <= Decimal::ZERO {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Run an auction uncrossing pass on the current resting book.
    ///
    /// This is intended for open/close auctions. It selects a clearing price that:
    /// 1) Maximizes matched volume.
    /// 2) Minimizes absolute imbalance at the clearing price.
    /// 3) Minimizes distance to `reference_price` (if provided).
    ///
    /// All fills are emitted with `Liquidity::Auction` and `BookDeltaReason::Auction`.
    pub fn auction_uncross_kinds(&mut self, reference_price: Option<Decimal>) -> Result<Vec<EngineEventKind>> {
        let mut out: Vec<EngineEventKind> = Vec::new();

        if self.bids.is_empty() || self.asks.is_empty() {
            return Ok(out);
        }

        // Candidate prices are the union of bid and ask prices.
        let mut candidates: Vec<Decimal> = self
            .bids
            .keys()
            .chain(self.asks.keys())
            .cloned()
            .collect();
        candidates.sort();
        candidates.dedup();

        // Precompute cumulative buy qty at or above each candidate.
        let mut buy_at_or_above = vec![Decimal::ZERO; candidates.len()];
        {
            let mut cum = Decimal::ZERO;
            let mut it = self.bids.iter().rev().peekable();
            for (i, &p) in candidates.iter().enumerate().rev() {
                while let Some((&bp, lvl)) = it.peek() {
                    if bp >= p {
                        cum += lvl.total_qty;
                        it.next();
                    } else {
                        break;
                    }
                }
                buy_at_or_above[i] = cum;
            }
        }

        // Precompute cumulative sell qty at or below each candidate.
        let mut sell_at_or_below = vec![Decimal::ZERO; candidates.len()];
        {
            let mut cum = Decimal::ZERO;
            let mut it = self.asks.iter().peekable();
            for (i, &p) in candidates.iter().enumerate() {
                while let Some((&ap, lvl)) = it.peek() {
                    if ap <= p {
                        cum += lvl.total_qty;
                        it.next();
                    } else {
                        break;
                    }
                }
                sell_at_or_below[i] = cum;
            }
        }

        // Select clearing price by the deterministic tie-break rules.
        let mut best_idx: Option<usize> = None;
        for i in 0..candidates.len() {
            let buy = buy_at_or_above[i];
            let sell = sell_at_or_below[i];
            let matched = buy.min(sell);
            let imbalance = (buy - sell).abs();

            let better = match best_idx {
                None => true,
                Some(j) => {
                    let best_buy = buy_at_or_above[j];
                    let best_sell = sell_at_or_below[j];
                    let best_matched = best_buy.min(best_sell);
                    let best_imbalance = (best_buy - best_sell).abs();
                    if matched > best_matched {
                        true
                    } else if matched < best_matched {
                        false
                    } else if imbalance < best_imbalance {
                        true
                    } else if imbalance > best_imbalance {
                        false
                    } else if let Some(r) = reference_price {
                        let d = (candidates[i] - r).abs();
                        let best_d = (candidates[j] - r).abs();
                        if d < best_d {
                            true
                        } else if d > best_d {
                            false
                        } else {
                            // Final deterministic tie-break: choose the lower price.
                            candidates[i] < candidates[j]
                        }
                    } else {
                        // Final deterministic tie-break: choose the lower price.
                        candidates[i] < candidates[j]
                    }
                }
            };

            if better {
                best_idx = Some(i);
            }
        }

        let Some(idx) = best_idx else { return Ok(out) };
        let clearing_price = candidates[idx];
        let max_matched = buy_at_or_above[idx].min(sell_at_or_below[idx]);
        if max_matched <= Decimal::ZERO {
            return Ok(out);
        }

        // Execute auction matches at the clearing price.
        loop {
            let best_bid = self.bids.keys().next_back().cloned();
            let best_ask = self.asks.keys().next().cloned();
            let (Some(bid_p), Some(ask_p)) = (best_bid, best_ask) else { break };
            if bid_p < clearing_price || ask_p > clearing_price {
                break;
            }

            let bid_head = self
                .bids
                .get(&bid_p)
                .and_then(|lvl| lvl.peek_front())
                .expect("non-empty bid level must have head");
            let ask_head = self
                .asks
                .get(&ask_p)
                .and_then(|lvl| lvl.peek_front())
                .expect("non-empty ask level must have head");

            let bid_order_id = self.arena.get(bid_head).order_id.clone();
            let ask_order_id = self.arena.get(ask_head).order_id.clone();

            let trade_id = self.next_trade_id();

            // Compute trade quantity without holding mutable borrows.
            let (bid_available, ask_available) = {
                let bid_e = self.orders.get(&bid_order_id).expect("bid must exist");
                let ask_e = self.orders.get(&ask_order_id).expect("ask must exist");
                (bid_e.order.leaves_qty, ask_e.order.leaves_qty)
            };
            let trade_qty = bid_available.min(ask_available);

            // Update both orders (tight scopes).
            let (bid_loc, bid_client, bid_leaves, bid_cum, bid_filled) = {
                let bid_entry = self.orders.get_mut(&bid_order_id).expect("bid must exist");
                bid_entry.order.cum_qty += trade_qty;
                bid_entry.order.leaves_qty -= trade_qty;
                bid_entry.order.status = if bid_entry.order.leaves_qty > Decimal::ZERO {
                    OrderStatus::Partial
                } else {
                    OrderStatus::Filled
                };
                (
                    bid_entry.loc,
                    bid_entry.order.client_order_id.clone(),
                    bid_entry.order.leaves_qty,
                    bid_entry.order.cum_qty,
                    bid_entry.order.status == OrderStatus::Filled,
                )
            };
            let (ask_loc, ask_client, ask_leaves, ask_cum, ask_filled) = {
                let ask_entry = self.orders.get_mut(&ask_order_id).expect("ask must exist");
                ask_entry.order.cum_qty += trade_qty;
                ask_entry.order.leaves_qty -= trade_qty;
                ask_entry.order.status = if ask_entry.order.leaves_qty > Decimal::ZERO {
                    OrderStatus::Partial
                } else {
                    OrderStatus::Filled
                };
                (
                    ask_entry.loc,
                    ask_entry.order.client_order_id.clone(),
                    ask_entry.order.leaves_qty,
                    ask_entry.order.cum_qty,
                    ask_entry.order.status == OrderStatus::Filled,
                )
            };

            // Update book totals + deltas.
            {
                let (book, _arena) = self.side_book_and_arena_mut(Side::Buy);
                let level = book.get_mut(&bid_loc.price).expect("bid level must exist");
                level.total_qty -= trade_qty;
                Self::push_level_delta(
                    &mut out,
                    Side::Buy,
                    bid_loc.price,
                    -trade_qty,
                    level.total_qty,
                    BookDeltaReason::Auction,
                );
            }
            {
                let (book, _arena) = self.side_book_and_arena_mut(Side::Sell);
                let level = book.get_mut(&ask_loc.price).expect("ask level must exist");
                level.total_qty -= trade_qty;
                Self::push_level_delta(
                    &mut out,
                    Side::Sell,
                    ask_loc.price,
                    -trade_qty,
                    level.total_qty,
                    BookDeltaReason::Auction,
                );
            }

            // Emit fills.
            out.push(EngineEventKind::Fill {
                trade_id: trade_id.clone(),
                order_id: bid_order_id.clone(),
                client_order_id: bid_client.clone(),
                contra_order_id: ask_order_id.clone(),
                side: Side::Buy,
                liquidity: Liquidity::Auction,
                price: clearing_price,
                qty: trade_qty,
                leaves_qty: bid_leaves,
                cum_qty: bid_cum,
            });
            out.push(EngineEventKind::Fill {
                trade_id: trade_id.clone(),
                order_id: ask_order_id.clone(),
                client_order_id: ask_client.clone(),
                contra_order_id: bid_order_id.clone(),
                side: Side::Sell,
                liquidity: Liquidity::Auction,
                price: clearing_price,
                qty: trade_qty,
                leaves_qty: ask_leaves,
                cum_qty: ask_cum,
            });

            self.last_trade_price = Some(clearing_price);

            // Remove filled orders and emit Done.
            if bid_filled {
                let _ = self.orders.remove(&bid_order_id).expect("must exist");
                let (book, arena) = self.side_book_and_arena_mut(Side::Buy);
                let level = book.get_mut(&bid_loc.price).expect("bid level must exist");
                level.remove(arena, bid_loc.node_idx);
                if level.is_empty() {
                    book.remove(&bid_loc.price);
                }
                out.push(EngineEventKind::Done {
                    order_id: bid_order_id.clone(),
                    client_order_id: bid_client.clone(),
                    reason: DoneReason::Filled,
                });
            }
            if ask_filled {
                let _ = self.orders.remove(&ask_order_id).expect("must exist");
                let (book, arena) = self.side_book_and_arena_mut(Side::Sell);
                let level = book.get_mut(&ask_loc.price).expect("ask level must exist");
                level.remove(arena, ask_loc.node_idx);
                if level.is_empty() {
                    book.remove(&ask_loc.price);
                }
                out.push(EngineEventKind::Done {
                    order_id: ask_order_id.clone(),
                    client_order_id: ask_client.clone(),
                    reason: DoneReason::Filled,
                });
            }
        }

        Ok(out)
    }

    /// Process an order and return an execution-style event stream (without sequence numbers).
    ///
    /// The engine wraps these kinds into `EngineEvent` with sequence numbers.
    pub fn match_order_kinds(&mut self, incoming: Order) -> Result<Vec<EngineEventKind>> {
        self.match_order_kinds_with_price_rule(incoming, MatchPriceRule::Maker)
    }

    /// Process an order using a configurable trade price rule.
    pub fn match_order_kinds_with_price_rule(
        &mut self,
        mut incoming: Order,
        price_rule: MatchPriceRule,
    ) -> Result<Vec<EngineEventKind>> {
        Self::validate_order(&incoming)?;

        if incoming.order_id.is_empty() {
            incoming.order_id = self.next_order_id();
        }

        // Normalize bookkeeping fields.
        incoming.cum_qty = Decimal::ZERO.max(incoming.cum_qty);
        incoming.leaves_qty = incoming.qty - incoming.cum_qty;
        incoming.status = OrderStatus::Ack;

        let mut out: Vec<EngineEventKind> = Vec::new();
        out.push(EngineEventKind::Ack {
            order_id: incoming.order_id.clone(),
            client_order_id: incoming.client_order_id.clone(),
            status: incoming.status,
            leaves_qty: incoming.leaves_qty,
            cum_qty: incoming.cum_qty,
        });

        // FOK must be all-or-nothing: pre-check book depth at eligible prices.
        if incoming.time_in_force == TimeInForce::FOK && !self.can_fully_fill(&incoming) {
            incoming.status = OrderStatus::Canceled;
            out.push(EngineEventKind::Done {
                order_id: incoming.order_id.clone(),
                client_order_id: incoming.client_order_id.clone(),
                reason: DoneReason::Canceled,
            });
            return Ok(out);
        }

        let mut remaining = incoming.leaves_qty;

        // Match against the opposite book.
        while remaining > Decimal::ZERO {
            let best_opp_price = match incoming.side {
                Side::Buy => self.asks.keys().next().cloned(),
                Side::Sell => self.bids.keys().next_back().cloned(),
            };
            let Some(best_price) = best_opp_price else { break };
            if !self.crosses(&incoming, best_price) {
                break;
            }

            // Identify best maker order at the best price.
            let maker_side = match incoming.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let maker_level_head = {
                let opp_book = match maker_side {
                    Side::Buy => &self.bids,
                    Side::Sell => &self.asks,
                };
                let level = opp_book
                    .get(&best_price)
                    .expect("best price level must exist");
                level.peek_front().expect("non-empty level must have head")
            };
            let maker_order_id = self.arena.get(maker_level_head).order_id.clone();

            // Allocate trade id before borrowing maker entry.
            let trade_id = self.next_trade_id();

            // Update maker inside a tight scope and extract immutable snapshots for events.
            let (trade_qty, maker_price, maker_loc, maker_side, maker_client_order_id, maker_order_id_snapshot, maker_leaves_qty, maker_cum_qty, maker_filled) =
            {
                let maker_entry = self
                    .orders
                    .get_mut(&maker_order_id)
                    .expect("maker order must exist");

                let trade_qty = remaining.min(maker_entry.order.leaves_qty);
                let maker_price = maker_entry.order.price;

                maker_entry.order.cum_qty += trade_qty;
                maker_entry.order.leaves_qty -= trade_qty;
                maker_entry.order.status = if maker_entry.order.leaves_qty > Decimal::ZERO {
                    OrderStatus::Partial
                } else {
                    OrderStatus::Filled
                };

                (
                    trade_qty,
                    maker_price,
                    maker_entry.loc,
                    maker_entry.order.side,
                    maker_entry.order.client_order_id.clone(),
                    maker_entry.order.order_id.clone(),
                    maker_entry.order.leaves_qty,
                    maker_entry.order.cum_qty,
                    maker_entry.order.status == OrderStatus::Filled,
                )
            };

            let trade_price = self.compute_trade_price(price_rule, &incoming, maker_price);

            // Update maker price level total qty + emit L2 book delta.
            {
                let (maker_book, _arena) = self.side_book_and_arena_mut(maker_loc.side);
                let before = Self::level_qty(maker_book, maker_loc.price);
                let level = maker_book
                    .get_mut(&maker_loc.price)
                    .expect("maker price level must exist");
                level.total_qty -= trade_qty;
                let after = level.total_qty;
                let _ = before;
                Self::push_level_delta(
                    &mut out,
                    maker_loc.side,
                    maker_loc.price,
                    -trade_qty,
                    after,
                    BookDeltaReason::Fill,
                );
            }

            // Update incoming.
            incoming.cum_qty += trade_qty;
            remaining -= trade_qty;
            incoming.leaves_qty = remaining;
            incoming.status = if remaining > Decimal::ZERO {
                OrderStatus::Partial
            } else {
                OrderStatus::Filled
            };

            // Emit fills for both sides (maker + taker).
            out.push(EngineEventKind::Fill {
                trade_id: trade_id.clone(),
                order_id: incoming.order_id.clone(),
                client_order_id: incoming.client_order_id.clone(),
                contra_order_id: maker_order_id_snapshot.clone(),
                side: incoming.side,
                liquidity: Liquidity::Taker,
                price: trade_price,
                qty: trade_qty,
                leaves_qty: incoming.leaves_qty,
                cum_qty: incoming.cum_qty,
            });
            out.push(EngineEventKind::Fill {
                trade_id: trade_id.clone(),
                order_id: maker_order_id_snapshot.clone(),
                client_order_id: maker_client_order_id.clone(),
                contra_order_id: incoming.order_id.clone(),
                side: maker_side,
                liquidity: Liquidity::Maker,
                price: trade_price,
                qty: trade_qty,
                leaves_qty: maker_leaves_qty,
                cum_qty: maker_cum_qty,
            });

            self.last_trade_price = Some(trade_price);

            // If maker filled, remove from book/map.
            if maker_filled {
                // Remove from map first.
                let _filled = self.orders.remove(&maker_order_id).expect("must exist");

                let (book, arena) = self.side_book_and_arena_mut(maker_loc.side);
                let level = book
                    .get_mut(&maker_loc.price)
                    .expect("maker price level must exist");
                level.remove(arena, maker_loc.node_idx);
                if level.is_empty() {
                    book.remove(&maker_loc.price);
                }

                out.push(EngineEventKind::Done {
                    order_id: maker_order_id_snapshot.clone(),
                    client_order_id: maker_client_order_id.clone(),
                    reason: DoneReason::Filled,
                });
            }

            // IOC: stop after immediate matching; remainder is canceled.
            if incoming.time_in_force == TimeInForce::IOC {
                break;
            }
        }

        // Decide whether to rest remainder.
        let is_market = incoming.order_type == OrderType::Market;
        let can_rest = matches!(incoming.time_in_force, TimeInForce::GTC | TimeInForce::Day)
            && !is_market
            && incoming.leaves_qty > Decimal::ZERO;

        if can_rest {
            // Rest remainder at tail (price-time priority).
            let (book, arena) = self.side_book_and_arena_mut(incoming.side);
            let before = Self::level_qty(book, incoming.price);
            let level = book.entry(incoming.price).or_insert_with(PriceLevel::default);
            let node_idx = level.push_back(arena, incoming.order_id.clone());
            level.total_qty += incoming.leaves_qty;
            let after = level.total_qty;
            let _ = before;
            Self::push_level_delta(
                &mut out,
                incoming.side,
                incoming.price,
                incoming.leaves_qty,
                after,
                BookDeltaReason::Add,
            );

            let loc = OrderLoc {
                side: incoming.side,
                price: incoming.price,
                node_idx,
            };
            self.orders.insert(
                incoming.order_id.clone(),
                OrderEntry {
                    order: incoming.clone(),
                    loc,
                },
            );
        } else {
            // Not resting remainder: mark canceled if not fully filled.
            if incoming.leaves_qty > Decimal::ZERO {
                incoming.status = OrderStatus::Canceled;
            }
        }

        let done_reason = if incoming.status == OrderStatus::Filled {
            DoneReason::Filled
        } else if incoming.status == OrderStatus::Canceled {
            DoneReason::Canceled
        } else {
            // Resting (Ack/Partial) orders are not terminal.
            // Do not emit Done in this case.
            return Ok(out);
        };

        out.push(EngineEventKind::Done {
            order_id: incoming.order_id.clone(),
            client_order_id: incoming.client_order_id.clone(),
            reason: done_reason,
        });
        Ok(out)
    }

    /// Match an order and return incoming-side trades (compatibility API).
    ///
    /// This returns the taker-side fills only, preserving the old behavior of
    /// `MatchingEngine::submit_order() -> Vec<Trade>`.
    pub fn match_order(&mut self, incoming: Order) -> Vec<Trade> {
        let timestamp_ns = incoming.timestamp_ns;
        let kinds = match self.match_order_kinds(incoming) {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };

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
                if liquidity == Liquidity::Taker {
                    trades.push(Trade {
                        trade_id,
                        order_id,
                        client_order_id,
                        contra_order_id: Some(contra_order_id),
                        symbol: self.symbol.clone(),
                        side,
                        price,
                        qty,
                        timestamp_ns,
                    });
                }
            }
        }
        trades
    }

    /// Get the best bid order (highest buy price).
    pub fn get_best_bid(&self) -> Option<&Order> {
        let best_price = self.bids.keys().next_back().cloned()?;
        let level = self.bids.get(&best_price)?;
        let head = level.peek_front()?;
        let id = self.arena.get(head).order_id.clone();
        self.orders.get(&id).map(|e| &e.order)
    }

    /// Get the best ask order (lowest sell price).
    pub fn get_best_ask(&self) -> Option<&Order> {
        let best_price = self.asks.keys().next().cloned()?;
        let level = self.asks.get(&best_price)?;
        let head = level.peek_front()?;
        let id = self.arena.get(head).order_id.clone();
        self.orders.get(&id).map(|e| &e.order)
    }

    /// Get an order by ID (only if it is currently resting).
    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id).map(|e| &e.order)
    }

    /// Get the last trade price.
    pub fn get_last_trade_price(&self) -> Option<Decimal> {
        self.last_trade_price
    }

    /// Get market data snapshot (top of book).
    pub fn get_market_data(&self) -> MarketData {
        let best_bid_price = self.bids.keys().next_back().cloned();
        let best_ask_price = self.asks.keys().next().cloned();
        let best_bid_qty = best_bid_price
            .as_ref()
            .and_then(|p| self.bids.get(p))
            .map(|l| l.total_qty);
        let best_ask_qty = best_ask_price
            .as_ref()
            .and_then(|p| self.asks.get(p))
            .map(|l| l.total_qty);

        MarketData {
            symbol: self.symbol.clone(),
            best_bid: best_bid_price,
            best_bid_qty,
            best_ask: best_ask_price,
            best_ask_qty,
            last_trade_price: self.last_trade_price,
        }
    }

    /// Get the symbol for this order book.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_order() {
        let mut book = OrderBook::new("AAPL".to_string());
        let order = Order::new_with_account(
            "client_1".to_string(),
            "acct".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            1000,
        );
        assert!(book.add_order(order).is_ok());
    }

    #[test]
    fn test_price_time_priority() {
        let mut book = OrderBook::new("AAPL".to_string());

        // Add buy orders with different prices.
        let order1 = Order::new_with_account(
            "client_1".to_string(),
            "acct".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            1000,
        );
        let order2 = Order::new_with_account(
            "client_2".to_string(),
            "acct".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10100, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            2000,
        );

        book.add_order(order1).unwrap();
        book.add_order(order2).unwrap();

        let best_bid = book.get_best_bid().unwrap();
        assert_eq!(best_bid.price, Decimal::new(10100, 2));
    }

    #[test]
    fn test_matching_limit_orders() {
        let mut book = OrderBook::new("AAPL".to_string());

        // Rest sell at 100.00
        let sell_order = Order::new_with_account(
            "client_sell".to_string(),
            "acct_s".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            1000,
        );
        book.add_order(sell_order).unwrap();

        // Incoming buy at 101.00
        let buy_order = Order::new_with_account(
            "client_buy".to_string(),
            "acct_b".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10100, 2),
            Decimal::new(100, 0),
            TimeInForce::GTC,
            2000,
        );

        let trades = book.match_order(buy_order);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].qty, Decimal::new(100, 0));
        assert_eq!(trades[0].price, Decimal::new(10000, 2));
    }

    #[test]
    fn test_fok_does_not_partially_consume_book() {
        let mut book = OrderBook::new("AAPL".to_string());

        let sell_order = Order::new_with_account(
            "client_sell".to_string(),
            "acct_s".to_string(),
            "AAPL".to_string(),
            Side::Sell,
            OrderType::Limit,
            Decimal::new(10000, 2),
            Decimal::new(50, 0),
            TimeInForce::GTC,
            1000,
        );
        book.add_order(sell_order).unwrap();

        let fok_buy = Order::new_with_account(
            "client_fok".to_string(),
            "acct_b".to_string(),
            "AAPL".to_string(),
            Side::Buy,
            OrderType::Limit,
            Decimal::new(10100, 2),
            Decimal::new(100, 0),
            TimeInForce::FOK,
            2000,
        );

        let kinds = book.match_order_kinds(fok_buy).unwrap();
        assert!(kinds.iter().all(|k| !matches!(k, EngineEventKind::Fill { .. })));

        // The original sell should still be resting.
        let best_ask = book.get_best_ask().unwrap();
        assert_eq!(best_ask.leaves_qty, Decimal::new(50, 0));
    }
}
