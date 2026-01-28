//! L3 Order Book with Queue Position Tracking
//!
//! Maintains individual order queue positions, enabling:
//! - Precise queue position tracking
//! - Queue position-based matching
//! - Hidden order support
//! - Post-only order validation

use std::collections::{HashMap, VecDeque};
use rust_decimal::Decimal;

use crate::types::{Order, Trade, Side, OrderComparable, OrderStatus, OrderType, TimeInForce};
use crate::error::{Result, MatchingError};
use crate::order_types::{HiddenOrder, PostOnlyOrder};

/// Visible order book representation: (bids, asks), each as `(price, qty)` levels.
pub type VisibleOrderBook = (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>);

/// Final outcome for a resting order's queue journey.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePositionOutcome {
    Filled,
    Canceled,
    Replaced,
}

/// Queue position telemetry event.
#[derive(Debug, Clone)]
pub struct QueuePositionEvent {
    pub order_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    /// Queue position at the time the order was enqueued (0 = front).
    pub enqueued_position: usize,
    /// Queue position at the time the order left the book (0 = front).
    pub exit_position: usize,
    /// Event time spent resting on the book (ns).
    pub wait_time_ns: i64,
    pub outcome: QueuePositionOutcome,
}

/// Controls how an incoming order's remaining quantity (if any) is rested on the book.
#[derive(Debug, Clone, Copy)]
enum RestingMode {
    /// Rest remaining quantity as a normal visible order.
    Visible,
    /// Rest remaining quantity as a hidden order with an optional display quantity.
    Hidden { display_qty: Option<Decimal> },
    /// Do not rest remaining quantity on the book.
    #[allow(dead_code)]
    None,
}

/// Order entry in L3 order book with queue position
#[derive(Debug, Clone)]
struct OrderEntry {
    /// The order
    order: Order,
    /// Queue position (0 = front of queue)
    queue_position: usize,
    /// Whether order is hidden
    is_hidden: bool,
    /// Display quantity (for hidden orders)
    display_qty: Option<Decimal>,
}

/// L3 Order Book with individual order queue positions
///
/// Unlike L2 which aggregates orders by price level, L3 maintains
/// individual orders and their exact queue positions.
pub struct OrderBookL3 {
    /// Trading symbol
    symbol: String,
    /// Buy orders (bids) - ordered by price-time priority
    bids: VecDeque<OrderEntry>,
    /// Sell orders (asks) - ordered by price-time priority
    asks: VecDeque<OrderEntry>,
    /// Map of order_id to order entry for quick lookup
    orders: HashMap<String, OrderEntry>,
    /// Last trade price
    last_trade_price: Option<Decimal>,
    /// Trade counter
    trade_counter: u64,
    /// Deterministic order id counter (used only when caller does not provide order_id).
    order_id_counter: u64,
    /// Resting order telemetry: order_id -> (enqueued_ts_ns, enqueued_position).
    resting_info: HashMap<String, (i64, usize)>,
    /// Buffered queue position events (drainable).
    queue_events: Vec<QueuePositionEvent>,
}

impl OrderBookL3 {
    /// Create a new L3 order book
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: VecDeque::new(),
            asks: VecDeque::new(),
            orders: HashMap::new(),
            last_trade_price: None,
            trade_counter: 0,
            order_id_counter: 0,
            resting_info: HashMap::new(),
            queue_events: Vec::new(),
        }
    }

    fn next_order_id(&mut self) -> String {
        self.order_id_counter += 1;
        format!("ORDER_{}", self.order_id_counter)
    }

    fn next_trade_id(&mut self) -> String {
        self.trade_counter += 1;
        format!("TRADE_{}", self.trade_counter)
    }

    /// Drain accumulated queue position telemetry events.
    pub fn drain_queue_position_events(&mut self) -> Vec<QueuePositionEvent> {
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.queue_events);
        out
    }

    /// Add an order to the book with queue position tracking
    ///
    /// Returns the queue position of the added order.
    pub fn add_order(&mut self, mut order: Order) -> Result<usize> {
        // Generate order_id if not set
        if order.order_id.is_empty() {
            order.order_id = self.next_order_id();
        }

        if self.orders.contains_key(&order.order_id) {
            return Err(MatchingError::OrderExists(order.order_id));
        }

        // Validate order
        if order.qty <= Decimal::ZERO {
            return Err(MatchingError::InvalidQuantity);
        }
        if matches!(order.order_type, OrderType::Limit) && order.price <= Decimal::ZERO {
            return Err(MatchingError::InvalidPrice);
        }

        // Find insertion point based on price-time priority
        let queue_position = self._find_insertion_position(&order);

        // Create order entry
        let entry = OrderEntry {
            order: order.clone(),
            queue_position,
            is_hidden: false,
            display_qty: None,
        };

        // Insert at correct position
        match order.side {
            Side::Buy => {
                self.bids.insert(queue_position, entry.clone());
            }
            Side::Sell => {
                self.asks.insert(queue_position, entry.clone());
            }
        }

        // Update queue positions for ALL elements (from insertion position onwards)
        // After insertion, all elements from insertion position onwards need position update
        // This includes the newly inserted element and all elements after it
        self._update_queue_positions(order.side, queue_position);

        // Store order
        self.orders.insert(order.order_id.clone(), entry.clone());

        // Record enqueue telemetry (event-time).
        self.resting_info.insert(order.order_id.clone(), (order.timestamp_ns, queue_position));

        // Return the final queue position (which is the insertion position)
        Ok(queue_position)
    }

    /// Add a hidden order
    pub fn add_hidden_order(&mut self, hidden: HiddenOrder) -> Result<usize> {
        let mut order = hidden.order.clone();
        
        // Generate order_id if not set
        if order.order_id.is_empty() {
            order.order_id = self.next_order_id();
        }

        // Find insertion position (hidden orders still follow price-time priority)
        let queue_position = self._find_insertion_position(&order);

        // Create order entry with hidden flag
        let entry = OrderEntry {
            order: order.clone(),
            queue_position,
            is_hidden: true,
            display_qty: hidden.display_qty,
        };

        // Insert at correct position
        match order.side {
            Side::Buy => {
                self.bids.insert(queue_position, entry.clone());
            }
            Side::Sell => {
                self.asks.insert(queue_position, entry.clone());
            }
        }

        // Update queue positions for elements after insertion
        // After insertion, elements shift, so we update from insertion position + 1
        self._update_queue_positions(order.side, queue_position + 1);

        // Store order
        self.orders.insert(order.order_id.clone(), entry.clone());

        // Record enqueue telemetry (event-time).
        self.resting_info.insert(order.order_id.clone(), (order.timestamp_ns, queue_position));

        Ok(queue_position)
    }

    /// Add a post-only order
    ///
    /// Returns error if order would immediately match.
    pub fn add_post_only_order(&mut self, post_only: PostOnlyOrder) -> Result<usize> {
        let order = post_only.order.clone();

        // Check if order would immediately match
        if self._would_immediately_match(&order) {
            return Err(MatchingError::InvalidOrder(
                "Post-only order would immediately match".to_string(),
            ));
        }

        // Add as regular order
        self.add_order(order)
    }

    /// Get queue position for an order
    pub fn get_queue_position(&self, order_id: &str) -> Option<usize> {
        self.orders.get(order_id).map(|e| e.queue_position)
    }

    /// Get visible order book (excluding hidden orders or showing display_qty)
    pub fn get_visible_orderbook(&self, max_levels: usize) -> VisibleOrderBook {
        // Aggregate visible bids
        let mut price_levels: HashMap<Decimal, Decimal> = HashMap::new();
        for entry in self.bids.iter().take(max_levels * 10) {
            // Skip fully hidden orders
            if entry.is_hidden && entry.display_qty.is_none() {
                continue;
            }

            let visible_qty = if entry.is_hidden {
                // For hidden orders, visible quantity is min(leaves_qty, display_qty)
                let display = entry.display_qty.unwrap_or(Decimal::ZERO);
                entry.order.leaves_qty.min(display)
            } else {
                entry.order.leaves_qty
            };

            if visible_qty > Decimal::ZERO {
                *price_levels.entry(entry.order.price).or_insert(Decimal::ZERO) += visible_qty;
            }
        }

        // Sort by price (descending for bids)
        let mut bid_levels: Vec<_> = price_levels.into_iter().collect();
        bid_levels.sort_by(|a, b| b.0.cmp(&a.0));
        let bids: Vec<_> = bid_levels.into_iter().take(max_levels).collect();

        // Aggregate visible asks
        let mut price_levels: HashMap<Decimal, Decimal> = HashMap::new();
        for entry in self.asks.iter().take(max_levels * 10) {
            if entry.is_hidden && entry.display_qty.is_none() {
                continue;
            }

            let visible_qty = if entry.is_hidden {
                // For hidden orders, visible quantity is min(leaves_qty, display_qty)
                let display = entry.display_qty.unwrap_or(Decimal::ZERO);
                entry.order.leaves_qty.min(display)
            } else {
                entry.order.leaves_qty
            };

            if visible_qty > Decimal::ZERO {
                *price_levels.entry(entry.order.price).or_insert(Decimal::ZERO) += visible_qty;
            }
        }

        // Sort by price (ascending for asks)
        let mut ask_levels: Vec<_> = price_levels.into_iter().collect();
        ask_levels.sort_by(|a, b| a.0.cmp(&b.0));
        let asks: Vec<_> = ask_levels.into_iter().take(max_levels).collect();

        (bids, asks)
    }

    /// Cancel an order
    pub fn cancel_order(&mut self, order_id: &str) -> Result<Order> {
        self.remove_resting_order(order_id, QueuePositionOutcome::Canceled)
    }

    fn remove_resting_order(&mut self, order_id: &str, outcome: QueuePositionOutcome) -> Result<Order> {
        // Clone necessary data before borrowing.
        let (side, queue_pos, mut removed_order) = {
            let entry = self
                .orders
                .get(order_id)
                .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?;
            (entry.order.side, entry.queue_position, entry.order.clone())
        };

        // Remove from queue.
        match side {
            Side::Buy => {
                self.bids.remove(queue_pos);
            }
            Side::Sell => {
                self.asks.remove(queue_pos);
            }
        }

        // Update queue positions for remaining orders (start from removed position).
        self._update_queue_positions(side, queue_pos);

        // Remove from map.
        removed_order.status = match outcome {
            QueuePositionOutcome::Filled => OrderStatus::Filled,
            QueuePositionOutcome::Canceled => OrderStatus::Canceled,
            QueuePositionOutcome::Replaced => OrderStatus::Canceled,
        };
        self.orders.remove(order_id);

        // Emit queue position telemetry.
        if let Some((enq_ts, enq_pos)) = self.resting_info.remove(order_id) {
            let exit_pos = queue_pos;
            let wait_time_ns = removed_order.timestamp_ns.saturating_sub(enq_ts);
            self.queue_events.push(QueuePositionEvent {
                order_id: removed_order.order_id.clone(),
                client_order_id: removed_order.client_order_id.clone(),
                symbol: removed_order.symbol.clone(),
                side: removed_order.side,
                price: removed_order.price,
                enqueued_position: enq_pos,
                exit_position: exit_pos,
                wait_time_ns,
                outcome,
            });
        }

        Ok(removed_order)
    }

    /// Cancel/replace (amend) a resting order.
    ///
    /// Rule: replace loses time priority (treated as cancel + new at the new price).
    pub fn replace_order(
        &mut self,
        order_id: &str,
        new_price: Decimal,
        new_qty: Decimal,
        timestamp_ns: i64,
    ) -> Result<usize> {
        if new_qty <= Decimal::ZERO {
            return Err(MatchingError::InvalidQuantity);
        }
        if new_price <= Decimal::ZERO {
            return Err(MatchingError::InvalidPrice);
        }

        let existing = self
            .orders
            .get(order_id)
            .ok_or_else(|| MatchingError::OrderNotFound(order_id.to_string()))?
            .order
            .clone();

        // Remove the old resting order, marking the queue outcome as Replaced.
        let mut canceled = self.remove_resting_order(order_id, QueuePositionOutcome::Replaced)?;

        // Preserve identity fields; update economic fields.
        canceled.price = new_price;
        canceled.qty = new_qty;
        canceled.timestamp_ns = timestamp_ns;

        if new_qty < canceled.cum_qty {
            return Err(MatchingError::InvalidOrder(
                "replace new_qty cannot be less than cum_qty".to_string(),
            ));
        }
        canceled.leaves_qty = new_qty - canceled.cum_qty;
        canceled.status = if canceled.cum_qty > Decimal::ZERO {
            OrderStatus::Partial
        } else {
            OrderStatus::Ack
        };

        // Important: keep the same order_id / client_order_id.
        canceled.order_id = existing.order_id;
        canceled.client_order_id = existing.client_order_id;
        canceled.account_id = existing.account_id;
        canceled.symbol = existing.symbol;
        canceled.side = existing.side;
        canceled.order_type = existing.order_type;
        canceled.time_in_force = existing.time_in_force;

        self.add_order(canceled)
    }

    /// Match an incoming order
    pub fn match_order(&mut self, incoming: Order) -> Vec<Trade> {
        self.match_order_internal(incoming, RestingMode::Visible)
    }

    /// Match an incoming hidden order.
    ///
    /// Hidden orders participate in matching, but any unfilled remainder will rest as hidden
    /// (and only its `display_qty` contributes to the visible order book).
    pub fn match_hidden_order(&mut self, hidden: HiddenOrder) -> Vec<Trade> {
        self.match_order_internal(hidden.order, RestingMode::Hidden { display_qty: hidden.display_qty })
    }

    fn match_order_internal(&mut self, mut incoming: Order, resting: RestingMode) -> Vec<Trade> {
        let mut trades = Vec::new();
        let mut remaining_qty = incoming.leaves_qty;

        // Generate order_id if not set
        if incoming.order_id.is_empty() {
            incoming.order_id = self.next_order_id();
        }

        match incoming.side {
            Side::Buy => {
                // Match against asks
                while remaining_qty > Decimal::ZERO && !self.asks.is_empty() {
                    // Get best ask (front of queue)
                    if let Some(mut ask_entry) = self.asks.pop_front() {
                        // Check if price matches
                        match incoming.order_type {
                            OrderType::Limit => {
                                if incoming.price < ask_entry.order.price {
                                    // Price too low, put back and stop
                                    self.asks.push_front(ask_entry);
                                    break;
                                }
                            }
                            OrderType::Market => {
                                // Market orders match at any price
                            }
                            _ => break,
                        }

                        // Determine trade price
                        let trade_price = match incoming.order_type {
                            OrderType::Limit => {
                                incoming.price.min(ask_entry.order.price)
                            }
                            OrderType::Market => {
                                ask_entry.order.price
                            }
                            _ => ask_entry.order.price,
                        };

                        // Determine trade quantity
                        let trade_qty = remaining_qty.min(ask_entry.order.leaves_qty);

                        // Create trade (deterministic id)
                        let trade_id = self.next_trade_id();
                        let trade = Trade {
                            trade_id,
                            order_id: incoming.order_id.clone(),
                            client_order_id: incoming.client_order_id.clone(),
                            contra_order_id: Some(ask_entry.order.order_id.clone()),
                            symbol: incoming.symbol.clone(),
                            side: Side::Buy,
                            price: trade_price,
                            qty: trade_qty,
                            timestamp_ns: incoming.timestamp_ns,
                        };
                        trades.push(trade.clone());

                        // Update ask order
                        ask_entry.order.cum_qty += trade_qty;
                        ask_entry.order.leaves_qty -= trade_qty;

                        if ask_entry.order.leaves_qty > Decimal::ZERO {
                            ask_entry.order.status = OrderStatus::Partial;
                            // Put back at front (maintains priority)
                            ask_entry.queue_position = 0;
                            if let Some(entry) = self.orders.get_mut(&ask_entry.order.order_id) {
                                entry.order = ask_entry.order.clone();
                                entry.queue_position = 0;
                            }
                            self.asks.push_front(ask_entry);
                        } else {
                            ask_entry.order.status = OrderStatus::Filled;
                            // Remove from orders map
                            self.orders.remove(&ask_entry.order.order_id);
                            // Emit queue position telemetry for maker fill.
                            if let Some((enq_ts, enq_pos)) = self.resting_info.remove(&ask_entry.order.order_id) {
                                let exit_pos = ask_entry.queue_position;
                                let wait_time_ns = incoming.timestamp_ns.saturating_sub(enq_ts);
                                self.queue_events.push(QueuePositionEvent {
                                    order_id: ask_entry.order.order_id.clone(),
                                    client_order_id: ask_entry.order.client_order_id.clone(),
                                    symbol: ask_entry.order.symbol.clone(),
                                    side: ask_entry.order.side,
                                    price: ask_entry.order.price,
                                    enqueued_position: enq_pos,
                                    exit_position: exit_pos,
                                    wait_time_ns,
                                    outcome: QueuePositionOutcome::Filled,
                                });
                            }
                            // Ask queue permanently shifted, refresh positions
                            self._update_queue_positions(Side::Sell, 0);
                        }

                        remaining_qty -= trade_qty;
                    } else {
                        break;
                    }
                }
            }
            Side::Sell => {
                // Match against bids
                while remaining_qty > Decimal::ZERO && !self.bids.is_empty() {
                    if let Some(mut bid_entry) = self.bids.pop_front() {
                        // Check if price matches
                        match incoming.order_type {
                            OrderType::Limit => {
                                if incoming.price > bid_entry.order.price {
                                    self.bids.push_front(bid_entry);
                                    break;
                                }
                            }
                            OrderType::Market => {
                                // Market orders match at any price
                            }
                            _ => break,
                        }

                        // Determine trade price
                        let trade_price = match incoming.order_type {
                            OrderType::Limit => {
                                incoming.price.max(bid_entry.order.price)
                            }
                            OrderType::Market => {
                                bid_entry.order.price
                            }
                            _ => bid_entry.order.price,
                        };

                        // Determine trade quantity
                        let trade_qty = remaining_qty.min(bid_entry.order.leaves_qty);

                        // Create trade (deterministic id)
                        let trade_id = self.next_trade_id();
                        let trade = Trade {
                            trade_id,
                            order_id: incoming.order_id.clone(),
                            client_order_id: incoming.client_order_id.clone(),
                            contra_order_id: Some(bid_entry.order.order_id.clone()),
                            symbol: incoming.symbol.clone(),
                            side: Side::Sell,
                            price: trade_price,
                            qty: trade_qty,
                            timestamp_ns: incoming.timestamp_ns,
                        };
                        trades.push(trade.clone());

                        // Update bid order
                        bid_entry.order.cum_qty += trade_qty;
                        bid_entry.order.leaves_qty -= trade_qty;

                        if bid_entry.order.leaves_qty > Decimal::ZERO {
                            bid_entry.order.status = OrderStatus::Partial;
                            bid_entry.queue_position = 0;
                            if let Some(entry) = self.orders.get_mut(&bid_entry.order.order_id) {
                                entry.order = bid_entry.order.clone();
                                entry.queue_position = 0;
                            }
                            self.bids.push_front(bid_entry);
                        } else {
                            bid_entry.order.status = OrderStatus::Filled;
                            self.orders.remove(&bid_entry.order.order_id);
                            // Emit queue position telemetry for maker fill.
                            if let Some((enq_ts, enq_pos)) = self.resting_info.remove(&bid_entry.order.order_id) {
                                let exit_pos = bid_entry.queue_position;
                                let wait_time_ns = incoming.timestamp_ns.saturating_sub(enq_ts);
                                self.queue_events.push(QueuePositionEvent {
                                    order_id: bid_entry.order.order_id.clone(),
                                    client_order_id: bid_entry.order.client_order_id.clone(),
                                    symbol: bid_entry.order.symbol.clone(),
                                    side: bid_entry.order.side,
                                    price: bid_entry.order.price,
                                    enqueued_position: enq_pos,
                                    exit_position: exit_pos,
                                    wait_time_ns,
                                    outcome: QueuePositionOutcome::Filled,
                                });
                            }
                            // Bid queue permanently shifted, refresh positions
                            self._update_queue_positions(Side::Buy, 0);
                        }

                        remaining_qty -= trade_qty;
                    } else {
                        break;
                    }
                }
            }
        }

        // Handle TimeInForce constraints
        // Check IOC/FOK BEFORE potentially adding to book
        let should_add_to_book = match incoming.time_in_force {
            TimeInForce::IOC => {
                // Immediate or Cancel: cancel remaining quantity if any
                if remaining_qty > Decimal::ZERO || trades.is_empty() {
                    // IOC orders are not added to book if not fully filled or no match
                    incoming.status = OrderStatus::Canceled;
                    false
                } else {
                    incoming.status = OrderStatus::Filled;
                    // Update status if order is in map (from partial fills)
                    if let Some(incoming_entry) = self.orders.get_mut(&incoming.order_id) {
                        incoming_entry.order.status = OrderStatus::Filled;
                    }
                    false // IOC orders are never added to book (they're either filled or canceled)
                }
            }
            TimeInForce::FOK => {
                // Fill or Kill: if not fully filled, cancel and remove all trades
                if remaining_qty > Decimal::ZERO || trades.is_empty() {
                    incoming.status = OrderStatus::Canceled;
                    trades.clear(); // FOK requires complete fill
                    false
                } else {
                    incoming.status = OrderStatus::Filled;
                    // Update status if order is in map (from partial fills)
                    if let Some(incoming_entry) = self.orders.get_mut(&incoming.order_id) {
                        incoming_entry.order.status = OrderStatus::Filled;
                    }
                    false // FOK orders are never added to book (they're either filled or canceled)
                }
            }
            _ => {
                // GTC, Day: add remaining quantity to book if not fully filled
                if remaining_qty > Decimal::ZERO {
                    true
                } else {
                    // Update incoming order status
                    if let Some(incoming_entry) = self.orders.get_mut(&incoming.order_id) {
                        incoming_entry.order.status = OrderStatus::Filled;
                    }
                    false
                }
            }
        };

        // Add to book only if should_add_to_book is true
        if should_add_to_book {
            incoming.leaves_qty = remaining_qty;
            incoming.cum_qty = incoming.qty - remaining_qty;
            if incoming.cum_qty > Decimal::ZERO {
                incoming.status = OrderStatus::Partial;
            }
            match resting {
                RestingMode::Visible => {
                    let _ = self.add_order(incoming);
                }
                RestingMode::Hidden { display_qty } => {
                    let _ = self.add_hidden_order(HiddenOrder::new(incoming, display_qty));
                }
                RestingMode::None => {}
            }
        }

        // Update last trade price
        if let Some(last_trade) = trades.last() {
            self.last_trade_price = Some(last_trade.price);
        }

        trades
    }

    /// Find insertion position based on price-time priority
    fn _find_insertion_position(&self, order: &Order) -> usize {
        match order.side {
            Side::Buy => {
                // Find position in bids (higher price first, then earlier time)
                for (idx, entry) in self.bids.iter().enumerate() {
                    let comparable = OrderComparable { order: order.clone() };
                    let existing = OrderComparable { order: entry.order.clone() };
                    
                    // If new order has higher priority, insert here
                    if comparable.cmp(&existing) == std::cmp::Ordering::Greater {
                        return idx;
                    }
                }
                self.bids.len()
            }
            Side::Sell => {
                // Find position in asks (lower price first, then earlier time)
                for (idx, entry) in self.asks.iter().enumerate() {
                    let comparable = OrderComparable { order: order.clone() };
                    let existing = OrderComparable { order: entry.order.clone() };
                    
                    if comparable.cmp(&existing) == std::cmp::Ordering::Greater {
                        return idx;
                    }
                }
                self.asks.len()
            }
        }
    }

    /// Update queue positions after insertion/removal
    fn _update_queue_positions(&mut self, side: Side, start_pos: usize) {
        match side {
            Side::Buy => {
                // Recalculate all positions from start_pos onwards
                // After insertion/removal, elements shift, so we need to update from start_pos
                let mut idx = start_pos;
                for entry in self.bids.iter_mut().skip(start_pos) {
                    entry.queue_position = idx;
                    if let Some(order_entry) = self.orders.get_mut(&entry.order.order_id) {
                        order_entry.queue_position = idx;
                    }
                    idx += 1;
                }
            }
            Side::Sell => {
                // Recalculate all positions from start_pos onwards
                let mut idx = start_pos;
                for entry in self.asks.iter_mut().skip(start_pos) {
                    entry.queue_position = idx;
                    if let Some(order_entry) = self.orders.get_mut(&entry.order.order_id) {
                        order_entry.queue_position = idx;
                    }
                    idx += 1;
                }
            }
        }
    }

    /// Check if order would immediately match
    fn _would_immediately_match(&self, order: &Order) -> bool {
        match order.side {
            Side::Buy => {
                if let Some(best_ask) = self.asks.front() {
                    match order.order_type {
                        OrderType::Limit => {
                            order.price >= best_ask.order.price
                        }
                        OrderType::Market => true,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            Side::Sell => {
                if let Some(best_bid) = self.bids.front() {
                    match order.order_type {
                        OrderType::Limit => {
                            order.price <= best_bid.order.price
                        }
                        OrderType::Market => true,
                        _ => false,
                    }
                } else {
                    false
                }
            }
        }
    }

    /// Get best bid
    pub fn get_best_bid(&self) -> Option<&Order> {
        self.bids.front().map(|e| &e.order)
    }

    /// Get best ask
    pub fn get_best_ask(&self) -> Option<&Order> {
        self.asks.front().map(|e| &e.order)
    }

    /// Get order by ID
    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id).map(|e| &e.order)
    }

    /// Get symbol
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}
