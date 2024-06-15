use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::cmp::Ordering;
use std::cmp::PartialOrd;

use priority_queue::PriorityQueue;

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
enum Side {
    Buy,
    Sell,
}

#[derive(Clone)]
struct Order {
    order_id: String,
    symbol: String,
    price: f64,
    qty: u32,
    cum_qty: u32,
    leaves_qty: u32,
    side: Side,
}

#[derive(Clone)]
struct Trade {
    order_id: String,
    symbol: String,
    trade_price: f64,
    trade_qty: u32,
    trade_side: Side,
    trade_id: String,
}

#[derive(Clone, Eq, PartialEq)]
struct OrderComparable {
    order: Order,
}

impl Ord for OrderComparable {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.order.side != other.order.side {
            panic!("can only compare same side order");
        }

        // Higher price priority for buyer side
        if self.order.side == Side::Buy {
            if self.order.price > other.order.price {
                Ordering::Greater
            } else if self.order.price == other.order.price {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        // Lower price priority for seller side
        } else {
            if self.order.price < other.order.price {
                Ordering::Greater
            } else if self.order.price == other.order.price {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
    }
}

impl PartialOrd for OrderComparable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct OrderBook {
    symbol: String,
    asks: PriorityQueue<OrderComparable, i32>,
    bids: PriorityQueue<OrderComparable, i32>,
    order_id_mapping: HashMap<String, Order>,
}

impl OrderBook {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            asks: PriorityQueue::new(),
            bids: PriorityQueue::new(),
            order_id_mapping: HashMap::new(),
        }
    }

    fn add_order(&mut self, order: Order) -> bool {
        if self.order_id_mapping.contains_key(&order.order_id) {
            return false;
        }
        self.order_id_mapping.insert(order.order_id.clone(), order.clone());

        let comparable_order = OrderComparable { order };

        match order.side {
            Side::Buy => self.bids.push(comparable_order, 1),
            Side::Sell => self.asks.push(comparable_order, 1),
        }

        true
    }

    fn pop_order(&mut self, side: Side) -> Option<Order> {
        match side {
            Side::Buy => {
                if let Some((order, _)) = self.bids.pop() {
                    Some(order.order)
                } else {
                    None
                }
            }
            Side::Sell => {
                if let Some((order, _)) = self.asks.pop() {
                    Some(order.order)
                } else {
                    None
                }
            }
        }
    }
}

struct TradingEngine {
    orderbook_mapping: HashMap<String, Arc<Mutex<OrderBook>>>,
    ticker_mapping: Arc<Mutex<HashMap<String, f64>>>,
    is_live: bool,
}

impl TradingEngine {
    fn new(symbols: Option<Vec<String>>, is_live: bool) -> Self {
        let symbols = symbols.unwrap_or_else(|| vec!["AAPL".to_string(), "GOOGL".to_string()]);
        let orderbook_mapping = symbols
            .into_iter()
            .map(|symbol| (symbol.clone(), Arc::new(Mutex::new(OrderBook::new(symbol)))))
            .collect();

        Self {
            orderbook_mapping,
            ticker_mapping: Arc::new(Mutex::new(HashMap::new())),
            is_live,
        }
    }

    fn monitor(&self) {
        // Monitoring logic
    }

    fn init_orderbook(&mut self, symbols: Vec<String>) {
        for symbol in symbols {
            self.orderbook_mapping.insert(symbol.clone(), Arc::new(Mutex::new(OrderBook::new(symbol))));
        }
    }

    fn process_order(&self, data: HashMap<String, String>) -> bool {
        // Order processing logic
        true
    }

    fn update_ticker(&self, new_ticker: HashMap<String, f64>) {
        let mut ticker_mapping = self.ticker_mapping.lock().unwrap();
        for (symbol, price) in new_ticker {
            ticker_mapping.insert(symbol, price);
        }
    }

    fn close_order(&self, order_id: String, actual_price: f64, actual_time: SystemTime) -> bool {
        // Close order logic
        true
    }

    fn trade(&self, new_ticker: Option<HashMap<String, f64>>) {
        // Trade logic
    }

    fn run(&self) {
        let ticker_mapping = self.ticker_mapping.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(3));
            let mut ticker_mapping = ticker_mapping.lock().unwrap();
            // Update ticker logic
        });

        // Monitoring and trading threads
    }
}

fn main() {
    let trade_system = TradingEngine::new(None, true);
    trade_system.run();
}
