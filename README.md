# MatchingEngine

A high-performance, deterministic exchange-style matching engine (L2 + L3) with:
- A sequenced **event stream** (`Ack/Reject/CancelAck/ReplaceAck/Fill/Done`)
- Replayable **market data** (`BookSnapshot` + `BookDelta`)
- **Market states** and **auction uncrossing** (pre-open → open)
- **OCO** and **iceberg** order support (L2)
- Rust core with **C ABI** + **Go/Python bindings**

This engine lives inside the MarginForest monorepo at `third_party/MatchingEngine/`.

## Why this exists

The goal is to provide a **production-grade**, **replayable**, **high-performance** matching core that:
- Produces a deterministic output stream for auditing and backtesting
- Is easy to embed (Rust) or integrate (C ABI, Go, Python)
- Provides realistic exchange behavior (market states, auction, order lifecycle)

## Directory layout

```
third_party/MatchingEngine/
├── rust/                       # Rust core library + C ABI (cdylib/staticlib)
│   ├── include/                # C header: matching_engine.h
│   ├── src/                    # Engine + order books
│   ├── tests/                  # Correctness + replay tests
│   └── benches/                # Criterion benchmarks
├── bindings/
│   ├── go/                     # Go bindings (cgo + msgpack decode)
│   └── python/                 # Python bindings (ctypes + msgpack decode)
```

## Features (high level)

### Matching
- **Price-time priority** (FIFO within price)
- **Limit and market orders**
- **Time-in-force**: GTC / Day / IOC / FOK
- **Partial fills**
- **Multiple symbols**

### Exchange behavior
- **Market status** per symbol: `PreOpen / Open / Halted / Closed`
- **Open auction**: deterministic uncrossing with stable tie-break rules
- **Replace/Amend** with explicit event output

### Market data
- **`BookSnapshot`** (top N levels)
- **`BookDelta`** (level deltas, sequenced, replayable)
- Deltas are emitted for: add/rest, fill, cancel, replace, auction

### Advanced orders (L2)
- **OCO**: when one order is filled/canceled, the sibling is canceled automatically
- **Iceberg**: sequential child slices with deterministic refresh

## Core API (Rust)

The core Rust crate is `matching-engine` in `third_party/MatchingEngine/rust`.

### Continuous trading (simple)

```rust
use matching_engine::{MatchingEngine, Order, Side, OrderType, TimeInForce};
use rust_decimal::Decimal;

let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);

let buy = Order::new(
    "c1".to_string(),
    "AAPL".to_string(),
    Side::Buy,
    OrderType::Limit,
    Decimal::new(10100, 2),
    Decimal::new(10, 0),
    TimeInForce::GTC,
    1,
);

// Sequenced event stream (recommended)
let events = engine.submit_order_events(buy).unwrap();
```

### Market status + auction (L2)

```rust
use matching_engine::{MatchingEngine, MarketStatus};

let mut engine = MatchingEngine::new(vec!["AAPL".to_string()]);
engine.set_market_status_events("AAPL", MarketStatus::PreOpen, 1).unwrap();

// In PreOpen, orders rest without continuous matching.
// Run the open auction to uncross and transition to Open:
engine.open_auction_events("AAPL", 10, None).unwrap();
```

## Output contract: deterministic event stream

All high-level actions should be consumed through `Vec<EngineEvent>` (recommended).

- **Deterministic ordering**: `EngineEvent.seq` is monotonic per engine instance.
- **Replay**: applying `BookDelta` in seq order reconstructs the L2 book state when
  combined with a `BookSnapshot`.

Key kinds:
- `Ack`, `Reject`, `CancelAck`, `ReplaceAck`
- `Fill` (includes `Liquidity`: Maker/Taker/Auction)
- `Done` (terminal reason: Filled/Canceled/Rejected/Expired)
- `BookSnapshot`, `BookDelta`
- `MarketStatus` transition event

## Build and test (Rust)

From `third_party/MatchingEngine/rust`:

```bash
cargo test
cargo clippy -- -D warnings
```

### Benchmarks

```bash
cargo bench
```

## C ABI (FFI)

The Rust crate exports a stable C ABI:
- Header: `third_party/MatchingEngine/rust/include/matching_engine.h`
- Library output:
  - macOS: `rust/target/release/libmatching_engine.dylib`
  - Linux: `rust/target/release/libmatching_engine.so`

All API functions return **MessagePack**-encoded buffers (`MFBuffer`) that must be freed
with `mf_buffer_free`.

**Fixed-point decimals**
- price/qty are passed as scaled `int64`:
  - `Decimal = scaled / 1_000_000_000`

## Go bindings

Location: `third_party/MatchingEngine/bindings/go`

```bash
cd third_party/MatchingEngine/rust && cargo build --release
cd ../bindings/go && go test ./...
```

The Go wrapper:
- Calls the C ABI via cgo
- Decodes MessagePack into Go structs

## Python bindings

Location: `third_party/MatchingEngine/bindings/python`

```bash
cd third_party/MatchingEngine/rust && cargo build --release
cd ../bindings/python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
```

Usage:

```python
from matchingengine import Engine, Order, DECIMAL_SCALE

e = Engine(["AAPL"])
events = e.submit_order_events(Order(
    client_order_id="c1",
    symbol="AAPL",
    side=0,          # Buy
    order_type=1,    # Limit
    time_in_force=0, # GTC
    price=100 * DECIMAL_SCALE,
    qty=10 * DECIMAL_SCALE,
    timestamp_ns=1,
))
print(events)
```

## Compatibility notes

- The legacy directories `third_party/MatchingEngine/go` and `third_party/MatchingEngine/python`
  are not the current bindings. Use `third_party/MatchingEngine/bindings/`.
- The compatibility Rust API `submit_order() -> Vec<Trade>` still exists, but the
  recommended integration is the event stream (`submit_order_events`).

## License

Dual-licensed under **MIT OR Apache-2.0** (see `rust/Cargo.toml`).

