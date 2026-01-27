## Python bindings (ctypes)

### Build the Rust shared library

From `third_party/MatchingEngine/rust`:

```bash
cargo build --release
```

This produces:
- macOS: `target/release/libmatching_engine.dylib`
- Linux: `target/release/libmatching_engine.so`

### Install Python package

From `third_party/MatchingEngine/bindings/python`:

```bash
python -m pip install -e .
```

### Quick usage

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

Notes:
- Returned values are MessagePack-decoded Python objects (dict/list) matching the Rust serde schema.

