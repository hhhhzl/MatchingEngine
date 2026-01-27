# MatchingEngine Test Suite

This directory contains comprehensive tests for the MatchingEngine library.

## Test Files

### `integration_test.rs`
Basic integration tests for L2 matching engine:
- Basic matching
- Market orders
- IOC orders
- FOK orders
- Order cancellation

### `l3_test.rs`
L3 functionality tests:
- Queue position tracking
- Price-time priority ordering
- Hidden orders (with display quantity)
- Fully hidden orders
- Post-only orders (accept/reject)
- MatchingEngineL3 integration
- Partial fills with queue position
- Multiple price levels
- Visible orderbook aggregation

### `order_types_test.rs`
Advanced order types tests:
- PostOnlyOrder (accept/reject scenarios)
- HiddenOrder (visible/hidden quantity calculations)
- StopOrder (buy/sell trigger conditions)
- StopLimitOrder (buy/sell trigger with limit)
- Integration with MatchingEngineL3

### `rules_engine_test.rs`
Market rules engine tests:
- Price limit rules
- Order size limit rules
- Position limit rules
- Price change limit rules
- Trading halt rules
- Symbol-specific rules
- Rule actions (Reject, Warn, Halt, Throttle)
- Disabled rules
- Integration with MatchingEngineL3

### `edge_cases_test.rs`
Edge cases and error handling:
- Zero quantity orders
- Negative price limit orders
- Duplicate order IDs
- Canceling nonexistent orders
- Canceling already filled orders
- Market orders with empty book
- IOC/FOK order edge cases
- Multiple partial fills
- Cancellation from middle of queue
- Order not found after fill
- Empty orderbook operations
- Very small/very high values
- Same price, different times
- Canceling best order

### `property_test.rs`
Property-based tests using `proptest`:
- Price-time priority invariants
- Quantity conservation
- Trade price validity
- Queue position consistency
- No negative quantities
- Order status transitions
- Deterministic matching
- Price improvement
- Time priority at same price

## Running Tests

```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test l3_test
cargo test --test order_types_test
cargo test --test rules_engine_test
cargo test --test edge_cases_test
cargo test --test property_test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_l3_queue_position_tracking

# Run property tests (may take longer)
cargo test --test property_test
```

## Test Coverage Goals

- **Unit Tests**: >90% coverage of public APIs
- **Integration Tests**: All public APIs covered
- **Edge Cases**: All error paths tested
- **Property Tests**: Critical invariants verified

## Test Categories

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test components working together
3. **Functional Tests**: Test complete workflows
4. **Edge Cases**: Test error conditions and boundary cases
5. **Property-Based Tests**: Test invariants across random inputs
6. **Performance Tests**: Benchmarks (in `benches/`)

## Notes

- All tests use English comments
- Tests are designed to be deterministic
- Property tests use `proptest` for random input generation
- Tests cover both L2 and L3 matching engines
- Tests verify price-time priority correctness
- Tests ensure no negative quantities or invalid states
