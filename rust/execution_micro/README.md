# execution-micro (M3)

Fast-path micro-decision library for execution.

## What it does

- **Deterministic quantization**: aligns price/qty to tick/lot constraints.
- **Price safety**: clamps candidate price using `price_limit`, `price_protection_bps`, and optional dynamic `max_slippage_bps`.
- **Cancel/replace**: proposes `Replace` when the price difference exceeds a threshold and the order has aged past `min_lifetime_ms`.

## What it does NOT do

- No networking, no gateway integration, no I/O in the library API.
- No strategy logic. It only performs deterministic calculations given an input context.

## Sidecar mode

This crate includes a JSONL sidecar binary for non-Rust callers:

```bash
cargo run --manifest-path execution_micro/Cargo.toml --bin sidecar
```

Protocol:
- stdin: one `MicroInput` JSON per line
- stdout: one `MicroDecision` JSON per line

