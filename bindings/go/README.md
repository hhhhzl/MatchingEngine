## Go bindings (cgo)

### Build the Rust shared library

From `third_party/MatchingEngine/rust`:

```bash
cargo build --release
```

This produces:
- macOS: `target/release/libmatching_engine.dylib`
- Linux: `target/release/libmatching_engine.so`

### Use from Go

From `third_party/MatchingEngine/bindings/go`:

```bash
go test ./...
```

Notes:
- The cgo linker flags assume the Rust library is at `../../rust/target/release`.
- Events and snapshots are returned as MessagePack. This wrapper decodes them using `msgpack/v5`.

