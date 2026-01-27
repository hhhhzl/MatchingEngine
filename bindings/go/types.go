package matchingengine

// These structs mirror the Rust `EngineEvent`/`EngineEventKind` schema as encoded by MessagePack.
// We represent `Decimal` values as strings here because Rust `rust_decimal` serializes via serde.
// Consumers that need numeric arithmetic can parse these strings or use scaled int64 inputs/outputs.

type EngineEvent struct {
	Seq         uint64         `msgpack:"seq"`
	TimestampNS int64          `msgpack:"timestamp_ns"`
	Symbol      string         `msgpack:"symbol"`
	Kind        EngineEventKind `msgpack:"kind"`
}

// Rust enum encoding (serde) is represented as a map with a single key for the variant.
// msgpack library decodes into `map[string]any` by default; we keep it as raw.
type EngineEventKind map[string]any

type BookLevel struct {
	Price string `msgpack:"price"`
	Qty   string `msgpack:"qty"`
}

type BookSnapshot struct {
	Bids []BookLevel `msgpack:"bids"`
	Asks []BookLevel `msgpack:"asks"`
}

