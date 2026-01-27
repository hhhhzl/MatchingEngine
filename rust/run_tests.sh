#!/bin/bash
# Test runner script for MatchingEngine

set -e

echo "=========================================="
echo "MatchingEngine Test Suite"
echo "=========================================="
echo ""

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed. Please install Rust first."
    echo "Visit: https://www.rust-lang.org/tools/install"
    exit 1
fi

# Check if rustup default is set
if ! rustup show default &> /dev/null; then
    echo "Setting default Rust toolchain..."
    rustup default stable || {
        echo "Error: Failed to set default toolchain."
        echo "Please run: rustup default stable"
        exit 1
    }
fi

echo "Rust version:"
cargo --version
rustc --version
echo ""

# Change to project directory
cd "$(dirname "$0")"

echo "Running cargo check..."
cargo check --lib --tests || {
    echo "Error: Compilation failed. Please fix errors first."
    exit 1
}

echo ""
echo "Running all tests..."
echo "=========================================="
cargo test --lib --tests -- --test-threads=1

echo ""
echo "Running integration tests..."
echo "=========================================="
cargo test --test integration_test

echo ""
echo "Running L3 tests..."
echo "=========================================="
cargo test --test l3_test

echo ""
echo "Running order types tests..."
echo "=========================================="
cargo test --test order_types_test

echo ""
echo "Running rules engine tests..."
echo "=========================================="
cargo test --test rules_engine_test

echo ""
echo "Running edge cases tests..."
echo "=========================================="
cargo test --test edge_cases_test

echo ""
echo "Running property-based tests..."
echo "=========================================="
cargo test --test property_test

echo ""
echo "=========================================="
echo "All tests completed!"
echo "=========================================="
