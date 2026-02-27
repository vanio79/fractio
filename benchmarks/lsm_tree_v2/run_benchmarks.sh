#!/bin/bash
# Benchmark script for LSM Tree v2 - compares Nim and Rust implementations
# Uses native CPU optimization for both
#
# Usage: ./run_benchmarks.sh [num_ops] [--rebuild]
#   num_ops: Number of operations (default: 1000000)
#   --rebuild: Force rebuild of binaries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."
NIM_BIN="$SCRIPT_DIR/nim_bench"
RUST_BIN="$SCRIPT_DIR/rust/target/release/lsm_tree_bench"

# Number of operations (default 1M)
NUM_OPS="${1:-1000000}"
REBUILD="$2"

echo "=========================================="
echo "LSM Tree v2 Benchmark ($NUM_OPS ops)"
echo "=========================================="
echo ""

# Check if Nim binary exists, compile if not
if [ ! -f "$NIM_BIN" ] || [ "$REBUILD" == "--rebuild" ]; then
    echo "Compiling Nim benchmark..."
    cd "$PROJECT_DIR"
    nim c -d:nimV2 -d:release -d:useMalloc \
        --passC:"-march=native" \
        --passL:"-ljemalloc -lrt" \
        -p:src \
        -o:"$NIM_BIN" \
        "$SCRIPT_DIR/nim.nim"
    echo ""
fi

# Check if Rust binary exists, compile if not
if [ ! -f "$RUST_BIN" ] || [ "$REBUILD" == "--rebuild" ]; then
    echo "Compiling Rust benchmark..."
    cd "$SCRIPT_DIR/rust"
    RUSTFLAGS="-C target-cpu=native" cargo build --release
    echo ""
fi

echo "=========================================="
echo "Running Nim Benchmark ($NUM_OPS ops)"
echo "=========================================="
echo ""
"$NIM_BIN"

echo ""
echo "=========================================="
echo "Running Rust Benchmark ($NUM_OPS ops)"
echo "=========================================="
echo ""

# Run Rust with native CPU optimization
RUSTFLAGS="-C target-cpu=native" "$RUST_BIN" --ops "$NUM_OPS"
