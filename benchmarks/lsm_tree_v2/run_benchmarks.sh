#!/bin/sh
# Benchmark script for LSM Tree v2 - compares Nim and Rust implementations
# Uses native CPU optimization for both
#
# Usage: ./run_benchmarks.sh [num_ops] [--rebuild]
#   num_ops: Number of operations (default: 1000000)
#   --rebuild: Force rebuild of binaries

set -e

# Get script directory using POSIX-compliant method
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/../.."
NIM_BIN="$SCRIPT_DIR/nim"
NIM_SRC="$SCRIPT_DIR/nim.nim"
RUST_BIN="$SCRIPT_DIR/rust/target/release/lsm_tree_bench"

# Number of operations (default 1M)
NUM_OPS="${1:-1000000}"
REBUILD="$2"

echo "=========================================="
echo "LSM Tree v2 Benchmark ($NUM_OPS ops)"
echo "=========================================="
echo ""

# Clean up any existing benchmark data
rm -rf /tmp/bench_lsm_tree_nim /tmp/bench_lsm_tree 2>/dev/null || true

# Always compile Nim benchmark
echo "Compiling Nim benchmark..."
cd "$PROJECT_DIR" \
&& nim c -d:nimV2 -d:release -d:useMalloc \
    --path:src \
    --passC:"-march=native" \
    --passL:"-ljemalloc -lrt" \
    -o:"$NIM_BIN" \
    "$NIM_SRC"
echo ""

# Always compile Rust benchmark
echo "Compiling Rust benchmark..."
cd "$SCRIPT_DIR/rust"
RUSTFLAGS="-C target-cpu=native" cargo build --release
echo ""

echo "=========================================="
echo "Running Nim Benchmark ($NUM_OPS ops)"
echo ""
"$NIM_BIN" --ops $NUM_OPS --warmup 100

echo ""
echo "=========================================="
echo "Running Rust Benchmark ($NUM_OPS ops)"
echo "=========================================="
echo ""

# Run Rust with native CPU optimization
RUSTFLAGS="-C target-cpu=native" "$RUST_BIN" --ops $NUM_OPS
