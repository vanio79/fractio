# Storage Benchmarks

This directory contains benchmarks comparing **Fractio** (Nim) against **fjall** (Rust) storage implementations.

## Overview

Both Fractio and fjall are LSM-tree based key-value storage engines. Fractio is a Nim port/implementation inspired by the fjall Rust project. These benchmarks measure the performance difference between the two implementations.

## Benchmarks

The following operations are benchmarked:

| Benchmark | Description |
|-----------|-------------|
| `sequential_writes` | Insert keys in sequential order |
| `random_writes` | Insert keys in random order |
| `sequential_reads` | Read keys in sequential order |
| `random_reads` | Read keys in random order |
| `range_scan` | Scan a range of keys |
| `prefix_scan` | Scan all keys with a given prefix |
| `deletions` | Delete keys |
| `batch_writes` | Batch insert operations |
| `contains_key` | Check key existence |

## Prerequisites

- Nim 1.6+ (for Fractio)
- Rust 1.91+ (for fjall)
- bc (for comparison calculations)

## Quick Start

Run the full benchmark comparison:

```bash
./run_benchmarks.sh
```

## Configuration Options

### Command Line

```bash
# Custom number of operations
./run_benchmarks.sh --ops 500000

# Custom key and value sizes
./run_benchmarks.sh --key-size 32 --value-size 256

# Run only one engine
./run_benchmarks.sh --fractio-only
./run_benchmarks.sh --fjall-only

# Show help
./run_benchmarks.sh --help
```

### Environment Variables

```bash
NUM_OPS=100000       # Number of operations
KEY_SIZE=16          # Key size in bytes
VALUE_SIZE=100       # Value size in bytes
```

## Directory Structure

```
benchmarks/
├── README.md                 # This file
├── run_benchmarks.sh         # Main runner script
├── fractio_bench.nim         # Fractio (Nim) benchmark
├── fjall_bench/              # fjall (Rust) benchmark
│   ├── Cargo.toml
│   └── main.rs
└── suite/                    # Additional benchmark suites
```

## Results

Results are saved to `tmp/benchmark_results/`:

- `fractio_YYYYMMDD_HHMMSS.log` - Full Fractio output
- `fjall_YYYYMMDD_HHMMSS.log` - Full fjall output
- `fractio_YYYYMMDD_HHMMSS.json` - Fractio results in JSON
- `fjall_YYYYMMDD_HHMMSS.json` - fjall results in JSON
- `comparison_YYYYMMDD_HHMMSS.md` - Comparison summary

## Interpreting Results

The benchmark outputs include:

1. **Ops/sec**: Operations per second throughput
2. **Latency (us)**: Average latency per operation in microseconds
3. **Diff %**: Percentage difference between implementations

A positive diff % means the first engine (Fractio) is faster; negative means fjall is faster.

## Notes

### Fair Comparison

To ensure a fair comparison:

- Both implementations use the same data patterns
- Default configuration is used for both engines
- Benchmarks are run in release mode
- Database directories are cleaned between runs

### Known Differences

1. **Memory Management**: Nim uses GC, Rust uses RAII
2. **Thread Safety**: Both implementations are thread-safe
3. **Durability**: Both support configurable durability levels

## Extending Benchmarks

To add new benchmarks:

1. Add the benchmark function to both `fractio_bench.nim` and `fjall_bench/main.rs`
2. Update the `BENCHMARKS` array in `run_benchmarks.sh`
3. Ensure consistent naming across all files

## Troubleshooting

### Compilation Errors

If you get compilation errors:

```bash
# Ensure dependencies are installed
nimble install    # For Fractio
cd fjall_bench && cargo build --release  # For fjall
```

### Runtime Errors

If benchmarks fail to run:

```bash
# Clean up stale database files
rm -rf /tmp/bench_fractio /tmp/bench_fjall

# Check permissions
chmod +x run_benchmarks.sh
```

### Missing bc

On Ubuntu/Debian:
```bash
sudo apt-get install bc
```

On macOS:
```bash
brew install bc
```
