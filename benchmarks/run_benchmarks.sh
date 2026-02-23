#!/bin/bash
#
# Storage Benchmark Runner
# Compares Fractio (Nim) vs fjall (Rust) storage performance
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/tmp/benchmark_results"

# Default configuration
NUM_OPS=${NUM_OPS:-100000}
KEY_SIZE=${KEY_SIZE:-16}
VALUE_SIZE=${VALUE_SIZE:-100}
FRACTIO_PATH=${FRACTIO_PATH:-/tmp/bench_fractio}
FJALL_PATH=${FJALL_PATH:-/tmp/bench_fjall}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BOLD}${CYAN}"
    echo "╔════════════════════════════════════════════════════════════════════════════╗"
    echo "║                      STORAGE BENCHMARK COMPARISON                          ║"
    echo "║                     Fractio (Nim) vs fjall (Rust)                          ║"
    echo "╚════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_config() {
    echo -e "${YELLOW}Configuration:${NC}"
    echo "  Operations:  $NUM_OPS"
    echo "  Key size:    $KEY_SIZE bytes"
    echo "  Value size:  $VALUE_SIZE bytes"
    echo ""
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --ops, -n <NUM>       Number of operations (default: 100000)"
    echo "  --key-size, -k <NUM>  Key size in bytes (default: 16)"
    echo "  --value-size, -v <NUM> Value size in bytes (default: 100)"
    echo "  --fractio-only        Run only Fractio benchmark"
    echo "  --fjall-only          Run only fjall benchmark"
    echo "  --help, -h            Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  NUM_OPS               Number of operations"
    echo "  KEY_SIZE              Key size in bytes"
    echo "  VALUE_SIZE            Value size in bytes"
    exit 0
}

# Parse arguments
RUN_FRACTIO=true
RUN_FJALL=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --ops|-n)
            NUM_OPS="$2"
            shift 2
            ;;
        --key-size|-k)
            KEY_SIZE="$2"
            shift 2
            ;;
        --value-size|-v)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --fractio-only)
            RUN_FJALL=false
            shift
            ;;
        --fjall-only)
            RUN_FRACTIO=false
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Create results directory
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

print_header
print_config

# Function to clear system caches (requires sudo, silently skips if not available)
clear_caches() {
    sync
    # Try to drop caches if we have sudo access
    if [ -w /proc/sys/vm/drop_caches ] 2>/dev/null; then
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    fi
    # Small sleep to let system settle
    sleep 1
}

# Function to setup tmpfs for fair disk I/O comparison
setup_tmpfs() {
    local path=$1
    if [ ! -d "$path" ]; then
        mkdir -p "$path"
    fi
    # Try to use tmpfs for the database path (avoids disk I/O variance)
    # Only works if we have sudo access
    if command -v sudo &> /dev/null && sudo -n true 2>/dev/null; then
        sudo mount -t tmpfs -o size=512M tmpfs "$path" 2>/dev/null || true
    fi
}

# Function to cleanup tmpfs
cleanup_tmpfs() {
    local path=$1
    if command -v sudo &> /dev/null && sudo -n true 2>/dev/null; then
        sudo umount "$path" 2>/dev/null || true
    fi
    rm -rf "$path"
}

# Function to compile and run Fractio benchmark
run_fractio_benchmark() {
    echo -e "${BOLD}${GREEN}═══ Running Fractio (Nim) Benchmark ═══${NC}"
    echo ""
    
    # Clear system caches for fair comparison
    echo "Clearing system caches..."
    clear_caches
    
    # Clean up previous data
    rm -rf "$FRACTIO_PATH"
    
    # Compile the benchmark
    echo "Compiling Fractio benchmark..."
    cd "$PROJECT_ROOT"
    nim c -d:release -p:src --checks:off "$SCRIPT_DIR/fractio_bench.nim"
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to compile Fractio benchmark${NC}"
        return 1
    fi
    
    # Run the benchmark
    echo "Running Fractio benchmark..."
    "$SCRIPT_DIR/fractio_bench" \
        --ops "$NUM_OPS" \
        --key-size "$KEY_SIZE" \
        --value-size "$VALUE_SIZE" \
        --path "$FRACTIO_PATH" \
        2>&1 | tee "$RESULTS_DIR/fractio_$TIMESTAMP.log"
    
    FRACTIO_EXIT_CODE=${PIPESTATUS[0]}
    
    # Extract JSON results
    if grep -q '"engine": "fractio"' "$RESULTS_DIR/fractio_$TIMESTAMP.log"; then
        # Extract JSON block
        sed -n '/^{/,/^}/p' "$RESULTS_DIR/fractio_$TIMESTAMP.log" > "$RESULTS_DIR/fractio_$TIMESTAMP.json"
    fi
    
    # Clean up database immediately to free memory
    rm -rf "$FRACTIO_PATH"
    
    echo ""
    return $FRACTIO_EXIT_CODE
}

# Function to compile and run fjall benchmark
run_fjall_benchmark() {
    echo -e "${BOLD}${GREEN}═══ Running fjall (Rust) Benchmark ═══${NC}"
    echo ""
    
    # Clear system caches for fair comparison
    echo "Clearing system caches..."
    clear_caches
    
    # Clean up previous data
    rm -rf "$FJALL_PATH"
    
    # Compile the benchmark
    echo "Compiling fjall benchmark..."
    cd "$SCRIPT_DIR/fjall_bench"
    cargo build --release 2>&1
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to compile fjall benchmark${NC}"
        return 1
    fi
    
    # Run the benchmark
    echo "Running fjall benchmark..."
    "$SCRIPT_DIR/fjall_bench/target/release/fjall_bench" \
        --ops "$NUM_OPS" \
        --key-size "$KEY_SIZE" \
        --value-size "$VALUE_SIZE" \
        --path "$FJALL_PATH" \
        2>&1 | tee "$RESULTS_DIR/fjall_$TIMESTAMP.log"
    
    FJALL_EXIT_CODE=${PIPESTATUS[0]}
    
    # Extract JSON results
    if grep -q '"engine": "fjall"' "$RESULTS_DIR/fjall_$TIMESTAMP.log"; then
        # Extract JSON block
        sed -n '/^{/,/^}/p' "$RESULTS_DIR/fjall_$TIMESTAMP.log" > "$RESULTS_DIR/fjall_$TIMESTAMP.json"
    fi
    
    # Clean up database immediately to free memory
    rm -rf "$FJALL_PATH"
    
    echo ""
    return $FJALL_EXIT_CODE
}

# Function to parse and compare results
compare_results() {
    echo -e "${BOLD}${CYAN}═══ Performance Comparison ═══${NC}"
    echo ""
    
    # Find the most recent JSON files
    FRACTIO_JSON=$(ls -t "$RESULTS_DIR"/fractio_*.json 2>/dev/null | head -1)
    FJALL_JSON=$(ls -t "$RESULTS_DIR"/fjall_*.json 2>/dev/null | head -1)
    
    if [ -z "$FRACTIO_JSON" ] || [ -z "$FJALL_JSON" ]; then
        echo -e "${YELLOW}Warning: Could not find JSON results for comparison${NC}"
        return 1
    fi
    
    echo "Using results from:"
    echo "  Fractio: $(basename $FRACTIO_JSON)"
    echo "  fjall:   $(basename $FJALL_JSON)"
    echo ""
    
    # Create comparison table for throughput
    echo -e "${BOLD}Throughput (ops/sec):${NC}"
    printf "%-25s %15s %15s %15s %15s\n" "Benchmark" "Fractio" "fjall" "Diff %" "Winner"
    echo "-----------------------------------------------------------------------------------------------------------"
    
    FRACTIO_WINS=0
    FJALL_WINS=0
    
    for bench in sequential_writes random_writes sequential_reads random_reads range_scan prefix_scan deletions batch_writes contains_key; do
        # Extract ops_per_sec from JSON files using jq if available, otherwise use grep
        if command -v jq &> /dev/null; then
            FRACTIO_OPS=$(jq -r ".results[\"$bench\"].ops_per_sec" "$FRACTIO_JSON" 2>/dev/null)
            FJALL_OPS=$(jq -r ".results[\"$bench\"].ops_per_sec" "$FJALL_JSON" 2>/dev/null)
        else
            # Fallback to grep for systems without jq
            FRACTIO_OPS=$(grep -A 10 "\"$bench\"" "$FRACTIO_JSON" 2>/dev/null | grep '"ops_per_sec"' | grep -oE '[0-9.]+')
            FJALL_OPS=$(grep -A 10 "\"$bench\"" "$FJALL_JSON" 2>/dev/null | grep '"ops_per_sec"' | grep -oE '[0-9.]+')
        fi
        
        # Validate that we got numeric values
        if [ -z "$FRACTIO_OPS" ] || [ -z "$FJALL_OPS" ] || ! [[ "$FRACTIO_OPS" =~ ^[0-9.]+$ ]] || ! [[ "$FJALL_OPS" =~ ^[0-9.]+$ ]]; then
            continue
        fi
        
        # Calculate percentage difference
        IS_FRACTIO_FASTER=$(echo "$FRACTIO_OPS > $FJALL_OPS" | bc -l 2>/dev/null)
        if [ "$IS_FRACTIO_FASTER" = "1" ]; then
            DIFF=$(echo "scale=2; (($FRACTIO_OPS - $FJALL_OPS) / $FJALL_OPS) * 100" | bc 2>/dev/null)
            WINNER="${GREEN}Fractio${NC}"
            FRACTIO_WINS=$((FRACTIO_WINS + 1))
        else
            DIFF=$(echo "scale=2; (($FJALL_OPS - $FRACTIO_OPS) / $FRACTIO_OPS) * 100" | bc 2>/dev/null)
            WINNER="${BLUE}fjall${NC}"
            FJALL_WINS=$((FJALL_WINS + 1))
        fi
        
        printf "%-25s %15.2f %15.2f %+14.1f%% " "$bench" "$FRACTIO_OPS" "$FJALL_OPS" "$DIFF"
        echo -e "$WINNER"
    done
    
    echo ""
    
    # Create comparison table for latency (lower is better)
    echo -e "${BOLD}Latency (microseconds):${NC}"
    printf "%-25s %15s %15s %15s %15s\n" "Benchmark" "Fractio" "fjall" "Diff %" "Winner"
    echo "-----------------------------------------------------------------------------------------------------------"
    
    FRACTIO_LATENCY_WINS=0
    FJALL_LATENCY_WINS=0
    
    for bench in sequential_writes random_writes sequential_reads random_reads range_scan prefix_scan deletions batch_writes contains_key; do
        # Extract latency from JSON files
        if command -v jq &> /dev/null; then
            FRACTIO_LATENCY=$(jq -r ".results[\"$bench\"].latency_us" "$FRACTIO_JSON" 2>/dev/null)
            FJALL_LATENCY=$(jq -r ".results[\"$bench\"].latency_us" "$FJALL_JSON" 2>/dev/null)
        else
            # Fallback to grep
            FRACTIO_LATENCY=$(grep -A 10 "\"$bench\"" "$FRACTIO_JSON" 2>/dev/null | grep '"latency_us"' | grep -oE '[0-9.]+')
            FJALL_LATENCY=$(grep -A 10 "\"$bench\"" "$FJALL_JSON" 2>/dev/null | grep '"latency_us"' | grep -oE '[0-9.]+')
        fi
        
        # Validate that we got numeric values
        if [ -z "$FRACTIO_LATENCY" ] || [ -z "$FJALL_LATENCY" ] || ! [[ "$FRACTIO_LATENCY" =~ ^[0-9.]+$ ]] || ! [[ "$FJALL_LATENCY" =~ ^[0-9.]+$ ]]; then
            continue
        fi
        
        # For latency, lower is better
        IS_FRACTIO_FASTER=$(echo "$FRACTIO_LATENCY < $FJALL_LATENCY" | bc -l 2>/dev/null)
        if [ "$IS_FRACTIO_FASTER" = "1" ]; then
            if [ "$(echo "$FJALL_LATENCY > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FJALL_LATENCY - $FRACTIO_LATENCY) / $FJALL_LATENCY) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${GREEN}Fractio${NC}"
            FRACTIO_LATENCY_WINS=$((FRACTIO_LATENCY_WINS + 1))
        else
            if [ "$(echo "$FRACTIO_LATENCY > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FRACTIO_LATENCY - $FJALL_LATENCY) / $FRACTIO_LATENCY) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${BLUE}fjall${NC}"
            FJALL_LATENCY_WINS=$((FJALL_LATENCY_WINS + 1))
        fi
        
        printf "%-25s %15.2f %15.2f %+14.1f%% " "$bench" "$FRACTIO_LATENCY" "$FJALL_LATENCY" "$DIFF"
        echo -e "$WINNER"
    done
    
    echo ""
    
    # Create comparison table for memory usage
    echo -e "${BOLD}Memory Usage (MB):${NC}"
    printf "%-25s %15s %15s %15s %15s\n" "Benchmark" "Fractio" "fjall" "Diff %" "Winner"
    echo "-----------------------------------------------------------------------------------------------------------"
    
    FRACTIO_MEMORY_WINS=0
    FJALL_MEMORY_WINS=0
    
    for bench in sequential_writes random_writes sequential_reads random_reads range_scan prefix_scan deletions batch_writes contains_key; do
        # Extract memory usage from JSON files
        if command -v jq &> /dev/null; then
            FRACTIO_MEMORY=$(jq -r ".results[\"$bench\"].memory_mb" "$FRACTIO_JSON" 2>/dev/null)
            FJALL_MEMORY=$(jq -r ".results[\"$bench\"].memory_mb" "$FJALL_JSON" 2>/dev/null)
        else
            # Fallback to grep
            FRACTIO_MEMORY=$(grep -A 10 "\"$bench\"" "$FRACTIO_JSON" 2>/dev/null | grep '"memory_mb"' | grep -oE '[0-9.]+')
            FJALL_MEMORY=$(grep -A 10 "\"$bench\"" "$FJALL_JSON" 2>/dev/null | grep '"memory_mb"' | grep -oE '[0-9.]+')
        fi
        
        # Validate that we got numeric values
        if [ -z "$FRACTIO_MEMORY" ] || [ -z "$FJALL_MEMORY" ] || ! [[ "$FRACTIO_MEMORY" =~ ^[0-9.]+$ ]] || ! [[ "$FJALL_MEMORY" =~ ^[0-9.]+$ ]]; then
            continue
        fi
        
        # For memory, lower is better
        IS_FRACTIO_BETTER=$(echo "$FRACTIO_MEMORY < $FJALL_MEMORY" | bc -l 2>/dev/null)
        if [ "$IS_FRACTIO_BETTER" = "1" ]; then
            if [ "$(echo "$FJALL_MEMORY > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FJALL_MEMORY - $FRACTIO_MEMORY) / $FJALL_MEMORY) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${GREEN}Fractio${NC}"
            FRACTIO_MEMORY_WINS=$((FRACTIO_MEMORY_WINS + 1))
        else
            if [ "$(echo "$FRACTIO_MEMORY > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FRACTIO_MEMORY - $FJALL_MEMORY) / $FRACTIO_MEMORY) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${BLUE}fjall${NC}"
            FJALL_MEMORY_WINS=$((FJALL_MEMORY_WINS + 1))
        fi
        
        printf "%-25s %15.2f %15.2f %+14.1f%% " "$bench" "$FRACTIO_MEMORY" "$FJALL_MEMORY" "$DIFF"
        echo -e "$WINNER"
    done
    
    echo ""
    
    # Create comparison table for CPU usage
    echo -e "${BOLD}CPU Usage (%):${NC}"
    printf "%-25s %15s %15s %15s %15s\n" "Benchmark" "Fractio" "fjall" "Diff %" "Winner"
    echo "-----------------------------------------------------------------------------------------------------------"
    
    FRACTIO_CPU_WINS=0
    FJALL_CPU_WINS=0
    
    for bench in sequential_writes random_writes sequential_reads random_reads range_scan prefix_scan deletions batch_writes contains_key; do
        # Extract CPU usage from JSON files
        if command -v jq &> /dev/null; then
            FRACTIO_CPU=$(jq -r ".results[\"$bench\"].cpu_percent" "$FRACTIO_JSON" 2>/dev/null)
            FJALL_CPU=$(jq -r ".results[\"$bench\"].cpu_percent" "$FJALL_JSON" 2>/dev/null)
        else
            # Fallback to grep
            FRACTIO_CPU=$(grep -A 10 "\"$bench\"" "$FRACTIO_JSON" 2>/dev/null | grep '"cpu_percent"' | grep -oE '[0-9.]+')
            FJALL_CPU=$(grep -A 10 "\"$bench\"" "$FJALL_JSON" 2>/dev/null | grep '"cpu_percent"' | grep -oE '[0-9.]+')
        fi
        
        # Validate that we got numeric values
        if [ -z "$FRACTIO_CPU" ] || [ -z "$FJALL_CPU" ] || ! [[ "$FRACTIO_CPU" =~ ^[0-9.]+$ ]] || ! [[ "$FJALL_CPU" =~ ^[0-9.]+$ ]]; then
            continue
        fi
        
        # For CPU, lower is better
        IS_FRACTIO_BETTER=$(echo "$FRACTIO_CPU < $FJALL_CPU" | bc -l 2>/dev/null)
        if [ "$IS_FRACTIO_BETTER" = "1" ]; then
            if [ "$(echo "$FJALL_CPU > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FJALL_CPU - $FRACTIO_CPU) / $FJALL_CPU) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${GREEN}Fractio${NC}"
            FRACTIO_CPU_WINS=$((FRACTIO_CPU_WINS + 1))
        else
            if [ "$(echo "$FRACTIO_CPU > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FRACTIO_CPU - $FJALL_CPU) / $FRACTIO_CPU) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${BLUE}fjall${NC}"
            FJALL_CPU_WINS=$((FJALL_CPU_WINS + 1))
        fi
        
        printf "%-25s %15.2f %15.2f %+14.1f%% " "$bench" "$FRACTIO_CPU" "$FJALL_CPU" "$DIFF"
        echo -e "$WINNER"
    done
    
    echo ""
    
    # Create comparison table for disk I/O
    echo -e "${BOLD}Disk I/O (MB):${NC}"
    printf "%-25s %15s %15s %15s %15s\n" "Benchmark" "Fractio" "fjall" "Diff %" "Winner"
    echo "-----------------------------------------------------------------------------------------------------------"
    
    FRACTIO_IO_WINS=0
    FJALL_IO_WINS=0
    
    for bench in sequential_writes random_writes sequential_reads random_reads range_scan prefix_scan deletions batch_writes contains_key; do
        # Extract disk usage from JSON files
        if command -v jq &> /dev/null; then
            FRACTIO_DISK=$(jq -r ".results[\"$bench\"].disk_mb" "$FRACTIO_JSON" 2>/dev/null)
            FJALL_DISK=$(jq -r ".results[\"$bench\"].disk_mb" "$FJALL_JSON" 2>/dev/null)
        else
            # Fallback to grep
            FRACTIO_DISK=$(grep -A 10 "\"$bench\"" "$FRACTIO_JSON" 2>/dev/null | grep '"disk_mb"' | grep -oE '[0-9.]+')
            FJALL_DISK=$(grep -A 10 "\"$bench\"" "$FJALL_JSON" 2>/dev/null | grep '"disk_mb"' | grep -oE '[0-9.]+')
        fi
        
        # Validate that we got numeric values
        if [ -z "$FRACTIO_DISK" ] || [ -z "$FJALL_DISK" ] || ! [[ "$FRACTIO_DISK" =~ ^[0-9.]+$ ]] || ! [[ "$FJALL_DISK" =~ ^[0-9.]+$ ]]; then
            continue
        fi
        
        # For disk I/O, lower is better
        IS_FRACTIO_BETTER=$(echo "$FRACTIO_DISK < $FJALL_DISK" | bc -l 2>/dev/null)
        if [ "$IS_FRACTIO_BETTER" = "1" ]; then
            if [ "$(echo "$FJALL_DISK > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FJALL_DISK - $FRACTIO_DISK) / $FJALL_DISK) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${GREEN}Fractio${NC}"
            FRACTIO_IO_WINS=$((FRACTIO_IO_WINS + 1))
        else
            if [ "$(echo "$FRACTIO_DISK > 0" | bc -l 2>/dev/null)" = "1" ]; then
                DIFF=$(echo "scale=2; (($FRACTIO_DISK - $FJALL_DISK) / $FRACTIO_DISK) * 100" | bc 2>/dev/null)
            else
                DIFF=0
            fi
            WINNER="${BLUE}fjall${NC}"
            FJALL_IO_WINS=$((FJALL_IO_WINS + 1))
        fi
        
        printf "%-25s %15.2f %15.2f %+14.1f%% " "$bench" "$FRACTIO_DISK" "$FJALL_DISK" "$DIFF"
        echo -e "$WINNER"
    done
    
    echo ""
    echo -e "${BOLD}Summary:${NC}"
    echo -e "  Throughput - Fractio wins: ${GREEN}$FRACTIO_WINS${NC} | fjall wins: ${BLUE}$FJALL_WINS${NC}"
    echo -e "  Latency    - Fractio wins: ${GREEN}$FRACTIO_LATENCY_WINS${NC} | fjall wins: ${BLUE}$FJALL_LATENCY_WINS${NC}"
    echo -e "  Memory     - Fractio wins: ${GREEN}$FRACTIO_MEMORY_WINS${NC} | fjall wins: ${BLUE}$FJALL_MEMORY_WINS${NC}"
    echo -e "  CPU        - Fractio wins: ${GREEN}$FRACTIO_CPU_WINS${NC} | fjall wins: ${BLUE}$FJALL_CPU_WINS${NC}"
    echo -e "  Disk I/O   - Fractio wins: ${GREEN}$FRACTIO_IO_WINS${NC} | fjall wins: ${BLUE}$FJALL_IO_WINS${NC}"
    echo ""
    
    # Save comparison results
    {
        echo "# Benchmark Comparison - $(date)"
        echo ""
        echo "Configuration:"
        echo "  - Operations: $NUM_OPS"
        echo "  - Key size: $KEY_SIZE bytes"
        echo "  - Value size: $VALUE_SIZE bytes"
        echo ""
        echo "Results:"
        echo "  Throughput - Fractio wins: $FRACTIO_WINS | fjall wins: $FJALL_WINS"
        echo "  Latency    - Fractio wins: $FRACTIO_LATENCY_WINS | fjall wins: $FJALL_LATENCY_WINS"
        echo "  Memory     - Fractio wins: $FRACTIO_MEMORY_WINS | fjall wins: $FJALL_MEMORY_WINS"
        echo "  CPU        - Fractio wins: $FRACTIO_CPU_WINS | fjall wins: $FJALL_CPU_WINS"
        echo "  Disk I/O   - Fractio wins: $FRACTIO_IO_WINS | fjall wins: $FJALL_IO_WINS"
    } > "$RESULTS_DIR/comparison_$TIMESTAMP.md"
    
    echo "Results saved to: $RESULTS_DIR/"
}

# Main execution
# Run fjall FIRST this time (Fractio ran first before) for fairer comparison
# Each run clears caches before starting
if [ "$RUN_FJALL" = true ]; then
    run_fjall_benchmark
fi

if [ "$RUN_FRACTIO" = true ]; then
    run_fractio_benchmark
fi

if [ "$RUN_FRACTIO" = true ] && [ "$RUN_FJALL" = true ]; then
    compare_results
fi

echo -e "${BOLD}${GREEN}Benchmark complete!${NC}"
