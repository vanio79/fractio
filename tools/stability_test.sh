#!/usr/bin/env bash
#
# stability_test.sh — Run the 3-replica OOM stability test 10 times in a row.
#
# For each iteration:
#   1. Start 3-node cluster
#   2. Create 3-replica space
#   3. Create table
#   4. Insert 10,000 rows
#   5. Verify SELECT works
#   6. Stop cluster and clean data
#
# If any step fails, the test stops and reports which iteration failed.
# If all 10 iterations pass, the OOM fix is considered verified.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLUSTER_DIR="$PROJECT_DIR/examples/3node-cluster"
BIN_DIR="$PROJECT_DIR/bin"
MAX_ITERATIONS=10
PASSED=0

echo "============================================"
echo "  Fractio 3-Replica Stability Test (10x)"
echo "============================================"
echo ""

for i in $(seq 1 $MAX_ITERATIONS); do
    echo ""
    echo "=========================================="
    echo "  ITERATION $i / $MAX_ITERATIONS"
    echo "=========================================="

    # --- Step 1: Clean and start cluster ---
    echo "[Iteration $i] Starting fresh cluster..."
    cd "$CLUSTER_DIR"
    bash start-cluster.sh clean > /dev/null 2>&1
    bash start-cluster.sh start > /dev/null 2>&1

    # Wait for cluster to be ready (all 9 ports listening)
    sleep 5
    all_ports_ok=true
    for port in 9001 9002 9003 8301 8302 8303 9871 9872 9873; do
        if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
            all_ports_ok=false
            echo "[Iteration $i] WARNING: Port $port not listening yet"
        fi
    done
    if [ "$all_ports_ok" = false ]; then
        sleep 3
    fi

    # Verify all 3 client nodes are up
    ALL_UP=true
    for port in 9001 9002 9003; do
        if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
            ALL_UP=false
            echo "[Iteration $i] ERROR: Port $port not listening!"
        fi
    done

    if [ "$ALL_UP" = false ]; then
        echo "[Iteration $i] FAILED: Cluster did not start properly"
        echo "--- Node logs ---"
        for n in 1 2 3; do
            echo "=== Node $n (last 30 lines) ==="
            tail -30 "$CLUSTER_DIR/logs/node${n}.log" 2>/dev/null || echo "(no log)"
        done
        bash start-cluster.sh clean > /dev/null 2>&1
        exit 1
    fi
    echo "[Iteration $i] Cluster started OK"

    # --- Step 2: Run bench_setup (space + table + 10K inserts) ---
    echo "[Iteration $i] Running bench_setup..."
    cd "$PROJECT_DIR"

    if ! timeout 300 "$BIN_DIR/bench_setup" > /tmp/fractio_stability_iter${i}.log 2>&1; then
        echo "[Iteration $i] FAILED: bench_setup exited with error"
        echo "--- bench_setup output ---"
        cat /tmp/fractio_stability_iter${i}.log
        echo ""
        echo "--- Node logs (last 40 lines each) ---"
        for n in 1 2 3; do
            echo "=== Node $n ==="
            tail -40 "$CLUSTER_DIR/logs/node${n}.log" 2>/dev/null || echo "(no log)"
        done
        # Check for OOM specifically
        if grep -qi "out of memory\|Resource deadlock\|SIGSEGV\|signal 6\|SIGABRT" "$CLUSTER_DIR"/logs/node*.log /tmp/fractio_stability_iter${i}.log 2>/dev/null; then
            echo ""
            echo "*** OOM/CRASH DETECTED in iteration $i ***"
        fi
        bash "$CLUSTER_DIR/start-cluster.sh" clean > /dev/null 2>&1
        exit 1
    fi

    echo "[Iteration $i] bench_setup completed OK"

    # --- Step 3: Verify nodes are still alive ---
    echo "[Iteration $i] Verifying nodes still alive..."
    for port in 9001 9002 9003; do
        if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
            echo "[Iteration $i] FAILED: Node on port $port crashed!"
            echo "--- Node logs ---"
            for n in 1 2 3; do
                echo "=== Node $n (last 40 lines) ==="
                tail -40 "$CLUSTER_DIR/logs/node${n}.log" 2>/dev/null || echo "(no log)"
            done
            bash "$CLUSTER_DIR/start-cluster.sh" clean > /dev/null 2>&1
            exit 1
        fi
    done
    echo "[Iteration $i] All 3 nodes alive"

    # --- Step 4: Check for OOM in logs ---
    if grep -qi "out of memory\|Resource deadlock\|SIGSEGV\|signal 6\|SIGABRT" "$CLUSTER_DIR"/logs/node*.log 2>/dev/null; then
        echo "[Iteration $i] FAILED: OOM/crash detected in node logs"
        for n in 1 2 3; do
            echo "=== Node $n (crash lines) ==="
            grep -i "out of memory\|Resource deadlock\|SIGSEGV\|signal 6\|SIGABRT" "$CLUSTER_DIR/logs/node${n}.log" 2>/dev/null || echo "(none)"
        done
        bash "$CLUSTER_DIR/start-cluster.sh" clean > /dev/null 2>&1
        exit 1
    fi

    # --- Step 5: Stop and clean ---
    echo "[Iteration $i] Stopping and cleaning cluster..."
    bash "$CLUSTER_DIR/start-cluster.sh" clean > /dev/null 2>&1

    PASSED=$((PASSED + 1))
    echo "[Iteration $i] PASSED ✓"
    echo ""
done

echo ""
echo "============================================"
echo "  ALL $MAX_ITERATIONS ITERATIONS PASSED ✓"
echo "============================================"
echo ""
echo "Successfully completed $PASSED/$MAX_ITERATIONS iterations."
echo "The 3-replica OOM fix is verified."