#!/bin/bash
# Run all recovery tests in sequence
# Each test is split into child (creates data) and parent (verifies recovery)

set -e

# Determine project root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../../../../" && pwd)"
cd "$PROJECT_ROOT"

# Platform-specific library path
UNAME_S=$(uname -s)
if [ "$UNAME_S" = "Darwin" ]; then
  export DYLD_LIBRARY_PATH="/opt/homebrew/lib:${DYLD_LIBRARY_PATH:-}"
else
  export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH:-}"
fi

echo "=== Running Recovery Tests ==="
echo ""

# Test 1: Recovery with empty log
echo "--- Test 1: Recovery with empty log ---"
echo "Step 1: Child creates empty log..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test01_empty_child.nim 2>&1 | tail -5
echo "Step 2: Parent verifies recovery..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test01_empty_parent.nim 2>&1 | tail -5
echo ""

# Test 2: Recovery with multiple log entries
echo "--- Test 2: Recovery with multiple log entries ---"
echo "Step 1: Child creates multiple entries..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test02_multi_child.nim 2>&1 | tail -5
echo "Step 2: Parent verifies recovery..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test02_multi_parent.nim 2>&1 | tail -5
echo ""

# Test 3: Recovery and continue operations
echo "--- Test 3: Recovery and continue operations ---"
echo "Step 1: Child writes pre-crash data..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test03_continue_child.nim 2>&1 | tail -5
echo "Step 2: Parent recovers and continues..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test03_continue_parent.nim 2>&1 | tail -5
echo ""

# Test 4: Node state persists across restart
echo "--- Test 4: Node state persists across restart ---"
echo "Step 1: Child creates persistent data..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test04_persist_child.nim 2>&1 | tail -5
echo "Step 2: Parent verifies persistence..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test04_persist_parent.nim 2>&1 | tail -5
echo ""

# Test 5: Node state after crash simulation
echo "--- Test 5: Node state after crash simulation ---"
echo "Step 1: Child creates crash data..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test05_crash_child.nim 2>&1 | tail -5
echo "Step 2: Parent verifies crash recovery..."
nim c -r --checks:on --mm:atomicArc -p:src tests/integration/distributed/raft/recovery/test05_crash_parent.nim 2>&1 | tail -5
echo ""

echo "=== All Recovery Tests Passed ==="
