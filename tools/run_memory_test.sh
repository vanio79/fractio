#!/bin/bash
# Run Phase A INSERT under strict 250MB per-process cgroup limits
# This script enforces memory limits via cgroups v2 and monitors growth

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MEMORY_LIMIT_KB=256000  # 250MB in KB (in bytes for cgroup)
MEMORY_LIMIT_BYTES=$((MEMORY_LIMIT_KB * 1024))
MEMORY_WARN_KB=230000   # 89% of limit (warning threshold)
MEMORY_WARN_BYTES=$((MEMORY_WARN_KB * 1024))

CGROUP_BASE="/sys/fs/cgroup/fractio"

echo "=========================================="
echo "Phase A Memory Test - 1M INSERT under cgroup limits"
echo "Memory limit: ${MEMORY_LIMIT_KB}KB (250MB) per process"
echo "=========================================="

# Clean up any previous test data and cgroups
pkill -9 -f "bin/fractio" 2>/dev/null || true
sleep 1
rm -rf /mnt/fractio_ramdisk/node{1,2,3}/*
mkdir -p /mnt/fractio_ramdisk/node{1,2,3}

# Kill any existing smoke test processes
pkill -9 -f "smoke_1m_inserts_deletes" 2>/dev/null || true
sleep 1

# Clean up old cgroups if they exist
for i in 1 2 3; do
    rmdir "$CGROUP_BASE/node${i}" 2>/dev/null || true
done

echo ""
echo "=== Starting cluster with cgroup enforcement ==="

# Start nodes and assign to memory-controlled cgroups
for i in 1 2 3; do
    echo "Starting Node $i..."
    
    # Create cgroup for this node (cgroup v2)
    mkdir -p "$CGROUP_BASE/node${i}" || {
        echo "ERROR: Failed to create cgroup fractio/node${i}"
        pkill -9 -f "bin/fractio" 2>/dev/null || true
        exit 1
    }
    
    # Start node with nohup
    if [ "$i" = "1" ]; then
        nohup bin/fractio start --config=examples/3node-cluster/node1.toml --foreground > "/tmp/fractio_node${i}.log" 2>&1 &
    else
        nohup bin/fractio start --config=examples/3node-cluster/node${i}.toml --join=127.0.0.1:9871 --foreground > "/tmp/fractio_node${i}.log" 2>&1 &
    fi
    
    NODE_PID=$!
    echo "$NODE_PID" > /tmp/fractio_node${i}.pid
    
    # Wait for process to start (give it a moment)
    sleep 2
    
    # Assign PID to cgroup (cgroup v2 uses cgroup.procs file)
    if [ -f "/proc/$NODE_PID/cgroup" ]; then
        echo $NODE_PID > "$CGROUP_BASE/node${i}/cgroup.procs" || {
            echo "ERROR: Failed to assign Node $i PID $NODE_PID to cgroup"
            pkill -9 -f "bin/fractio" 2>/dev/null || true
            exit 1
        }
    else
        echo "WARNING: Could not find process /proc/$NODE_PID/cgroup, trying direct assignment..."
        # Fallback: try to find the PID and assign it
        sleep 3
        if [ -f "/proc/$NODE_PID/cgroup" ]; then
            echo $NODE_PID > "$CGROUP_BASE/node${i}/cgroup.procs" || true
        fi
    fi
    
    # Set memory limit for this cgroup (cgroup v2 uses memory.max)
    echo $MEMORY_LIMIT_BYTES > "$CGROUP_BASE/node${i}/memory.max" 2>/dev/null || {
        echo "ERROR: Failed to set memory limit for Node $i"
        pkill -9 -f "bin/fractio" 2>/dev/null || true
        exit 1
    }
    
    # Set soft limit (throttling threshold) via memory.low or high
    echo $MEMORY_WARN_BYTES > "$CGROUP_BASE/node${i}/memory.high" 2>/dev/null || true
    
    echo "Node $i started with PID $NODE_PID, assigned to cgroup fractio/node${i}"
    echo "  Memory limit: ${MEMORY_LIMIT_KB}KB (memory.max = $(cat $CGROUP_BASE/node${i}/memory.max))"
done

echo ""
echo "Waiting for Node 1 to become leader..."
for i in $(seq 1 30); do
    sleep 2
    if grep -q "BECAME LEADER" /tmp/fractio_node1.log && \
       grep -q "Listening on port 9871" /tmp/fractio_node1.log; then
        echo "Node 1 is ready (PID $(cat /tmp/fractio_node1.pid))"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "ERROR: Node 1 failed to become leader after 60s"
        tail -50 /tmp/fractio_node1.log
        exit 1
    fi
done

echo ""
echo "Waiting for Nodes 2 and 3..."
for i in $(seq 1 30); do
    sleep 2
    
    NODE2_READY=false
    NODE3_READY=false
    
    if grep -q "join successful" /tmp/fractio_node2.log 2>/dev/null; then
        echo "Node 2 joined successfully (PID $(cat /tmp/fractio_node2.pid))"
        NODE2_READY=true
    fi
    
    if grep -q "join successful" /tmp/fractio_node3.log 2>/dev/null; then
        echo "Node 3 joined successfully (PID $(cat /tmp/fractio_node3.pid))"
        NODE3_READY=true
    fi
    
    if $NODE2_READY && $NODE3_READY; then
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "ERROR: Nodes failed to join after 60s"
        tail -50 /tmp/fractio_node{2,3}.log
        exit 1
    fi
done

echo ""
echo "=== Cluster startup complete ==="
echo "Running memory baseline snapshot..."

# Initial memory snapshot
for i in 1 2 3; do
    pid=$(cat /tmp/fractio_node${i}.pid)
    rss=$(ps -o rss= -p $pid | tr -d ' ')
    if [[ "$rss" =~ ^[0-9]+$ ]]; then
        mem_mb=$((rss / 1024))
        echo "  Node $i: ${rss}KB RSS (${mem_mb}MB)"
        
        # Check cgroup limits
        cgroup_limit=$(cat /sys/fs/cgroup/fractio/node${i}/memory.current 2>/dev/null || echo "N/A")
        cgroup_max=$(cat /sys/fs/cgroup/fractio/node${i}/memory.max 2>/dev/null || echo "N/A")
        echo "    CGroup: current=${cgroup_limit}B, max=${cgroup_max}B"
        
        if [ "$rss" -gt $MEMORY_WARN_KB ]; then
            echo "    WARNING: Node $i exceeding soft limit!"
        fi
    fi
done

echo ""
echo "=== Starting Phase A INSERT (1M rows) ==="
echo "Monitoring memory every 30 seconds..."

# Start background memory monitor
(
    while true; do
        timestamp=$(date +%H:%M:%S)
        
        for i in 1 2 3; do
            pid=$(cat /tmp/fractio_node${i}.pid 2>/dev/null) || continue
            
            if [ -d "/proc/$pid" ]; then
                rss=$(ps -o rss= -p $pid | tr -d ' ')
                cgroup_mem=$(cat /sys/fs/cgroup/fractio/node${i}/memory.current 2>/dev/null | cut -c1-8)
                
                if [[ "$rss" =~ ^[0-9]+$ ]]; then
                    mem_mb=$((rss / 1024))
                    status="OK"
                    
                    if [ "$rss" -gt $MEMORY_LIMIT_KB ]; then
                        status="EXCEEDED!"
                    elif [ "$rss" -gt $MEMORY_WARN_KB ]; then
                        status="WARN"
                    fi
                    
                    echo "[$timestamp] Node $i: ${mem_mb}MB RSS (cgroup: ${cgroup_mem:-N/A}) [$status]"
                else
                    echo "[$timestamp] Node $i: Process not found or not in cgroup"
                fi
            else
                echo "[$timestamp] Node $i: Process exited"
                
                # If process died, check if it was OOM killed
                if dmesg | grep -qi "killed process.*fractio\|oom.*fractio" 2>/dev/null; then
                    echo "[$timestamp] !!! OOM KILL DETECTED for Node $i !!!"
                    dmesg | tail -50 | grep -i "killed\|oom\|fractio" || true
                fi
            fi
        done
        
        sleep 30
    done
) &

MONITOR_PID=$!
echo "Memory monitor started (PID $MONITOR_PID)"

# Run the smoke test Phase A only
echo ""
echo "Starting INSERT of 1M rows..."
timeout 600 bin/smoke_1m_inserts_deletes > /tmp/phase_a_test.log 2>&1 &
TEST_PID=$!

echo "Test started with PID $TEST_PID"
echo "Waiting for test to complete or timeout..."

# Wait for the test (up to 10 minutes)
while kill -0 $TEST_PID 2>/dev/null; do
    sleep 60
    
    # Check if any node is close to limit
    for i in 1 2 3; do
        pid=$(cat /tmp/fractio_node${i}.pid 2>/dev/null) || continue
        
        if [ -d "/proc/$pid" ]; then
            rss=$(ps -o rss= -p $pid | tr -d ' ')
            
            if [[ "$rss" =~ ^[0-9]+$ ]]; then
                if [ "$rss" -gt $MEMORY_LIMIT_KB ]; then
                    echo "!!! MEMORY LIMIT EXCEEDED for Node $i ($rss KB) !!!"
                    echo "Terminating test and cluster..."
                    
                    # Kill everything
                    kill -9 $TEST_PID 2>/dev/null || true
                    pkill -9 -f "bin/fractio" 2>/dev/null || true
                    
                    # Report cgroup stats
                    for j in 1 2 3; do
                        echo ""
                        echo "=== Final CGroup Stats ==="
                        echo "fractio/node${j}:"
                        echo "  memory.current: $(cat /sys/fs/cgroup/fractio/node${j}/memory.current 2>/dev/null || echo 'N/A')"
                        echo "  memory.max: $(cat /sys/fs/cgroup/fractio/node${j}/memory.max 2>/dev/null || echo 'N/A')"
                        echo "  memory.stat:"
                        cat /sys/fs/cgroup/fractio/node${j}/memory.stat 2>/dev/null | head -10 || true
                    done
                    
                    exit 1
                fi
            fi
        fi
    done
    
    # Check for OOM kills in dmesg
    if dmesg | grep -qi "killed process.*fractio\|oom.*fractio" 2>/dev/null; then
        echo "!!! OOM KILL DETECTED !!!"
        kill -9 $TEST_PID 2>/dev/null || true
        pkill -9 -f "bin/fractio" 2>/dev/null || true
        
        dmesg | tail -100 | grep -iE "killed|oom|fractio" || true
        
        # Report final stats
        for j in 1 2 3; do
            echo ""
            echo "=== Final CGroup Stats ==="
            echo "fractio/node${j}:"
            cat /sys/fs/cgroup/fractio/node${j}/memory.stat 2>/dev/null || true
        done
        
        exit 1
    fi
done

wait $TEST_PID
test_exit=$?

# Stop memory monitor
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo "Phase A Test Complete (exit code: $test_exit)"
echo "=========================================="

if [ $test_exit -eq 0 ]; then
    echo "✓ Phase A INSERT completed successfully"
else
    echo "✗ Phase A INSERT failed with exit code $test_exit"
fi

# Final memory snapshot
echo ""
echo "Final Memory Snapshot:"
for i in 1 2 3; do
    pid=$(cat /tmp/fractio_node${i}.pid 2>/dev/null) || continue
    
    if [ -d "/proc/$pid" ]; then
        rss=$(ps -o rss= -p $pid | tr -d ' ')
        mem_mb=$((rss / 1024))
        
        echo "  Node $i: ${mem_mb}MB RSS (limit: 250MB)"
        
        # Show cgroup stats
        if [ -f "/sys/fs/cgroup/fractio/node${i}/memory.stat" ]; then
            cache=$(grep "^cache " /sys/fs/cgroup/fractio/node${i}/memory.stat | awk '{print $2}')
            anon=$(grep "^anon " /sys/fs/cgroup/fractio/node${i}/memory.stat | awk '{print $2}')
            
            if [ -n "$cache" ] && [ -n "$anon" ]; then
                cache_mb=$((cache / 1024 / 1024))
                anon_mb=$((anon / 1024 / 1024))
                
                echo "    Memory breakdown:"
                echo "      Anon (heap/stack): ${anon_mb}MB"
                echo "      Cache (file-backed): ${cache_mb}MB"
            fi
        fi
        
        # Check if close to limit
        rss_kb=$rss
        if [ "$rss_kb" -gt 200000 ]; then
            echo "    ⚠️  WARNING: Using >78% of memory budget!"
        elif [ "$rss_kb" -lt 150000 ]; then
            echo "    ✓ Well within budget"
        fi
    else
        echo "  Node $i: Process exited"
        
        # Check for OOM kill in dmesg
        if dmesg | grep -qi "killed process.*fractio\|oom.*node${i}" 2>/dev/null; then
            echo "    !!! Was OOM killed !!!"
        fi
    fi
done

# Show test log summary
echo ""
echo "=== Test Log Summary ==="
if [ -f /tmp/phase_a_test.log ]; then
    tail -100 /tmp/phase_a_test.log
fi

exit $test_exit
