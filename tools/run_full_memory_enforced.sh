#!/bin/bash
# Full smoke test with strict 250MB per-process memory enforcement
# This script starts cluster, runs Phase A INSERT under monitoring, and reports results

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MEMORY_LIMIT_KB=256000  # 250MB in KB

echo "=========================================="
echo "Full Smoke Test with Memory Enforcement"
echo "Memory limit: ${MEMORY_LIMIT_KB}KB (250MB) per process"
echo "=========================================="

# Clean up any previous test data and processes
pkill -9 -f "bin/fractio" 2>/dev/null || true
sleep 1
rm -rf /mnt/fractio_ramdisk/node{1,2,3}/*
mkdir -p /mnt/fractio_ramdisk/node{1,2,3}

# Kill any existing smoke test processes
pkill -9 -f "smoke_1m_inserts_deletes" 2>/dev/null || true
pkill -9 -f "enforce_memory_limit" 2>/dev/null || true
sleep 1

echo ""
echo "=== Starting cluster ==="

# Start Node 1 (leader)
nohup bin/fractio start --config=examples/3node-cluster/node1.toml --foreground > /tmp/fractio_node1.log 2>&1 &
NODE1_PID=$!
echo "$NODE1_PID" > /tmp/fractio_node1.pid
echo "Node 1 started with PID $NODE1_PID"

# Wait for Node 1 to become leader
for i in $(seq 1 30); do
    sleep 2
    if grep -q "BECAME LEADER" /tmp/fractio_node1.log && \
       grep -q "Listening on port 9871" /tmp/fractio_node1.log; then
        echo "Node 1 is ready (PID $NODE1_PID)"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "ERROR: Node 1 failed to become leader after 60s"
        tail -50 /tmp/fractio_node1.log
        exit 1
    fi
done

# Start Node 2 (follower)
nohup bin/fractio start --config=examples/3node-cluster/node2.toml --join=127.0.0.1:9871 --foreground > /tmp/fractio_node2.log 2>&1 &
NODE2_PID=$!
echo "$NODE2_PID" > /tmp/fractio_node2.pid
echo "Node 2 started with PID $NODE2_PID"

# Start Node 3 (follower)
nohup bin/fractio start --config=examples/3node-cluster/node3.toml --join=127.0.0.1:9871 --foreground > /tmp/fractio_node3.log 2>&1 &
NODE3_PID=$!
echo "$NODE3_PID" > /tmp/fractio_node3.pid
echo "Node 3 started with PID $NODE3_PID"

# Wait for Nodes 2 and 3 to join
for i in $(seq 1 30); do
    sleep 2
    
    NODE2_READY=false
    NODE3_READY=false
    
    if grep -q "join successful" /tmp/fractio_node2.log 2>/dev/null; then
        echo "Node 2 joined successfully (PID $NODE2_PID)"
        NODE2_READY=true
    fi
    
    if grep -q "join successful" /tmp/fractio_node3.log 2>/dev/null; then
        echo "Node 3 joined successfully (PID $NODE3_PID)"
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

# Start memory enforcer in background
bash tools/enforce_memory_limit.sh > /tmp/memory_enforcer.log 2>&1 &
ENFORCER_PID=$!
echo "Memory enforcer started (PID $ENFORCER_PID)"

# Initial memory snapshot
echo ""
echo "=== Initial Memory Snapshot ==="
for i in 1 2 3; do
    pid=$(cat /tmp/fractio_node${i}.pid)
    rss=$(ps -o rss= -p "$pid" | tr -d ' ')
    if [[ "$rss" =~ ^[0-9]+$ ]]; then
        mem_mb=$((rss / 1024))
        echo "  Node $i: ${mem_mb}MB RSS (limit: 250MB)"
        
        # Show VmRSS from /proc
        if [ -d "/proc/$pid" ]; then
            vm_rss=$(grep "^VmRSS:" /proc/$pid/status | awk '{print $2}')
            vm_size=$(grep "^VmSize:" /proc/$pid/status | awk '{print $2}')
            echo "    VmRSS: ${vm_rss}KB, VmSize: ${vm_size}KB"
        fi
    fi
done

# Start Phase A INSERT with memory monitoring
echo ""
echo "=== Starting Phase A INSERT (1M rows) ==="
echo "Memory will be monitored every 30 seconds..."

# Create a temporary script that runs the test with periodic memory reporting
cat > /tmp/run_phase_a.sh << 'EOFSCRIPT'
#!/bin/bash
# Wrapper to run Phase A and report memory periodically

LOGFILE=/tmp/phase_a_mem_monitor.log
REPORT_INTERVAL=30  # Report every 30 seconds

echo "[$(date +%H:%M:%S)] Starting Phase A INSERT..." > "$LOGFILE"

timeout 600 bin/smoke_1m_inserts_deletes >> "$LOGFILE" 2>&1 &
TEST_PID=$!

last_report=$(date +%s)

while kill -0 $TEST_PID 2>/dev/null; do
    current_time=$(date +%s)
    
    if [ $((current_time - last_report)) -ge $REPORT_INTERVAL ]; then
        echo "" >> "$LOGFILE"
        echo "=== Memory Report at $(date +%H:%M:%S) ===" >> "$LOGFILE"
        
        for i in 1 2 3; do
            pid=$(cat /tmp/fractio_node${i}.pid 2>/dev/null) || continue
            
            if [ -d "/proc/$pid" ]; then
                rss=$(ps -o rss= -p "$pid" | tr -d ' ')
                vm_rss=$(grep "^VmRSS:" /proc/$pid/status | awk '{print $2}')
                
                if [[ "$rss" =~ ^[0-9]+$ ]]; then
                    mem_mb=$((rss / 1024))
                    
                    status="OK"
                    if [ "$rss" -gt 256000 ]; then
                        status="EXCEEDED!"
                    elif [ "$rss" -gt 230000 ]; then
                        status="WARN"
                    fi
                    
                    echo "Node $i: ${mem_mb}MB RSS (VmRSS: ${vm_rss:-N/A}KB) [$status]" >> "$LOGFILE"
                fi
            else
                echo "Node $i: Process exited" >> "$LOGFILE"
                
                # Check for OOM kill in dmesg
                if dmesg | grep -qi "killed process.*fractio\|oom.*node${i}" 2>/dev/null; then
                    echo "!!! Node $i was OOM killed !!!" >> "$LOGFILE"
                    dmesg | tail -20 | grep -iE "killed|oom|fractio" >> "$LOGFILE" || true
                fi
            fi
        done
        
        last_report=$current_time
    fi
    
    sleep 10
done

wait $TEST_PID
echo "" >> "$LOGFILE"
echo "[$(date +%H:%M:%S)] Phase A INSERT completed (exit code: $?)" >> "$LOGFILE"
EOFSCRIPT

chmod +x /tmp/run_phase_a.sh

# Run the Phase A test with monitoring
/tmp/run_phase_a.sh &
TEST_PID=$!

echo "Phase A test started (PID $TEST_PID)"
echo "Monitor log: /tmp/phase_a_mem_monitor.log"
echo "Waiting for test to complete or timeout (max 10 minutes)..."

# Wait for the test (up to 10 minutes)
timeout 600 bash -c 'while kill -0 '$TEST_PID' 2>/dev/null; do sleep 30; done' && {
    wait $TEST_PID
    test_exit=$?
} || {
    echo "Test timed out after 10 minutes"
    kill -9 $TEST_PID 2>/dev/null || true
    pkill -9 -f "bin/fractio" 2>/dev/null || true
    exit 1
}

# Stop memory enforcer
kill $ENFORCER_PID 2>/dev/null || true
wait $ENFORCER_PID 2>/dev/null || true

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
echo "=== Final Memory Snapshot ==="
for i in 1 2 3; do
    pid=$(cat /tmp/fractio_node${i}.pid 2>/dev/null) || continue
    
    if [ -d "/proc/$pid" ]; then
        rss=$(ps -o rss= -p "$pid" | tr -d ' ')
        vm_rss=$(grep "^VmRSS:" /proc/$pid/status | awk '{print $2}')
        mem_mb=$((rss / 1024))
        
        echo "Node $i: ${mem_mb}MB RSS (VmRSS: ${vm_rss:-N/A}KB)"
        
        # Check if close to limit
        if [ "$rss" -gt 200000 ]; then
            echo "  ⚠️  WARNING: Using >78% of memory budget!"
        elif [ "$rss" -lt 150000 ]; then
            echo "    ✓ Well within budget"
        fi
        
        # Show memory breakdown if available
        if [ -f "/proc/$pid/status" ]; then
            vm_size=$(grep "^VmSize:" /proc/$pid/status | awk '{print $2}')
            vm_data=$(grep "^VmData:" /proc/$pid/status | awk '{print $2}')
            vm_stk=$(grep "^VmStk:" /proc/$pid/status | awk '{print $2}')
            
            if [ -n "$vm_rss" ] && [ -n "$vm_size" ]; then
                rss_mb=$((vm_rss / 1024))
                size_mb=$((vm_size / 1024))
                
                echo "    Memory details:"
                echo "      VmSize (total virtual): ${size_mb}MB"
                echo "      VmRSS (resident/physical): ${rss_mb}MB"
                if [ -n "$vm_data" ]; then
                    data_mb=$((vm_data / 1024))
                    echo "      VmData (heap+stack): ${data_mb}MB"
                fi
            fi
        fi
    else
        echo "Node $i: Process exited"
        
        # Check for OOM kill in dmesg
        if dmesg | grep -qi "killed process.*fractio\|oom.*node${i}" 2>/dev/null; then
            echo "  !!! Was OOM killed !!!"
            dmesg | tail -30 | grep -iE "killed|oom|fractio" || true
        fi
    fi
done

# Show memory monitor log summary
echo ""
echo "=== Memory Monitor Log (Last 100 lines) ==="
if [ -f /tmp/phase_a_mem_monitor.log ]; then
    tail -100 /tmp/phase_a_mem_monitor.log
else
    echo "No monitor log found"
fi

# Show test log summary
echo ""
echo "=== Phase A Test Log (Last 50 lines) ==="
if [ -f /tmp/phase_a_test.log ]; then
    tail -50 /tmp/phase_a_test.log
else
    echo "No test log found at /tmp/phase_a_test.log"
fi

exit $test_exit
