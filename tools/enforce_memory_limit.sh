#!/bin/bash
# Memory limit enforcer - kills processes exceeding 250MB RSS
# This script monitors fractio node processes and terminates them if they exceed the limit

MEMORY_LIMIT_KB=256000  # 250MB in KB
POLL_INTERVAL=10        # Check every 10 seconds

echo "Starting memory enforcer (limit: ${MEMORY_LIMIT_KB}KB per process)..."
echo "Monitoring fractio node processes every ${POLL_INTERVAL}s"
echo ""

# Trap to clean up on exit
cleanup() {
    echo ""
    echo "Memory enforcer shutting down..."
    pkill -9 -f "bin/fractio" 2>/dev/null || true
    exit 0
}
trap cleanup SIGINT SIGTERM

while true; do
    for i in 1 2 3; do
        pidfile="/tmp/fractio_node${i}.pid"
        
        if [ ! -f "$pidfile" ]; then
            continue
        fi
        
        pid=$(cat "$pidfile")
        
        # Check if process exists
        if [ ! -d "/proc/$pid" ]; then
            echo "[$(date +%H:%M:%S)] Node $i: Process $pid exited (not in /proc)"
            continue
        fi
        
        # Get RSS in KB
        rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
        
        if [ -z "$rss" ] || ! [[ "$rss" =~ ^[0-9]+$ ]]; then
            echo "[$(date +%H:%M:%S)] Node $i: Could not read RSS for PID $pid"
            continue
        fi
        
        mem_mb=$((rss / 1024))
        
        # Check against limit
        if [ "$rss" -gt "$MEMORY_LIMIT_KB" ]; then
            echo "[$(date +%H:%M:%S)] !!! MEMORY LIMIT EXCEEDED for Node $i: ${mem_mb}MB (limit: 250MB) !!!"
            
            # Report memory stats before killing
            if [ -d "/proc/$pid" ]; then
                cat /proc/$pid/status | grep -E "VmRSS|VmSize|Threads" || true
            fi
            
            echo "Terminating Node $i (PID $pid)..."
            kill -9 "$pid" 2>/dev/null || true
            rm -f "$pidfile"
            
        elif [ "$rss" -gt 200000 ]; then
            # Warning at >195MB (78% of limit)
            echo "[$(date +%H:%M:%S)] ⚠️  Node $i: ${mem_mb}MB RSS (approaching limit!)"
        fi
    done
    
    sleep $POLL_INTERVAL
done
