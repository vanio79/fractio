#!/usr/bin/env bash
#
# start-cluster.sh — Start a 3-node Fractio cluster on localhost
# Memory limit: 250MB per process enforced via ulimit -v (inherited by all child processes)
# Storage: ramdisk at /mnt/fractio_ramdisk for optimal I/O performance
#
# Usage:
#   ./start-cluster.sh          # Start cluster (nodes run in background)
#   ./start-cluster.sh stop     # Stop all nodes
#   ./start-cluster.sh clean    # Stop and wipe all data
#   ./start-cluster.sh status   # Check if nodes are running
#   ./start-cluster.sh logs     # Tail all node logs
#
# Ports:
#   Node 1: Raft=8301  Client=9001  Web=9871
#   Node 2: Raft=8302  Client=9002  Web=9872
#   Node 3: Raft=8303  Client=9003  Web=9873

set -euo pipefail

# Set virtual memory limit to 250MB for all child processes (in KB: 250 * 1024 = 256000)
ulimit -v 256000
echo "Virtual memory limit set to 250MB (ulimit -v 256000)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_DIR/bin/fractio"
DATA_DIR="/mnt/fractio_ramdisk/node{1,2,3}"  # Use ramdisk for data
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/pids"

# Port assignments
NODE1_RAFT=8301  NODE1_CLIENT=9001  NODE1_WEB=9871
NODE2_RAFT=8302  NODE2_CLIENT=9002  NODE2_WEB=9872
NODE3_RAFT=8303  NODE3_CLIENT=9003  NODE3_WEB=9873

# Wait for a TCP port to accept connections
wait_for_port() {
    local port=$1
    local max_wait=${2:-30}
    local waited=0
    while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
        if [ $waited -ge $max_wait ]; then
            echo "ERROR: Port $port not listening after ${max_wait}s"
            return 1
        fi
        sleep 0.5
        waited=$((waited + 1))
    done
    echo "  Port $port is listening"
}

# Function to start a node with memory limit enforcement via ulimit inheritance
start_node_with_limits() {
    local node_num=$1
    local config_file=$2
    local join_addr=$3
    
    # Create log and PID directories
    mkdir -p "$LOG_DIR" "$PID_DIR"
    
    local log_file="$LOG_DIR/node${node_num}.log"
    local pidfile="$PID_DIR/node${node_num}.pid"
    
    echo "[Node $node_num] Starting with 250MB memory limit (ulimit -v)"
    
    # Build the fractio command
    if [ -n "$join_addr" ]; then
        nohup "$BIN" start --config="$config_file" --join=$join_addr --foreground > "$log_file" 2>&1 &
    else
        nohup "$BIN" start --config="$config_file" --foreground > "$log_file" 2>&1 &
    fi
    
    local node_pid=$!
    echo $node_pid > "$pidfile"
    echo "[Node $node_num] Started with PID $node_pid (250MB limit inherited from ulimit -v)"
}

# Function to stop a node cleanly
stop_node() {
    local node_num=$1
    local pidfile="$PID_DIR/node${node_num}.pid"
    
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        echo "  Stopping Node $node_num (PID $pid)..."
        
        # Try graceful shutdown first
        kill "$pid" 2>/dev/null || true
        sleep 1
        
        # Force kill if still running
        if kill -0 "$pid" 2>/dev/null; then
            echo "    Force killing..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        
        rm -f "$pidfile"
    else
        echo "  No PID file found for Node $node_num"
    fi
}

stop_nodes() {
    echo "Stopping Fractio nodes..."
    
    # Kill by PID files first (SIGTERM)
    for i in 1 2 3; do
        local pidfile="$PID_DIR/node${i}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping node $i (PID $pid)..."
                
                # Try graceful shutdown first
                kill "$pid" 2>/dev/null || true
                
                sleep 1
                
                # Force kill if still running
                if kill -0 "$pid" 2>/dev/null; then
                    echo "    Forcing..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
                
                rm -f "$pidfile"
            else
                rm -f "$pidfile"
            fi
        fi
    done
    
    sleep 1
    
    # Force-kill any remaining processes on ALL our ports (client, raft, web)
    for port in "$NODE1_CLIENT" "$NODE2_CLIENT" "$NODE3_CLIENT" \
                "$NODE1_RAFT" "$NODE2_RAFT" "$NODE3_RAFT" \
                "$NODE1_WEB" "$NODE2_WEB" "$NODE3_WEB"; do
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            echo "  Force-killing process on port $port: $pids"
            echo "$pids" | xargs kill -9 2>/dev/null || true
        fi
    done
    
    sleep 1
    
    # Verify ports are free
    local still_busy=false
    for port in "$NODE1_CLIENT" "$NODE2_CLIENT" "$NODE3_CLIENT"; do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            still_busy=true
        fi
    done
    
    if [ "$still_busy" = true ]; then
        echo "WARNING: some ports still busy after kill"
    fi
    
    echo "All nodes stopped."
}

clean_data() {
    # Ensure ramdisk is mounted
    if ! mountpoint -q /mnt/fractio_ramdisk; then
        echo "Mounting ramdisk..."
        sudo mount -t tmpfs -o size=10G tmpfs /mnt/fractio_ramdisk || {
            echo "ERROR: Failed to mount ramdisk"
            exit 1
        }
    fi
    
    echo "Wiping cluster data on ramdisk..."
    rm -rf /mnt/fractio_ramdisk/node{1,2,3}/*
    mkdir -p /mnt/fractio_ramdisk/node{1,2,3}
    
    # Also clean local logs and pids
    rm -rf "$LOG_DIR"
    rm -rf "$PID_DIR"
    
    echo "Data wiped from ramdisk."
    df -h /mnt/fractio_ramdisk | tail -1
}

start_cluster() {
    # Build the binary if needed
    if [ ! -x "$BIN" ]; then
        echo "Binary not found at $BIN — building..."
        cd "$PROJECT_DIR"
        nim c --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
    fi
    
    # Ensure ramdisk is mounted for data storage
    if ! mountpoint -q /mnt/fractio_ramdisk; then
        echo "Mounting ramdisk at /mnt/fractio_ramdisk (10GB)..."
        sudo mount -t tmpfs -o size=10G tmpfs /mnt/fractio_ramdisk || {
            echo "ERROR: Failed to mount ramdisk"
            exit 1
        }
    fi
    
    mkdir -p "$LOG_DIR" "$PID_DIR"
    
    echo "=== Starting 3-node Fractio cluster ==="
    echo "Memory limit per node: 250MB (enforced via ulimit -v 256000)"
    echo "Storage: ramdisk at /mnt/fractio_ramdisk"
    echo ""
    
    # cd to project root so relative paths in toml resolve correctly
    cd "$PROJECT_DIR"
    
    # --- Node 1: seed node (becomes leader) ---
    echo "[Node 1] Starting seed node with 250MB limit..."
    start_node_with_limits 1 "examples/3node-cluster/node1.toml" ""
    
    # Wait for node 1 to be ready (both client and web ports)
    wait_for_port "$NODE1_CLIENT" 30
    wait_for_port "$NODE1_WEB" 30
    
    sleep 2
    
    local pid1
    pid1=$(cat "$PID_DIR/node1.pid")
    local rss1
    rss1=$(ps -o rss= -p "$pid1" 2>/dev/null | tr -d ' ') || echo "N/A"
    echo "[Node 1] Initial RSS: ${rss1}KB (limit: $((250 * 1024))KB)"
    
    # --- Node 2: join cluster ---
    echo ""
    echo "[Node 2] Joining cluster via Node 1..."
    start_node_with_limits 2 "examples/3node-cluster/node2.toml" "127.0.0.1:$NODE1_WEB"
    
    wait_for_port "$NODE2_CLIENT" 30
    wait_for_port "$NODE2_WEB" 30
    
    local pid2
    pid2=$(cat "$PID_DIR/node2.pid")
    local rss2
    rss2=$(ps -o rss= -p "$pid2" 2>/dev/null | tr -d ' ') || echo "N/A"
    echo "[Node 2] Initial RSS: ${rss2}KB (limit: $((250 * 1024))KB)"
    
    # --- Node 3: join cluster ---
    echo ""
    echo "[Node 3] Joining cluster via Node 1..."
    start_node_with_limits 3 "examples/3node-cluster/node3.toml" "127.0.0.1:$NODE1_WEB"
    
    wait_for_port "$NODE3_CLIENT" 30
    wait_for_port "$NODE3_WEB" 30
    
    local pid3
    pid3=$(cat "$PID_DIR/node3.pid")
    local rss3
    rss3=$(ps -o rss= -p "$pid3" 2>/dev/null | tr -d ' ') || echo "N/A"
    echo "[Node 3] Initial RSS: ${rss3}KB (limit: $((250 * 1024))KB)"
    
    sleep 3
    
    # Show current memory status for all nodes
    echo ""
    echo "=== Memory Usage Summary ==="
    local total_rss=0
    for i in 1 2 3; do
        local pid_i rss_i limit_kb=256000
        pid_i=$(cat "$PID_DIR/node${i}.pid")
        rss_i=$(ps -o rss= -p "$pid_i" 2>/dev/null | tr -d ' ') || echo "N/A"
        
        if [[ "$rss_i" =~ ^[0-9]+$ ]]; then
            local usage_pct=$(( rss_i * 100 / limit_kb ))
            echo "[Node $i] RSS: ${rss_i}KB (${usage_pct}% of limit)"
            
            # Warning if close to limit (80%+)
            if [ "$rss_i" -gt 200000 ]; then
                echo "  WARNING: Node $i using over 80% of memory limit!"
            fi
            
            total_rss=$((total_rss + rss_i))
        else
            echo "[Node $i] Could not determine RSS (process may have exited)"
        fi
    done
    
    local total_mb=$((total_rss / 1024))
    echo "Total cluster RSS: ${total_rss}KB ($total_mb MB)"
    
    # Show ramdisk usage
    echo ""
    echo "=== Ramdisk Usage ==="
    df -h /mnt/fractio_ramdisk | tail -1
    
    echo ""
    echo "=== Cluster started ==="
    echo ""
    echo "Node 1:  Raft=$NODE1_RAFT  Client=$NODE1_CLIENT  Web=$NODE1_WEB  PID=$pid1 (250MB limit via ulimit)"
    echo "Node 2:  Raft=$NODE2_RAFT  Client=$NODE2_CLIENT  Web=$NODE2_WEB  PID=$pid2 (250MB limit via ulimit)"
    echo "Node 3:  Raft=$NODE3_RAFT  Client=$NODE3_CLIENT  Web=$NODE3_WEB  PID=$pid3 (250MB limit via ulimit)"
    echo ""
    echo "Useful commands:"
    echo "  $0 status                     # Check node status"
    echo "  $0 logs                        # Tail all logs"
    echo "  $0 stop                        # Stop all nodes"
    echo "  $0 clean                       # Stop and wipe data"
    echo ""
    echo "  ./bin/fractio cluster info --port=$NODE1_CLIENT"
    echo "  ./bin/fractio node ls --port=$NODE1_CLIENT"
    echo "  curl http://127.0.0.1:$NODE1_WEB/"
    echo ""
    echo "Logs:  $LOG_DIR/"
}

show_status() {
    echo "=== Cluster status ==="
    local all_running=true
    for i in 1 2 3; do
        local pidfile="$PID_DIR/node${i}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                local rss
                rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ') || echo "N/A"
                echo "  Node $i: RUNNING (PID $pid, RSS: ${rss}KB)"
            else
                echo "  Node $i: STOPPED (stale PID $pid)"
                all_running=false
            fi
        else
            echo "  Node $i: NOT STARTED"
            all_running=false
        fi
    done
    
    if [ "$all_running" = true ]; then
        echo ""
        echo "All nodes running. Connect to any node:"
        echo "  ./bin/fractio cluster info --port $NODE1_CLIENT"
        
        # Show total memory usage
        echo ""
        echo "=== Total Memory Usage ==="
        local total_rss=0
        for i in 1 2 3; do
            local pidfile="$PID_DIR/node${i}.pid"
            if [ -f "$pidfile" ]; then
                local pid rss
                pid=$(cat "$pidfile")
                rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ') || echo "0"
                if [[ "$rss" =~ ^[0-9]+$ ]]; then
                    total_rss=$((total_rss + rss))
                fi
            fi
        done
        local total_mb=$((total_rss / 1024))
        echo "Total cluster RSS: ${total_rss}KB ($total_mb MB)"
    fi
}

tail_logs() {
    echo "=== Tailing logs (Ctrl-C to stop) ==="
    tail -f "$LOG_DIR/node1.log" "$LOG_DIR/node2.log" "$LOG_DIR/node3.log"
}

case "${1:-start}" in
    start)
        start_cluster
        ;;
    stop)
        stop_nodes
        ;;
    clean)
        stop_nodes
        clean_data
        ;;
    status)
        show_status
        ;;
    logs)
        tail_logs
        ;;
    *)
        echo "Usage: $0 {start|stop|clean|status|logs}"
        exit 1
        ;;
esac
