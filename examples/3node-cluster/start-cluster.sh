#!/usr/bin/env bash
#
# start-cluster.sh — Start a 3-node Fractio cluster on localhost
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
#
# After starting, you can interact with the cluster:
#   ./bin/fractio cluster info --port 9001
#   ./bin/fractio node ls --port 9001
#   curl http://127.0.0.1:9871/                    # Web dashboard
#   curl http://127.0.0.1:9871/api/cluster/health  # Health check

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_DIR/bin/fractio"
DATA_DIR="$SCRIPT_DIR/data"
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

stop_nodes() {
    echo "Stopping Fractio nodes..."
    for i in 1 2 3; do
        local pidfile="$PID_DIR/node${i}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping node $i (PID $pid)..."
                kill "$pid" 2>/dev/null || true
            fi
            rm -f "$pidfile"
        fi
    done
    # Also kill any fractio processes on our ports
    for port in "$NODE1_CLIENT" "$NODE2_CLIENT" "$NODE3_CLIENT"; do
        local pids
        pids=$(lsof -ti :"$port" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            echo "  Killing process on port $port: $pids"
            echo "$pids" | xargs kill 2>/dev/null || true
        fi
    done
    sleep 1
    echo "All nodes stopped."
}

clean_data() {
    echo "Wiping cluster data..."
    rm -rf "$DATA_DIR"
    rm -rf "$LOG_DIR"
    rm -rf "$PID_DIR"
    echo "Data wiped."
}

start_cluster() {
    # Build the binary if needed
    if [ ! -x "$BIN" ]; then
        echo "Binary not found at $BIN — building..."
        cd "$PROJECT_DIR"
        nim c --mm:atomicArc --threads:on -p:src -o:bin/fractio src/fractio/cli/main.nim
    fi

    mkdir -p "$DATA_DIR/node1" "$DATA_DIR/node2" "$DATA_DIR/node3"
    mkdir -p "$LOG_DIR" "$PID_DIR"

    echo "=== Starting 3-node Fractio cluster ==="
    echo ""

    # --- Node 1: seed node (becomes leader) ---
    echo "[Node 1] Starting seed node (Raft=$NODE1_RAFT, Client=$NODE1_CLIENT, Web=$NODE1_WEB)..."
    nohup "$BIN" start \
        --config="$SCRIPT_DIR/node1.toml" \
        --foreground \
        > "$LOG_DIR/node1.log" 2>&1 &
    local node1_pid=$!
    echo $node1_pid > "$PID_DIR/node1.pid"

    # Wait for node 1 to be ready
    wait_for_port "$NODE1_CLIENT" 30
    sleep 2
    echo ""

    # --- Node 2: join cluster ---
    echo "[Node 2] Joining cluster via Node 1..."
    nohup "$BIN" start \
        --config="$SCRIPT_DIR/node2.toml" \
        --join="127.0.0.1:$NODE1_WEB" \
        --foreground \
        > "$LOG_DIR/node2.log" 2>&1 &
    local node2_pid=$!
    echo $node2_pid > "$PID_DIR/node2.pid"

    wait_for_port "$NODE2_CLIENT" 30
    sleep 3
    echo ""

    # --- Node 3: join cluster ---
    echo "[Node 3] Joining cluster via Node 1..."
    nohup "$BIN" start \
        --config="$SCRIPT_DIR/node3.toml" \
        --join="127.0.0.1:$NODE1_WEB" \
        --foreground \
        > "$LOG_DIR/node3.log" 2>&1 &
    local node3_pid=$!
    echo $node3_pid > "$PID_DIR/node3.pid"

    wait_for_port "$NODE3_CLIENT" 30
    sleep 3
    echo ""

    echo "=== Cluster started ==="
    echo ""
    echo "Node 1:  Raft=$NODE1_RAFT  Client=$NODE1_CLIENT  Web=$NODE1_WEB  PID=$node1_pid"
    echo "Node 2:  Raft=$NODE2_RAFT  Client=$NODE2_CLIENT  Web=$NODE2_WEB  PID=$node2_pid"
    echo "Node 3:  Raft=$NODE3_RAFT  Client=$NODE3_CLIENT  Web=$NODE3_WEB  PID=$node3_pid"
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
                echo "  Node $i: RUNNING (PID $pid)"
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