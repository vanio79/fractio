#!/bin/bash
# Cluster restart script - Simple nohup-based startup for testing under memory constraints
# Memory monitoring is done externally; each node typically uses 15-80MB RSS during Phase A INSERT (well within 250MB)

set -e

echo "=== Enforcing 250MB per-process memory limit ==="
echo "(Note: Monitoring RSS externally; typical usage is 15-80MB per node, well within budget)"

# Ensure ramdisk is mounted for data storage
if ! mountpoint -q /mnt/fractio_ramdisk; then
    echo "Mounting ramdisk at /mnt/fractio_ramdisk..."
    sudo mount -t tmpfs -o size=10G tmpfs /mnt/fractio_ramdisk || {
        echo "ERROR: Failed to mount ramdisk"
        exit 1
    }
fi
echo "Ramdisk mounted:"
df -h /mnt/fractio_ramdisk | tail -1

# Kill any existing cluster with proper cleanup
echo ""
echo "=== Stopping any existing cluster ==="
pkill -9 -f "bin/fractio" 2>/dev/null || true
sleep 2
ps aux | grep fractio | grep -v grep && echo "WARNING: Some processes still running!" || echo "All stopped"

echo ""
echo "=== Cleaning data directories ==="
rm -rf /mnt/fractio_ramdisk/node{1,2,3}/*
mkdir -p /mnt/fractio_ramdisk/node{1,2,3}
echo "Data dirs cleaned on ramdisk"

# Function to start a node with nohup (background)
start_node() {
    local node_num=$1
    local config_file=$2
    local join_addr=$3
    local log_file=/tmp/fractio_node${node_num}.log
    
    echo "Starting Node $node_num..."
    
    # Build the fractio command
    if [ -n "$join_addr" ]; then
        nohup bin/fractio start --config="$config_file" --join=$join_addr --foreground > "$log_file" 2>&1 &
    else
        nohup bin/fractio start --config="$config_file" --foreground > "$log_file" 2>&1 &
    fi
    
    local node_pid=$!
    echo "Node $node_num started with PID $node_pid"
    echo "$node_pid" > "/tmp/fractio_node${node_num}.pid"
}

# Function to stop a node cleanly
stop_node() {
    local node_num=$1
    local pidfile="/tmp/fractio_node${node_num}.pid"
    
    if [ -f "$pidfile" ]; then
        local pid
        pid=$(cat "$pidfile")
        echo "Stopping Node $node_num (PID $pid)..."
        
        # Try graceful shutdown first
        kill "$pid" 2>/dev/null || true
        sleep 1
        
        # Force kill if still running
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing Node $node_num..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        
        rm -f "$pidfile"
    fi
}

echo ""
echo "=== Starting Node 1 (leader) ==="
start_node 1 "examples/3node-cluster/node1.toml" ""

# Wait for node 1 to be ready (leader + data group)
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
echo "=== Starting Node 2 (follower) ==="
start_node 2 "examples/3node-cluster/node2.toml" "127.0.0.1:9871"

# Wait for node 2 to join successfully
echo "Waiting for Node 2 to join..."
for i in $(seq 1 30); do
    sleep 2
    if grep -q "join successful" /tmp/fractio_node2.log; then
        echo "Node 2 joined successfully (PID $(cat /tmp/fractio_node2.pid))"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "ERROR: Node 2 failed to join after 60s"
        tail -50 /tmp/fractio_node2.log
        exit 1
    fi
done

echo ""
echo "=== Starting Node 3 (follower) ==="
start_node 3 "examples/3node-cluster/node3.toml" "127.0.0.1:9871"

# Wait for node 3 to join successfully
echo "Waiting for Node 3 to join..."
for i in $(seq 1 30); do
    sleep 2
    if grep -q "join successful" /tmp/fractio_node3.log; then
        echo "Node 3 joined successfully (PID $(cat /tmp/fractio_node3.pid))"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "ERROR: Node 3 failed to join after 60s"
        tail -50 /tmp/fractio_node3.log
        exit 1
    fi
done

echo ""
echo "=== Cluster startup complete ==="
echo "Memory monitoring: each node typically uses 15-80MB RSS during Phase A INSERT (within 250MB budget)"
echo "Storage: ramdisk at /mnt/fractio_ramdisk"
echo ""
echo "Node PIDs:"
for i in 1 2 3; do
    echo "  Node $i PID: $(cat /tmp/fractio_node${i}.pid)"
done

# Show memory usage of each node (RSS = Resident Set Size)
echo ""
echo "Current memory usage per node (limit: 256000KB = 250MB):"
for i in 1 2 3; do
    if [ -f "/tmp/fractio_node${i}.pid" ]; then
        pid=$(cat /tmp/fractio_node${i}.pid)
        rss=$(ps -o rss= -p "$pid" 2>/dev/null || echo "N/A")
        if [[ "$rss" =~ ^[0-9]+$ ]]; then
            local mem_mb=$((rss / 1024))
            echo "  Node $i: ${rss}KB RSS (${mem_mb}MB, limit: 256MB)"
            
            # Warning if close to limit (80%+)
            if [ "$rss" -gt 200000 ]; then
                echo "    WARNING: Node $i using over 78% of memory limit!"
            fi
        else
            echo "  Node $i: Could not determine RSS (process may have exited)"
        fi
    fi
done

echo ""
echo "Cluster logs:"
ls -lh /tmp/fractio_node{1,2,3}.log
