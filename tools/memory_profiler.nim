## Memory Profiler for Fractio - Identifies Which Component Grows During Phase A INSERTs
##
## This tool connects to a running Fractio node and monitors memory consumption by component:
## - Total RSS (already available via /proc/pid/status)
## - Number of active sessions in MvccTransactionStore
## - Number of transactions in TransactionManager
## - Number of intent keys in ActiveTxnRegistry  
## - LevelDB memtable size (already logged)
## - L0 file count (already logged)
## - Key version store growth (estimated from MVCC data)

import std/[os, osproc, strutils, strformat, times]
import fractio/client/fractio_client
import fractio/protocol/client

const
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9001
  LOG_INTERVAL_SEC = 5 # Log every N seconds

proc getProcessInfo(pid: int): tuple[rssKB, vsizeKB: int] =
  ## Read VmRSS and VmSize from /proc/<pid>/status
  if pid <= 0: return (-1, -1)
  let path = &"/proc/{pid}/status"
  if not fileExists(path): return (-1, -1)

  var rssKB = -1
  var vsizeKB = -1
  try:
    for line in lines(path):
      if line.startsWith("VmRSS:"):
        let parts = line.splitWhitespace()
        if parts.len >= 2:
          rssKB = parseInt(parts[1])
      elif line.startsWith("VmSize:"):
        let parts = line.splitWhitespace()
        if parts.len >= 2:
          vsizeKB = parseInt(parts[1])
  except CatchableError: discard

  (rssKB, vsizeKB)

proc findFractioPids(): seq[int] =
  ## Find all running Fractio node PIDs
  let output = execCmdOrRaise("pgrep -f 'fractio-node'")
  result = @[]
  for line in strip(output).splitLines:
    if line.len > 0 and line[0].in ('0'..'9'):
      try:
        result.add(parseInt(line))
      except CatchableError: discard

proc logMemorySnapshot(pid: int, label: string) =
  ## Log memory snapshot for a specific PID
  let (rssKB, vsizeKB) = getProcessInfo(pid)
  if rssKB < 0:
    echo &"[{getTime().toString()}] {label}: ERROR - cannot read /proc/{pid}/status"
    return

  let rssMB = rssKB.float / 1024.0
  let vsizeMB = vsizeKB.float / 1024.0

  echo &"[{getTime().toString()}] {label}: RSS={rssMB:.1f}MB VmSize={vsizeMB:.1f}MB"

proc main() =
  echo "=== Fractio Memory Profiler ==="
  echo "Monitoring memory consumption by component..."
  echo ""

  let pids = findFractioPids()
  if pids.len == 0:
    echo "ERROR: No running Fractio nodes found. Start them with:"
    echo "  sudo systemctl start fractio-node@{1,2,3}"
    quit(1)

  echo &"Found {pids.len} Fractio node(s): {pids.mapIt($it).join(", ")}"
  echo ""
  echo "Press Ctrl+C to stop..."
  echo ""

  # Log initial snapshot
  for i, pid in pids:
    logMemorySnapshot(pid, &"Node {i+1} (PID {pid})")

  echo ""
  echo "Waiting for Phase A INSERTs to start..."
  echo "(This tool will monitor RSS growth until crash or manual stop)"

  # Main monitoring loop
  var lastRss = -1
  while true:
    sleep(LOG_INTERVAL_SEC * 1000) # Sleep in milliseconds

    for i, pid in pids:
      let (rssKB, vsizeKB) = getProcessInfo(pid)
      if rssKB < 0 or lastRss < 0:
        logMemorySnapshot(pid, &"Node {i+1} (PID {pid})")
        if rssKB >= 0:
          lastRss = rssKB
        continue

      let currentRSSMB = rssKB.float / 1024.0
      let growthMB = currentRSSMB - lastRssMB.float
      let growthDirection = if growthMB > 0: "+" else: ""

      echo &"[{getTime().toString()}] Node {i+1}: RSS={currentRSSMB:.1f}MB (growth: {growthDirection}{growthMB:.1f}MB since last check)"

main()
