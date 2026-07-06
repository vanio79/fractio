## INSERT-only smoke test - isolates INSERT path from DELETE/SCAN memory leaks.
##
## Inserts N rows in batches and exits.  No SELECTs, no DELETEs, no SCANs.
## Used to verify whether the memory growth is in the INSERT path or in
## the scan/delete path.
##
## Usage:
##   nim c --mm:atomicArc --threads:on --opt:speed -p:src \
##        -o:bin/smoke_insert_only tools/smoke_insert_only.nim
##   bin/smoke_insert_only 127.0.0.1 9001 --rows=100000

import std/[os, strutils, strformat, times, algorithm]
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

const
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9001
  DATABASE = "smoke"
  SCHEMA = "public"
  TABLE = "users"
  INSERT_BATCH_ROWS = 500

proc buildInsertBatch(startId, count: int): string =
  var values: seq[string] = @[]
  for i in 0 ..< count:
    let id = startId + i
    let name = &"user{id:06d}"
    let value = id * 10
    values.add(&"({id}, '{name}', {value})")
  return &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES " &
      join(values, ", ")

proc main() =
  var host = DEFAULT_HOST
  var port = DEFAULT_PORT
  var totalRows = 100_000
  var positionalIdx = 0
  for i in 1 .. paramCount():
    let a = paramStr(i)
    if a.startsWith("--rows="):
      let v = a[7 .. ^1]
      let last = v[^1]
      let (numPart, mult) =
        if last == 'K' or last == 'k': (v[0 ..< ^1], 1_000)
        elif last == 'M' or last == 'm': (v[0 ..< ^1], 1_000_000)
        else: (v, 1)
      try:
        totalRows = int(parseFloat(numPart) * float(mult))
      except CatchableError:
        echo "ERROR: bad --rows= value: ", v
        quit(1)
    else:
      inc positionalIdx
      if positionalIdx == 1: host = a
      elif positionalIdx == 2: port = parseInt(a)

  echo "INSERT-only smoke test"
  echo "  target:  ", host, ":", port
  echo "  rows:    ", totalRows
  echo ""

  var cfg = newFractioClientConfig(host, port)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 120_000
  cfg.maxKvRetries = 5
  let client = newFractioClient(cfg)
  if not client.initialize():
    echo "FAIL: client.initialize"
    quit(1)
  discard client.forceMetadataRefresh()
  echo "  client ready"

  let totalBatches = (totalRows + INSERT_BATCH_ROWS - 1) div INSERT_BATCH_ROWS
  var inserted = 0
  let t0 = epochTime()

  for batchIdx in 0 ..< totalBatches:
    let startId = 1 + batchIdx * INSERT_BATCH_ROWS
    let remaining = totalRows - batchIdx * INSERT_BATCH_ROWS
    let thisBatch = min(INSERT_BATCH_ROWS, remaining)
    if thisBatch <= 0: break
    let sql = buildInsertBatch(startId, thisBatch)
    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    if res.kind == erkModified:
      inserted += thisBatch
    else:
      echo &"  ERROR batch {batchIdx}: {res.error}"

    # Print progress every 1% and at completion
    if (batchIdx + 1) mod max(1, totalBatches div 100) == 0 or
       (batchIdx + 1) == totalBatches:
      let elapsed = epochTime() - t0
      echo &"  [{batchIdx + 1:4}/{totalBatches}] inserted {inserted}/{totalRows} elapsed {elapsed:.1f}s ({inserted.float / elapsed:.0f} rows/s)"

  let elapsed = epochTime() - t0
  echo ""
  echo &"INSERT-only test done: {inserted}/{totalRows} rows in {elapsed:.1f}s ({inserted.float / elapsed:.0f} rows/s)"
  client.close()

when isMainModule:
  main()
