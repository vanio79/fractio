## Focused test: individual INSERT/DELETE operations like Phase C.
## Run after smoke_setup to reproduce the "table not found" issue.

import std/[os, strutils, strformat, times, options]
import fractio/core/types
import fractio/protocol/client
import fractio/client/fractio_client
import fractio/client/sql_client
import fractio/sql/executor

const
  HOST = "127.0.0.1"
  PORT = 9001
  DATABASE = "smoke"
  SCHEMA = "public"
  TABLE = "users"
  NUM_OPS = 200

proc main() =
  var cfg = newFractioClientConfig(HOST, PORT)
  cfg.connectionTimeoutMs = 5000
  cfg.requestTimeoutMs = 30000
  cfg.maxKvRetries = 10
  let client = newFractioClient(cfg)

  if not client.initialize():
    echo "Failed to initialize client"
    quit(1)
  discard client.forceMetadataRefresh()
  echo "Client initialized."

  # First, insert a few rows so we have something to delete
  echo "Inserting 20 initial rows..."
  for i in 1..20:
    let sql = &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES ({1000000 + i}, 'init{i:03d}', {i * 10})"
    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    if res.kind == erkError:
      echo &"  INSERT {i} FAILED: {res.error}"
    elif res.kind == erkModified:
      echo &"  INSERT {i} ok (count={res.count})"
    else:
      echo &"  INSERT {i} kind={res.kind}"

  echo ""
  echo &"Now running {NUM_OPS} interleaved INSERT/DELETE operations..."
  var ok = 0
  var fail = 0
  let t0 = epochTime()

  for opIdx in 0 ..< NUM_OPS:
    let sql = if opIdx mod 2 == 0:
      &"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} (id, name, value) VALUES ({2000000 + opIdx}, 'op{opIdx:05d}', {opIdx})"
    else:
      let targetId = 1000000 + ((opIdx * 7) mod 20) + 1
      &"DELETE FROM {DATABASE}.{SCHEMA}.{TABLE} WHERE id = {targetId}"

    let res = client.query(sql, database = DATABASE, schema = SCHEMA)
    if res.kind == erkError:
      echo &"  [{opIdx}] FAIL: {res.error}"
      inc fail
      # If table not found, try refreshing metadata
      if "not found" in res.error:
        echo "  refreshing metadata..."
        discard client.forceMetadataRefresh()
    elif res.kind == erkModified:
      inc ok
    else:
      echo &"  [{opIdx}] kind={res.kind}"

    if (opIdx + 1) mod 20 == 0:
      let elapsed = epochTime() - t0
      echo &"  [{opIdx + 1}/{NUM_OPS}] ok={ok} fail={fail} elapsed={elapsed:.1f}s"

  let elapsed = epochTime() - t0
  echo ""
  echo &"Done: {ok} ok, {fail} fail in {elapsed:.2f}s"
  client.close()

when isMainModule:
  main()
