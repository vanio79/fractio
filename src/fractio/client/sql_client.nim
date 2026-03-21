## SQL Client Extension for Fractio
## ==============================
##
## This module extends FractioClient with SQL capabilities.
## It handles parsing, planning, and execution on the client side.

import std/[options, atomics, json]
import ./fractio_client
import ../sql/parser
import ../sql/planner
import ../sql/executor
import ../protocol/types

proc query*(client: FractioClient, sql: string,
    database: string = "default", schema: string = "public"): ExecResult =
  ## Execute a SQL query on the client.
  ## Parses the SQL, plans it using cluster metadata, and executes it
  ## by sending direct KV operations to Raft group leaders.
  try:
    if not client.initialized.load(moRelaxed):
      if not client.initialize():
        return errorResult("failed to initialize client")

    let stmts = parseAll(sql)
    if stmts.len == 0:
      return errorResult("empty SQL statement")

    var lastResult = okResult("empty query")
    for stmt in stmts:
      let plan = planStatement(stmt, client, database, schema)
      lastResult = execute(plan, client, database)
      if lastResult.kind == erkError:
        return lastResult
    return lastResult
  except ParseError as e:
    return errorResult("SQL parse error: " & e.msg)
  except PlanError as e:
    return errorResult("SQL plan error: " & e.msg)
  except CatchableError as e:
    return errorResult("SQL execution error: " & e.msg)
