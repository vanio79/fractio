# Helper script to start a 3-node test cluster for manual testing
#
# Run:
#   nim c -r --mm:atomicArc --threads:on -p:src tests/integration/web/start_test_cluster.nim
#
# The cluster will stay running until you press Enter.
# Then you can test with:
#   curl http://127.0.0.1:9870/api/health
#   curl http://127.0.0.1:9871/api/health
#   curl http://127.0.0.1:9872/api/health

import std/os, std/httpclient, std/json, std/strformat, std/strutils
import ../../test_cluster

const BASE_PORT_OFFSET = 0 # Use default ports

proc httpGetJson(url: string): JsonNode =
  let client = newHttpClient(timeout = 5000)
  try:
    let resp = client.get(url)
    if resp.status.contains("200"):
      result = parseJson(resp.body)
    else:
      result = %*{"error": "HTTP " & resp.status}
  except CatchableError as e:
    result = %*{"error": e.msg}
  finally:
    client.close()

when isMainModule:
  echo "Starting 3-node Fractio cluster..."
  echo ""

  var cluster = newTestCluster(3, 3, basePort = BASE_PORT_OFFSET,
      verbose = true)

  if not cluster.start():
    echo "Failed to start cluster!"
    quit(1)

  echo ""
  echo "=== Cluster Started ==="
  echo ""
  echo "Web Dashboard URLs:"
  echo "  Node 1: http://127.0.0.1:9870"
  echo "  Node 2: http://127.0.0.1:9871"
  echo "  Node 3: http://127.0.0.1:9872"
  echo ""
  echo "Protocol Client Ports:"
  echo "  Node 1: 9000"
  echo "  Node 2: 9010"
  echo "  Node 3: 9020"
  echo ""

  # Wait for web to be ready
  echo "Waiting for web dashboard to be ready..."
  var ready = false
  for i in 0..30:
    try:
      let health = httpGetJson(cluster.getWebUrl() & "/api/health")
      if health.hasKey("metaLeaderOK") and health["metaLeaderOK"].getBool:
        ready = true
        break
      elif health.hasKey("status") and health["status"].getInt == 0:
        ready = true
        break
    except:
      discard
    sleep(500)

  if ready:
    echo "Web dashboard is ready!"
    echo ""
    echo "API Endpoints:"
    echo "  GET  /api/info         - Node information"
    echo "  GET  /api/health       - Cluster health"
    echo "  GET  /api/metrics      - Server metrics"
    echo "  GET  /api/storage      - Storage statistics"
    echo "  GET  /api/nodes        - Node registry"
    echo "  POST /api/nodes        - Add node"
    echo "  POST /api/cluster/join - Join cluster"
    echo "  GET  /api/spaces       - Space list"
    echo "  POST /api/sql          - Execute SQL"
    echo "  GET  /api/sql/databases - Database list"
    echo "  GET  /api/sql/schemas  - Schema list"
    echo "  GET  /api/sql/tables   - Table list"
    echo "  GET  /api/sql/system-tables - System tables"
    echo ""

    # Show initial health status
    echo "Initial health status:"
    let health = httpGetJson(cluster.getWebUrl() & "/api/health")
    echo "  Status: ", health["status"].getInt
    echo "  metaLeaderOK: ", health["metaLeaderOK"].getBool
    echo ""
  else:
    echo "Warning: Web dashboard may not be fully ready"

  echo "Press Enter to stop the cluster..."
  discard readLine(stdin)

  echo "Stopping cluster..."
  cluster.stop()
  echo "Cluster stopped."
