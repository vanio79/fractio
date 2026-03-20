## Integration tests using external cluster
## ==========================================
##
## Tests that run against a Fractio cluster started as external processes.
## Uses the TestCluster infrastructure to manage server lifecycle.

import std/[unittest, options, os]
import ../test_cluster

suite "External Cluster Integration Tests":
  test "1-node cluster starts and responds to health check":
    var cluster = newTestCluster(1, 1, verbose = true)
    let started = cluster.start()
    check started
    if started:
      defer: cluster.stop()

      check cluster.isHealthy()

  test "1-node cluster put/get/delete":
    var cluster = newTestCluster(1, 1, verbose = true)
    let started = cluster.start()
    check started
    if started:
      defer: cluster.stop()

      # Wait for Raft to be ready (leader election, etc.)
      sleep(2000)

      # Put
      check cluster.put("test_key", "test_value")

      # Get
      let val = cluster.get("test_key")
      check val.isSome()
      check val.get() == "test_value"

      # Delete
      check cluster.delete("test_key")

      # Verify deleted
      let val2 = cluster.get("test_key")
      check val2.isNone()

  test "3-node cluster starts and elects leader":
    var cluster = newTestCluster(3, 3, verbose = true)
    let started = cluster.start()
    check started
    if started:
      defer: cluster.stop()

      check cluster.isHealthy()

      let leader = cluster.findLeader()
      check leader > 0
      echo "  Leader elected: node ", leader

test "3-node cluster handles KV operations":
  var cluster = newTestCluster(3, 3, verbose = true)
  let started = cluster.start()
  check started
  if started:
    defer: cluster.stop()

    # Wait for Raft to be fully ready (leader election, group setup, replication)
    sleep(3000)

    # Multiple operations
    for i in 0 ..< 10:
      let key = "key_" & $i
      let value = "value_" & $i
      check cluster.put(key, value)

    # Verify all values
    for i in 0 ..< 10:
      let key = "key_" & $i
      let expected = "value_" & $i
      let val = cluster.get(key)
      check val.isSome()
      check val.get() == expected
