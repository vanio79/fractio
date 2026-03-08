# Unit tests for NodeRegistry persistence (saveRegistry / loadRegistry).
#
# These tests exercise the on-disk round-trip without requiring a live server.
# Port range: 20470–20479 (none needed for pure unit tests).

import std/[unittest, os]
import fractio/protocol/server
import fractio/protocol/messages/cluster as clusterMsgs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc tmpPath(name: string): string =
  getTempDir() / ("fractio_reg_test_" & name & ".dat")

proc makeEntry(id: uint16, host: string, rp, cp: uint16): ClusterNodeEntry =
  ClusterNodeEntry(
    nodeId: id,
    host: host,
    raftPort: rp,
    clientPort: cp,
    status: clusterMsgs.NodeStatusActive,
  )

# ---------------------------------------------------------------------------
# Suite
# ---------------------------------------------------------------------------

suite "NodeRegistry persistence":

  test "round-trip: save then load recovers all entries":
    let path = tmpPath("roundtrip")
    defer: (try: removeFile(path) except CatchableError: discard)

    let reg = newNodeRegistry()
    reg.addNode(makeEntry(1, "10.0.0.1", 8300, 9000))
    reg.addNode(makeEntry(2, "10.0.0.2", 8301, 9001))
    reg.addNode(makeEntry(3, "10.0.0.3", 8302, 9002))

    saveRegistry(reg, path)
    check fileExists(path)

    let reg2 = loadRegistry(path)
    let nodes = reg2.listNodes()
    check nodes.len == 3

    var ids: seq[uint16]
    for n in nodes: ids.add(n.nodeId)
    check 1'u16 in ids
    check 2'u16 in ids
    check 3'u16 in ids

  test "loadRegistry on missing file returns empty registry":
    let path = tmpPath("missing_file_that_does_not_exist")
    # ensure it does not exist
    try: removeFile(path) except CatchableError: discard

    let reg = loadRegistry(path)
    check reg.listNodes().len == 0

  test "loadRegistry on corrupt file returns empty registry":
    let path = tmpPath("corrupt")
    defer: (try: removeFile(path) except CatchableError: discard)

    writeFile(path, "this is definitely not valid binary data!!!")

    let reg = loadRegistry(path)
    check reg.listNodes().len == 0

  test "multiple save/load cycles preserve final state":
    let path = tmpPath("multicycle")
    defer: (try: removeFile(path) except CatchableError: discard)

    # Cycle 1: save 2 nodes
    block:
      let reg = newNodeRegistry()
      reg.addNode(makeEntry(10, "192.168.1.10", 8310, 9010))
      reg.addNode(makeEntry(20, "192.168.1.20", 8320, 9020))
      saveRegistry(reg, path)

    # Cycle 2: load, add one more, save
    block:
      let reg = loadRegistry(path)
      check reg.listNodes().len == 2
      reg.addNode(makeEntry(30, "192.168.1.30", 8330, 9030))
      saveRegistry(reg, path)

    # Cycle 3: load and verify all 3
    block:
      let reg = loadRegistry(path)
      let nodes = reg.listNodes()
      check nodes.len == 3
      var ids: seq[uint16]
      for n in nodes: ids.add(n.nodeId)
      check 10'u16 in ids
      check 20'u16 in ids
      check 30'u16 in ids

  test "save empty registry produces loadable file":
    let path = tmpPath("empty")
    defer: (try: removeFile(path) except CatchableError: discard)

    let reg = newNodeRegistry()
    saveRegistry(reg, path)
    check fileExists(path)

    let reg2 = loadRegistry(path)
    check reg2.listNodes().len == 0

  test "entry fields are preserved exactly":
    let path = tmpPath("fields")
    defer: (try: removeFile(path) except CatchableError: discard)

    let reg = newNodeRegistry()
    reg.addNode(ClusterNodeEntry(
      nodeId: 42,
      host: "my.host.example.com",
      raftPort: 12345,
      clientPort: 54321,
      status: clusterMsgs.NodeStatusDraining,
    ))
    saveRegistry(reg, path)

    let reg2 = loadRegistry(path)
    let nodes = reg2.listNodes()
    check nodes.len == 1
    let n = nodes[0]
    check n.nodeId == 42'u16
    check n.host == "my.host.example.com"
    check n.raftPort == 12345'u16
    check n.clientPort == 54321'u16
    check n.status == clusterMsgs.NodeStatusDraining
