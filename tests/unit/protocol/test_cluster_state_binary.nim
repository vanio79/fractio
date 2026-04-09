# Unit Tests for Persisted Cluster State Binary Serialization

import unittest
import std/[tables, os, times, options, strutils]
import fractio/protocol/cluster_state_binary

suite "Persisted Cluster State Binary Serialization Tests":

  test "Create empty cluster state":
    let state = newPersistedClusterState()
    check state.peers.len == 0
    check state.self.nodeId == 0
    check state.self.host == ""

  test "Create cluster state with self info":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(
      nodeId: 1,
      host: "127.0.0.1",
      clientPort: 9000,
      webPort: 8080
    )
    check state.self.nodeId == 1
    check state.self.host == "127.0.0.1"
    check state.self.clientPort == 9000
    check state.self.webPort == 8080

  test "Add peers to cluster state":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "host1", clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: "host2", port: 9100)
    state.peers[3] = (host: "host3", port: 9200)
    check state.peers.len == 2
    check state.peers[2].host == "host2"
    check state.peers[2].port == 9100
    check state.peers[3].host == "host3"
    check state.peers[3].port == 9200

  test "Encode empty cluster state":
    let state = newPersistedClusterState()
    let encoded = encodeClusterState(state)
    # Check magic header
    check encoded[0].ord == 0x43 # 'C'
    check encoded[1].ord == 0x53 # 'S'
    check encoded[2].ord == 0x42 # 'B'
    check encoded[3].ord == 0x01 # version
    check encoded.len >= 4

  test "Encode cluster state with self only":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(
      nodeId: 42,
      host: "192.168.1.100",
      clientPort: 5000,
      webPort: 3000
    )
    let encoded = encodeClusterState(state)
    check encoded.len >= 4

  test "Encode cluster state with peers":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "host1", clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: "host2", port: 9100)
    state.peers[3] = (host: "host3", port: 9200)
    state.peers[4] = (host: "host4", port: 9300)
    let encoded = encodeClusterState(state)
    check encoded.len >= 4

  test "Decode empty cluster state":
    let state = newPersistedClusterState()
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.nodeId == 0
    check decoded.self.host == ""
    check decoded.peers.len == 0

  test "Decode cluster state with self only":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(
      nodeId: 42,
      host: "192.168.1.100",
      clientPort: 5000,
      webPort: 3000
    )
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.nodeId == 42
    check decoded.self.host == "192.168.1.100"
    check decoded.self.clientPort == 5000
    check decoded.self.webPort == 3000
    check decoded.peers.len == 0

  test "Decode cluster state with multiple peers":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "host1", clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: "host2", port: 9100)
    state.peers[3] = (host: "host3", port: 9200)
    state.peers[4] = (host: "host4", port: 9300)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.nodeId == 1
    check decoded.self.host == "host1"
    check decoded.self.clientPort == 9000
    check decoded.self.webPort == 8080
    check decoded.peers.len == 3
    check decoded.peers[2].host == "host2"
    check decoded.peers[2].port == 9100
    check decoded.peers[3].host == "host3"
    check decoded.peers[3].port == 9200
    check decoded.peers[4].host == "host4"
    check decoded.peers[4].port == 9300

  test "Round-trip with 5-node cluster":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "node1.example.com",
        clientPort: 7000, webPort: 8000)
    state.peers[2] = (host: "node2.example.com", port: 7100)
    state.peers[3] = (host: "node3.example.com", port: 7200)
    state.peers[4] = (host: "node4.example.com", port: 7300)
    state.peers[5] = (host: "node5.example.com", port: 7400)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.nodeId == state.self.nodeId
    check decoded.self.host == state.self.host
    check decoded.self.clientPort == state.self.clientPort
    check decoded.self.webPort == state.self.webPort
    check decoded.peers.len == state.peers.len
    for nodeId, info in state.peers.pairs():
      check decoded.peers.hasKey(nodeId)
      check decoded.peers[nodeId].host == info.host
      check decoded.peers[nodeId].port == info.port

  test "Round-trip with large nodeId values":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 4294967295'u32, host: "max-node",
        clientPort: 65535, webPort: 65534)
    state.peers[1000000'u32] = (host: "peer1", port: 50000)
    state.peers[2000000'u32] = (host: "peer2", port: 50001)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.nodeId == 4294967295'u32
    check decoded.peers.len == 2
    check decoded.peers[1000000'u32].host == "peer1"
    check decoded.peers[2000000'u32].host == "peer2"

  test "Decode invalid magic header raises":
    let badData = "XXXX\x01\x00\x00\x00\x00"
    expect ValueError:
      discard decodeClusterState(badData)

  test "Decode unsupported version raises":
    let badData = "\x43\x53\x42\x02\x00\x00\x00\x00" # version 2
    expect ValueError:
      discard decodeClusterState(badData)

  test "Decode truncated data raises":
    let truncated = "\x43\x53\x42\x01" # only header
    expect ValueError:
      discard decodeClusterState(truncated)

  test "Decode empty string raises":
    expect ValueError:
      discard decodeClusterState("")

  test "Utility functions":
    var state = newPersistedClusterState()
    state.peers[2] = (host: "host2", port: 9100)
    check state.getPeerCount() == 1
    check state.hasPeers() == true
    check state.getPeer(2).isSome()
    check state.getPeer(2).get().host == "host2"
    check state.getPeer(99).isNone()

  test "Utility functions with empty state":
    let state = newPersistedClusterState()
    check state.getPeerCount() == 0
    check state.hasPeers() == false
    check state.getPeer(1).isNone()

  test "Save and load from file":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "localhost", clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: "peerhost", port: 9100)
    let testPath = "/tmp/test_cluster_state_" & $getTime().toUnix() & ".bin"
    saveClusterStateToFile(state, testPath)
    let loaded = loadClusterStateFromFile(testPath)
    check loaded.isSome()
    let ls = loaded.get()
    check ls.self.nodeId == 1
    check ls.self.host == "localhost"
    check ls.peers.len == 1
    check ls.peers[2].host == "peerhost"
    # Cleanup
    removeFile(testPath)

  test "Load from non-existent file returns none":
    let loaded = loadClusterStateFromFile("/tmp/nonexistent_cluster.bin")
    check loaded.isNone()

  test "Encode with empty strings":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "", clientPort: 0, webPort: 0)
    state.peers[2] = (host: "", port: 0)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.host == ""
    check decoded.self.clientPort == 0
    check decoded.peers[2].host == ""
    check decoded.peers[2].port == 0

  test "Encode with unicode hostnames":
    var state = newPersistedClusterState()
    state.self = SelfNodeInfo(nodeId: 1, host: "主机.example.com",
        clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: "节点2.example.com", port: 9100)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.host == "主机.example.com"
    check decoded.peers[2].host == "节点2.example.com"

  test "Encode with long hostnames":
    var state = newPersistedClusterState()
    let longHost = "very-long-hostname-" & "x".repeat(200) & ".example.com"
    state.self = SelfNodeInfo(nodeId: 1, host: longHost, clientPort: 9000, webPort: 8080)
    state.peers[2] = (host: longHost, port: 9100)
    let encoded = encodeClusterState(state)
    let decoded = decodeClusterState(encoded)
    check decoded.self.host == longHost
    check decoded.peers[2].host == longHost
