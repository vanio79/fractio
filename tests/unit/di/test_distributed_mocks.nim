# Test Distributed Layer Mocks for DI
# Tests for MockRaftCoordinator, MockRaftTransport, etc.

import std/unittest
import std/options
import fractio/di/mocks
import fractio/core/types # For NodeID
from fractio/distributed/raft/group_types import GroupID, genGroupIDLocal,
    ZeroGroupID, `==`

suite "MockRaftCoordinator":
  test "newMockRaftCoordinator creates empty coordinator":
    let rc = newMockRaftCoordinator()
    check not rc.isRunning()
    check rc.hasGroup(genGroupIDLocal()) == false
    check rc.startCallCount == 0
    check rc.stopCallCount == 0

  test "start marks coordinator as running":
    let rc = newMockRaftCoordinator()
    rc.start()
    check rc.isRunning()
    check rc.startCallCount == 1

  test "stop marks coordinator as stopped":
    let rc = newMockRaftCoordinator()
    rc.start()
    rc.stop()
    check not rc.isRunning()
    check rc.stopCallCount == 1

  test "addGroup adds a group with leader":
    let rc = newMockRaftCoordinator()
    let groupId = genGroupIDLocal()
    rc.addGroup(groupId, 1)
    check rc.hasGroup(groupId)
    check rc.getLeader(groupId) == 1

  test "removeGroup removes a group":
    let rc = newMockRaftCoordinator()
    let groupId = genGroupIDLocal()
    rc.addGroup(groupId, 1)
    rc.removeGroup(groupId)
    check not rc.hasGroup(groupId)

  test "getLeader returns -1 for unknown group":
    let rc = newMockRaftCoordinator()
    check rc.getLeader(genGroupIDLocal()) == -1

  test "setLeader updates leader for group":
    let rc = newMockRaftCoordinator()
    let groupId = genGroupIDLocal()
    rc.addGroup(groupId, 1)
    rc.setLeader(groupId, 2)
    check rc.getLeader(groupId) == 2

  test "isLeader returns false for all groups":
    let rc = newMockRaftCoordinator()
    let groupId = genGroupIDLocal()
    rc.addGroup(groupId, 1)
    check rc.isLeader(groupId) == false

  test "reset clears all state":
    let rc = newMockRaftCoordinator()
    rc.start()
    rc.addGroup(genGroupIDLocal(), 1)
    rc.reset()
    check not rc.isRunning()
    check rc.startCallCount == 0
    check rc.stopCallCount == 0

suite "MockRaftTransport":
  test "newMockRaftTransport creates stopped transport":
    let rt = newMockRaftTransport()
    check not rt.isServerRunning()
    check rt.messagesSent.len == 0
    check rt.sendCallCount == 0

  test "startServer marks as running":
    let rt = newMockRaftTransport()
    rt.startServer()
    check rt.isServerRunning()

  test "stopServer marks as stopped":
    let rt = newMockRaftTransport()
    rt.startServer()
    rt.stopServer()
    check not rt.isServerRunning()

  test "send records message":
    let rt = newMockRaftTransport()
    let nodeId = NodeID("n1")
    let data = @[1'u8, 2'u8, 3'u8]
    check rt.send(nodeId, data)
    check rt.messagesSent.len == 1
    check rt.messagesSent[0].target == nodeId
    check rt.messagesSent[0].data == data
    check rt.sendCallCount == 1

  test "receiveMessage records incoming":
    let rt = newMockRaftTransport()
    let data = @[4'u8, 5'u8, 6'u8]
    rt.receiveMessage(data)
    check rt.messagesReceived.len == 1
    check rt.messagesReceived[0] == data

  test "reset clears all state":
    let rt = newMockRaftTransport()
    rt.startServer()
    discard rt.send(NodeID("n1"), @[1'u8])
    rt.reset()
    check not rt.isServerRunning()
    check rt.messagesSent.len == 0
    check rt.sendCallCount == 0

suite "MockRaftStateMachine":
  test "newMockRaftStateMachine creates empty state machine":
    let sm = newMockRaftStateMachine()
    check sm.getLastAppliedIndex() == 0
    check sm.appliedEntries.len == 0
    check sm.applyCallCount == 0

  test "apply adds entry and increments index":
    let sm = newMockRaftStateMachine()
    let data = @[1'u8, 2'u8, 3'u8]
    check sm.apply(data)
    check sm.getLastAppliedIndex() == 1
    check sm.appliedEntries.len == 1
    check sm.appliedEntries[0] == data

  test "multiple apply calls":
    let sm = newMockRaftStateMachine()
    discard sm.apply(@[1'u8])
    discard sm.apply(@[2'u8])
    discard sm.apply(@[3'u8])
    check sm.getLastAppliedIndex() == 3
    check sm.applyCallCount == 3

  test "snapshot returns concatenated entries":
    let sm = newMockRaftStateMachine()
    discard sm.apply(@[1'u8, 2'u8])
    discard sm.apply(@[3'u8, 4'u8])
    let snap = sm.snapshot()
    check snap == @[1'u8, 2'u8, 3'u8, 4'u8]
    check sm.snapshots.len == 1

  test "reset clears all state":
    let sm = newMockRaftStateMachine()
    discard sm.apply(@[1'u8])
    sm.reset()
    check sm.getLastAppliedIndex() == 0
    check sm.appliedEntries.len == 0
    check sm.snapshots.len == 0

suite "MockRaftLog":
  test "newMockRaftLog creates empty log":
    let rl = newMockRaftLog()
    check rl.getLastIndex() == 0
    check rl.getLastTerm() == 0

  test "append adds entry":
    let rl = newMockRaftLog()
    let idx = rl.append(1, @[1'u8, 2'u8])
    check idx == 1
    check rl.getLastIndex() == 1
    check rl.getLastTerm() == 1

  test "get returns entry at index":
    let rl = newMockRaftLog()
    discard rl.append(1, @[1'u8, 2'u8])
    let entry = rl.get(1)
    check entry.isSome()
    check entry.get() == @[1'u8, 2'u8]

  test "get returns none for missing index":
    let rl = newMockRaftLog()
    let entry = rl.get(99)
    check entry.isNone()

  test "truncate removes entries after index":
    let rl = newMockRaftLog()
    discard rl.append(1, @[1'u8])
    discard rl.append(1, @[2'u8])
    discard rl.append(2, @[3'u8])
    check rl.truncate(2)
    check rl.getLastIndex() == 2
    check rl.getLastTerm() == 1
    check rl.entries.len == 2

  test "truncate fails if index > lastIndex":
    let rl = newMockRaftLog()
    discard rl.append(1, @[1'u8])
    check not rl.truncate(99)

  test "reset clears log":
    let rl = newMockRaftLog()
    discard rl.append(1, @[1'u8])
    rl.reset()
    check rl.getLastIndex() == 0
    check rl.getLastTerm() == 0

suite "MockSpaceManager":
  test "newMockSpaceManager creates empty manager":
    let sm = newMockSpaceManager()
    check sm.listSpaces().len == 0
    check sm.createCallCount == 0
    check sm.dropCallCount == 0

  test "createSpace creates new space":
    let sm = newMockSpaceManager()
    let spaceId = sm.createSpace("test-space")
    check sm.createCallCount == 1
    check sm.listSpaces().len == 1
    # Check spaceId is in listSpaces using sequtils
    let spaces = sm.listSpaces()
    var found = false
    for s in spaces:
      if s == spaceId:
        found = true
        break
    check found

  test "getSpaceInfo returns space info":
    let sm = newMockSpaceManager()
    let spaceId = sm.createSpace("test-space")
    let info = sm.getSpaceInfo(spaceId)
    check info.isSome()

  test "getSpaceInfo returns none for unknown space":
    let sm = newMockSpaceManager()
    let info = sm.getSpaceInfo(genGroupIDLocal())
    check info.isNone()

  test "dropSpace removes space":
    let sm = newMockSpaceManager()
    let spaceId = sm.createSpace("test-space")
    check sm.dropSpace(spaceId)
    check sm.dropCallCount == 1
    check sm.listSpaces().len == 0

  test "dropSpace returns false for unknown space":
    let sm = newMockSpaceManager()
    check not sm.dropSpace(genGroupIDLocal())

  test "addSpace adds predefined space":
    let sm = newMockSpaceManager()
    let spaceId = genGroupIDLocal()
    sm.addSpace(spaceId, "predefined-space")
    check sm.listSpaces().len == 1
    # Check spaceId is in listSpaces
    let spaces = sm.listSpaces()
    var found = false
    for s in spaces:
      if s == spaceId:
        found = true
        break
    check found

  test "reset clears all spaces":
    let sm = newMockSpaceManager()
    discard sm.createSpace("test")
    sm.reset()
    check sm.listSpaces().len == 0
    check sm.createCallCount == 0

suite "MockNetworkTransport":
  test "newMockNetworkTransport creates disconnected transport":
    let nt = newMockNetworkTransport()
    check not nt.isConnected()
    check nt.messagesSent.len == 0
    check nt.connectCallCount == 0

  test "connect marks as connected":
    let nt = newMockNetworkTransport()
    check nt.connect("localhost", 8080'u16)
    check nt.isConnected()
    check nt.connectCallCount == 1
    check nt.currentHost == "localhost"
    check nt.currentPort == 8080

  test "disconnect marks as disconnected":
    let nt = newMockNetworkTransport()
    discard nt.connect("localhost", 8080'u16)
    nt.disconnect()
    check not nt.isConnected()
    check nt.disconnectCallCount == 1

  test "send works when connected":
    let nt = newMockNetworkTransport()
    discard nt.connect("localhost", 8080'u16)
    let data = @[1'u8, 2'u8, 3'u8]
    check nt.send(data)
    check nt.messagesSent.len == 1
    check nt.messagesSent[0] == data

  test "send fails when disconnected":
    let nt = newMockNetworkTransport()
    check not nt.send(@[1'u8])
    check nt.messagesSent.len == 0

  test "recv returns queued messages":
    let nt = newMockNetworkTransport()
    discard nt.connect("localhost", 8080'u16)
    nt.queueReceive(@[1'u8, 2'u8])
    nt.queueReceive(@[3'u8, 4'u8])
    let msg1 = nt.recv(1000)
    check msg1.isSome()
    check msg1.get() == @[1'u8, 2'u8]
    let msg2 = nt.recv(1000)
    check msg2.isSome()
    check msg2.get() == @[3'u8, 4'u8]

  test "recv returns none when no messages":
    let nt = newMockNetworkTransport()
    discard nt.connect("localhost", 8080'u16)
    let msg = nt.recv(1000)
    check msg.isNone()

  test "recv returns none when disconnected":
    let nt = newMockNetworkTransport()
    nt.queueReceive(@[1'u8])
    let msg = nt.recv(1000)
    check msg.isNone()

  test "reset clears all state":
    let nt = newMockNetworkTransport()
    discard nt.connect("localhost", 8080'u16)
    discard nt.send(@[1'u8])
    nt.reset()
    check not nt.isConnected()
    check nt.messagesSent.len == 0
    check nt.connectCallCount == 0
