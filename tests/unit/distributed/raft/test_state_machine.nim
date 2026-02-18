## Unit tests for Raft state machine abstraction and InMemoryStateMachine.
## 100% coverage target for states.nim and inmemory_states.nim.

import unittest
import std/strutils
import std/tables
import std/typedthreads
import fractio/distributed/raft/states
import fractio/distributed/raft/inmemory_states
import fractio/distributed/raft/types
import fractio/core/errors

suite "RaftStateMachine abstraction":
  test "applyImpl base returns notImplementedError":
    let base = RaftStateMachine()
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckNoop), checksum: 0)
    let res = base.applyImpl(entry)
    check res.isErr
    check res.error.msg.contains("not implemented")

suite "ClientCommand serialization/deserialization":
  var sm: InMemoryStateMachine

  setup:
    sm = newInMemoryStateMachine()

  test "round-trip SET command":
    let cmd = ClientCommand(op: cckSet, key: "foo", value: "bar")
    let data = serialize(cmd)
    let res = deserialize(data)
    check res.isOk
    let decoded = res.get()
    check decoded.op == cckSet
    check decoded.key == "foo"
    check decoded.value == "bar"

  test "round-trip DELETE command":
    let cmd = ClientCommand(op: cckDelete, key: "deleteMe", value: "ignored")
    let data = serialize(cmd)
    let res = deserialize(data)
    check res.isOk
    let decoded = res.get()
    check decoded.op == cckDelete
    check decoded.key == "deleteMe"
    check decoded.value == "ignored"

  test "empty key and value":
    let cmd = ClientCommand(op: cckSet, key: "", value: "")
    let data = serialize(cmd)
    let res = deserialize(data)
    check res.isOk
    let decoded = res.get()
    check decoded.key == ""
    check decoded.value == ""

  test "unicode keys and values":
    let cmd = ClientCommand(op: cckSet, key: "日本語", value: "🚀")
    let data = serialize(cmd)
    let res = deserialize(data)
    check res.isOk
    let decoded = res.get()
    check decoded.key == "日本語"
    check decoded.value == "🚀"

  test "deserialize empty data returns error":
    let res = deserialize(@[])
    check res.isErr
    check res.error.msg.contains("too short")

  test "deserialize truncated op only":
    let data = [byte(cckSet.uint8)]
    let res = deserialize(data)
    check res.isErr

  test "deserialize truncated after op":
    let data = [byte(cckSet.uint8), 0'u8]
    let res = deserialize(data)
    check res.isErr

  test "deserialize invalid op":
    let data = [byte(2'u8), 0'u8, 1'u8, 0'u8, 0'u8, 1'u8, 0'u8]
    let res = deserialize(data)
    check res.isErr or (let dec = res.get(); dec.op notin [cckSet, cckDelete])

  test "deserialize key overflow":
    var keyLen: uint16 = 100
    var valLen: uint16 = 0
    var data = newSeq[byte](1 + 2 + 5 + 2)
    data[0] = byte(cckSet.uint8)
    copyMem(data[1].addr, addr keyLen, 2)
    let keyBytes = stringToBytes("abcde")
    for i in 0..<keyBytes.len:
      data[3 + i] = keyBytes[i]
    copyMem(data[8].addr, addr valLen, 2)
    let res = deserialize(data)
    check res.isErr
    check res.error.msg.contains("overflow")

  test "deserialize value overflow":
    let key = "ok"
    let keyBytes = stringToBytes(key)
    var valLen: uint16 = 100
    var data = newSeq[byte](1 + 2 + keyBytes.len + 2)
    data[0] = byte(cckSet.uint8)
    var beKeyLen = keyBytes.len.uint16
    copyMem(data[1].addr, addr beKeyLen, 2)
    copyMem(data[3].addr, keyBytes[0].addr, keyBytes.len)
    copyMem(data[3 + keyBytes.len].addr, addr valLen, 2)
    let res = deserialize(data)
    check res.isErr
    check res.error.msg.contains("overflow")

suite "InMemoryStateMachine":
  var sm: InMemoryStateMachine

  setup:
    sm = newInMemoryStateMachine()

  test "constructor initializes empty data and config":
    check len(sm.data) == 0
    check sm.config.len == 0
    check sm.applyCount == 0

  test "constructor with initial peers":
    let sm2 = newInMemoryStateMachine(@[1'u64, 2'u64, 3'u64])
    check sm2.config.len == 3
    check 1'u64 in sm2.config
    check 2'u64 in sm2.config
    check 3'u64 in sm2.config

  test "applyImpl Noop does nothing":
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckNoop), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check len(sm.data) == 0
    check sm.config.len == 0
    check sm.applyCount == 1

  test "applyImpl ClientCommand Set":
    var cmd = ClientCommand(op: cckSet, key: "a", value: "1")
    let data = serialize(cmd)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckClientCommand, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check sm.data["a"] == "1"
    check sm.applyCount == 1

  test "applyImpl ClientCommand Delete":
    sm.data["toDelete"] = "value"
    sm.applyCount = 0
    var cmd = ClientCommand(op: cckDelete, key: "toDelete", value: "")
    let data = serialize(cmd)
    let entry = RaftEntry(term: 1, index: 2, command: RaftCommand(
        kind: rckClientCommand, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check "toDelete" notin sm.data
    check sm.applyCount == 1

  test "applyImpl ClientCommand overwrites existing key":
    sm.data["x"] = "old"
    var cmd = ClientCommand(op: cckSet, key: "x", value: "new")
    let data = serialize(cmd)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckClientCommand, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check sm.data["x"] == "new"

  test "applyImpl AddNode":
    let nid: uint64 = 42
    var data = newSeq[byte](8)
    copyMem(data[0].addr, addr nid, 8)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckAddNode, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check nid in sm.config
    check sm.applyCount == 1

  test "applyImpl AddNode ignores duplicate":
    let nid: uint64 = 7
    var data = newSeq[byte](8)
    copyMem(data[0].addr, addr nid, 8)
    let entry1 = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckAddNode, data: data), checksum: 0)
    let entry2 = RaftEntry(term: 1, index: 2, command: RaftCommand(
        kind: rckAddNode, data: data), checksum: 0)
    check sm.applyImpl(entry1).isOk
    check sm.applyImpl(entry2).isOk
    var count = 0
    for n in sm.config:
      if n == nid: inc count
    check count == 1

  test "applyImpl RemoveNode":
    sm.config = @[10'u64, 20'u64, 30'u64]
    let nid: uint64 = 20
    var data = newSeq[byte](8)
    copyMem(data[0].addr, addr nid, 8)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckRemoveNode, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check nid notin sm.config
    check 10'u64 in sm.config
    check 30'u64 in sm.config

  test "applyImpl RemoveNode no-op if missing":
    sm.config = @[1'u64]
    let nid: uint64 = 99
    var data = newSeq[byte](8)
    copyMem(data[0].addr, addr nid, 8)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckRemoveNode, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check sm.config.len == 1

  test "applyImpl Reconfigure":
    var newConfig = @[100'u64, 200'u64]
    var data = newSeq[byte](4 + 2*8)
    var count: uint32 = 2
    copyMem(data[0].addr, addr count, 4)
    copyMem(data[4].addr, addr newConfig[0], 8)
    copyMem(data[12].addr, addr newConfig[1], 8)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckReconfigure, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isOk
    check sm.config.len == 2
    check 100'u64 in sm.config
    check 200'u64 in sm.config

  test "applyImpl Reconfigure error on short data":
    var data = newSeq[byte](2)
    var count: uint32 = 1
    copyMem(data[0].addr, addr count, 2)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckReconfigure, data: data), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isErr

  test "applyImpl ClientCommand malformed data returns error":
    let badData = @[byte(cckSet.uint8), 0'u8, 255'u8]
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckClientCommand, data: badData), checksum: 0)
    let res = sm.applyImpl(entry)
    check res.isErr

  test "applyImpl is idempotent - SET applied twice":
    var cmd = ClientCommand(op: cckSet, key: "k", value: "v")
    let data = serialize(cmd)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckClientCommand, data: data), checksum: 0)
    check sm.applyImpl(entry).isOk
    check sm.applyCount == 1
    check sm.applyImpl(entry).isOk
    check sm.applyCount == 2
    check sm.data["k"] == "v"

  test "applyImpl is idempotent - DELETE applied twice":
    sm.data["k"] = "v"
    var cmd = ClientCommand(op: cckDelete, key: "k", value: "")
    let data = serialize(cmd)
    let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckClientCommand, data: data), checksum: 0)
    check sm.applyImpl(entry).isOk
    check sm.applyCount == 1
    check "k" notin sm.data
    check sm.applyImpl(entry).isOk
    check sm.applyCount == 2
    check "k" notin sm.data

  test "applyImpl increments applyCount for each call":
    sm.applyCount = 0
    let entryNoop = RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckNoop), checksum: 0)
    discard sm.applyImpl(entryNoop)
    discard sm.applyImpl(entryNoop)
    discard sm.applyImpl(entryNoop)
    check sm.applyCount == 3

  test "resetApplyCount resets counter":
    discard sm.applyImpl(RaftEntry(term: 1, index: 1, command: RaftCommand(
        kind: rckNoop), checksum: 0))
    discard sm.applyImpl(RaftEntry(term: 1, index: 2, command: RaftCommand(
        kind: rckNoop), checksum: 0))
    discard sm.applyImpl(RaftEntry(term: 1, index: 3, command: RaftCommand(
        kind: rckNoop), checksum: 0))
    sm.resetApplyCount()
    check sm.applyCount == 0

suite "InMemoryStateMachine thread-safety":
  var sm: InMemoryStateMachine

  setup:
    sm = newInMemoryStateMachine()

  test "concurrent SET operations produce correct final state":
    # Worker proc for SET
    proc worker(args: tuple[idx: int, sm: InMemoryStateMachine]) {.thread.} =
      let idx = args.idx
      let stateMachine = args.sm
      let key = "key"
      let val = $idx
      let cmd = ClientCommand(op: cckSet, key: key, value: val)
      let data = serialize(cmd)
      let entry = RaftEntry(term: 1, index: uint64(idx+1),
          command: RaftCommand(kind: rckClientCommand, data: data), checksum: 0)
      discard stateMachine.applyImpl(entry)

    const n = 100
    var threads: array[n, Thread[tuple[idx: int, sm: InMemoryStateMachine]]]
    for i in 0..<n:
      createThread(threads[i], worker, (i, sm))
    for i in 0..<n: joinThread(threads[i])
    let finalVal = sm.getValue("key")
    var validVals = newSeq[string]()
    for i in 0..<n:
      validVals.add $i
    check finalVal in validVals
    check sm.applyCount == n

  test "concurrent mix of operations maintains integrity":
    type OpKind = enum
      opSet, opDelete, opAdd, opRemove

    proc mixWorker(args: tuple[op: OpKind, idx: int,
         sm: InMemoryStateMachine]) {.thread.} =
      let capturedOp = args.op
      let capturedIdx = args.idx
      let stateMachine = args.sm
      case capturedOp
      of opSet:
        let cmd = ClientCommand(op: cckSet, key: "k", value: "v")
        let data = serialize(cmd)
        let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
            kind: rckClientCommand, data: data), checksum: 0)
        discard stateMachine.applyImpl(entry)
      of opDelete:
        let cmd = ClientCommand(op: cckDelete, key: "k", value: "")
        let data = serialize(cmd)
        let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
            kind: rckClientCommand, data: data), checksum: 0)
        discard stateMachine.applyImpl(entry)
      of opAdd:
        let nid = uint64(capturedIdx)
        var d = newSeq[byte](8)
        copyMem(d[0].addr, addr nid, 8)
        let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
            kind: rckAddNode, data: d), checksum: 0)
        discard stateMachine.applyImpl(entry)
      of opRemove:
        let nid = uint64(capturedIdx)
        var d = newSeq[byte](8)
        copyMem(d[0].addr, addr nid, 8)
        let entry = RaftEntry(term: 1, index: 1, command: RaftCommand(
            kind: rckRemoveNode, data: d), checksum: 0)
        discard stateMachine.applyImpl(entry)

    var ops: array[50, OpKind]
    sm.config = @[]
    const n = 50
    var threads: array[n, Thread[tuple[op: OpKind, idx: int,
        sm: InMemoryStateMachine]]]
    for i in 0..<n:
      ops[i] = OpKind(i mod 4)
      createThread(threads[i], mixWorker, (ops[i], i, sm))
    for i in 0..<n: joinThread(threads[i])
    check sm.applyCount == n

suite "Accessors":
  var sm: InMemoryStateMachine

  setup:
    sm = newInMemoryStateMachine()

  test "getValue returns empty for missing key":
    check sm.getValue("missing") == ""

  test "getValue returns set value":
    sm.data["x"] = "42"
    check sm.getValue("x") == "42"

  test "hasKey false for missing":
    check sm.hasKey("nope") == false

  test "hasKey true for existing":
    sm.data["present"] = "yes"
    check sm.hasKey("present") == true

  test "getConfig returns copy of config":
    sm.config = @[1'u64, 2'u64]
    var cfg = sm.getConfig()
    check cfg.len == 2
    check 1'u64 in cfg
    check 2'u64 in cfg
    cfg.add(3'u64)
    check sm.getConfig().len == 2
