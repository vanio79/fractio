# Unit tests for fractio/protocol/messages/space.nim
# Tests CreateSpace, DropSpace encoding/decoding

import std/unittest
import fractio/protocol/messages/space
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types
import fractio/distributed/raft/group_types

# =============================================================================
# CreateSpace Tests
# =============================================================================

suite "CreateSpaceRequest/CreateSpaceResponse":

  test "encodeCreateSpaceRequest basic":
    let req = CreateSpaceRequest(name: "test-space", replicas: 3'i32)
    let encoded = encodeCreateSpaceRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtCreateSpace)

  test "encodeCreateSpaceRequest zero replicas":
    let req = CreateSpaceRequest(name: "all-nodes", replicas: 0'i32)
    let encoded = encodeCreateSpaceRequest(req)
    check encoded.len > 2

  test "encodeCreateSpaceRequest large replicas":
    let req = CreateSpaceRequest(name: "large-repl", replicas: 100'i32)
    let encoded = encodeCreateSpaceRequest(req)
    check encoded.len > 2

  test "encodeCreateSpaceRequest empty name":
    let req = CreateSpaceRequest(name: "", replicas: 1'i32)
    let encoded = encodeCreateSpaceRequest(req)
    check encoded.len > 2

  test "encodeCreateSpaceRequest long name":
    let req = CreateSpaceRequest(name: "very-long-space-name-here",
        replicas: 5'i32)
    let encoded = encodeCreateSpaceRequest(req)
    check encoded.len > 2

  test "decodeCreateSpaceRequest roundtrip":
    let req = CreateSpaceRequest(name: "my-space", replicas: 7'i32)
    let encoded = encodeCreateSpaceRequest(req)
    let decoded = decodeCreateSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == req.name
    check decoded.value.replicas == req.replicas

  test "decodeCreateSpaceRequest zero replicas roundtrip":
    let req = CreateSpaceRequest(name: "zero-repl", replicas: 0'i32)
    let encoded = encodeCreateSpaceRequest(req)
    let decoded = decodeCreateSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.replicas == 0'i32

  test "decodeCreateSpaceRequest empty name roundtrip":
    let req = CreateSpaceRequest(name: "", replicas: 3'i32)
    let encoded = encodeCreateSpaceRequest(req)
    let decoded = decodeCreateSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == ""

  test "decodeCreateSpaceRequest truncated":
    let truncated = "\x07\x08"
    let decoded = decodeCreateSpaceRequest(truncated)
    check decoded.isErr

  test "decodeCreateSpaceRequest missing replicas":
    let truncated = "\x07\x08\x00\x03ABC" # MT + nameLen + name only
    let decoded = decodeCreateSpaceRequest(truncated)
    check decoded.isErr

  test "encodeCreateSpaceResponse success empty groups":
    let spaceId = genSpaceID()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 0'i32,
      spaceRecord: "space-data",
      groupRecords: @[]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    check encoded.len > 2

  test "encodeCreateSpaceResponse success with groups":
    let spaceId = genSpaceID()
    let groupId1 = genULID()
    let groupId2 = genULID()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 2'i32,
      spaceRecord: "space-record-data",
      groupRecords: @[
        GroupRecordData(groupId: groupId1, record: "group1-data"),
        GroupRecordData(groupId: groupId2, record: "group2-data")
      ]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    check encoded.len > 2

  test "encodeCreateSpaceResponse failure":
    let resp = CreateSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      groupCount: 0'i32,
      spaceRecord: "",
      groupRecords: @[],
      error: "Space already exists"
    )
    let encoded = encodeCreateSpaceResponse(resp)
    check encoded.len > 2

  test "encodeCreateSpaceResponse failure long error":
    let resp = CreateSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      groupCount: 0'i32,
      spaceRecord: "",
      groupRecords: @[],
      error: "Failed to create space: maximum group limit exceeded"
    )
    let encoded = encodeCreateSpaceResponse(resp)
    check encoded.len > 2

  test "decodeCreateSpaceResponse roundtrip success empty":
    let spaceId = genSpaceID()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 0'i32,
      spaceRecord: "test-record",
      groupRecords: @[]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    let decoded = decodeCreateSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.spaceId == spaceId
    check decoded.value.groupCount == 0'i32
    check decoded.value.spaceRecord == "test-record"
    check decoded.value.groupRecords.len == 0

  test "decodeCreateSpaceResponse roundtrip success with groups":
    let spaceId = genSpaceID()
    let groupId1 = genULID()
    let groupId2 = genULID()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 2'i32,
      spaceRecord: "space-rec",
      groupRecords: @[
        GroupRecordData(groupId: groupId1, record: "g1"),
        GroupRecordData(groupId: groupId2, record: "g2")
      ]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    let decoded = decodeCreateSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.spaceId == spaceId
    check decoded.value.groupCount == 2'i32
    check decoded.value.groupRecords.len == 2
    check decoded.value.groupRecords[0].groupId == groupId1
    check decoded.value.groupRecords[0].record == "g1"
    check decoded.value.groupRecords[1].groupId == groupId2

  test "decodeCreateSpaceResponse roundtrip failure":
    let resp = CreateSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      groupCount: 0'i32,
      spaceRecord: "",
      groupRecords: @[],
      error: "Invalid space name"
    )
    let encoded = encodeCreateSpaceResponse(resp)
    let decoded = decodeCreateSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Invalid space name"

  test "decodeCreateSpaceResponse truncated":
    let truncated = "\x07\x08\x01" # MT + success byte only
    let decoded = decodeCreateSpaceResponse(truncated)
    check decoded.isErr

  test "decodeCreateSpaceResponse truncated after success":
    let truncated = "\x07\x08\x01\x00" # MT + success + partial spaceId
    let decoded = decodeCreateSpaceResponse(truncated)
    check decoded.isErr

# =============================================================================
# DropSpace Tests
# =============================================================================

suite "DropSpaceRequest/DropSpaceResponse":

  test "encodeDropSpaceRequest basic":
    let req = DropSpaceRequest(name: "my-space")
    let encoded = encodeDropSpaceRequest(req)
    check encoded.len > 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtDropSpace)

  test "encodeDropSpaceRequest empty name":
    let req = DropSpaceRequest(name: "")
    let encoded = encodeDropSpaceRequest(req)
    check encoded.len == 4 # MT + empty name length (2 bytes)

  test "encodeDropSpaceRequest long name":
    let req = DropSpaceRequest(name: "very-long-space-name-to-drop")
    let encoded = encodeDropSpaceRequest(req)
    check encoded.len > 2

  test "decodeDropSpaceRequest roundtrip":
    let req = DropSpaceRequest(name: "test-space-123")
    let encoded = encodeDropSpaceRequest(req)
    let decoded = decodeDropSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == req.name

  test "decodeDropSpaceRequest empty name roundtrip":
    let req = DropSpaceRequest(name: "")
    let encoded = encodeDropSpaceRequest(req)
    let decoded = decodeDropSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == ""

  test "decodeDropSpaceRequest truncated":
    let truncated = "\x07\x09"
    let decoded = decodeDropSpaceRequest(truncated)
    check decoded.isErr

  test "decodeDropSpaceRequest missing name":
    let truncated = "\x07\x09\x00\x05" # MT + nameLen but no name data
    let decoded = decodeDropSpaceRequest(truncated)
    check decoded.isErr

  test "encodeDropSpaceResponse success empty groups":
    let spaceId = genSpaceID()
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[]
    )
    let encoded = encodeDropSpaceResponse(resp)
    check encoded.len > 2

  test "encodeDropSpaceResponse success with groups":
    let spaceId = genSpaceID()
    let groupId1 = groupIDFromULID(genULID())
    let groupId2 = groupIDFromULID(genULID())
    let groupId3 = groupIDFromULID(genULID())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupId1, groupId2, groupId3]
    )
    let encoded = encodeDropSpaceResponse(resp)
    check encoded.len > 2

  test "encodeDropSpaceResponse failure":
    let resp = DropSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      deletedGroupIds: @[],
      error: "Space not found"
    )
    let encoded = encodeDropSpaceResponse(resp)
    check encoded.len > 2

  test "encodeDropSpaceResponse failure long error":
    let resp = DropSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      deletedGroupIds: @[],
      error: "Cannot drop space: space contains active transactions"
    )
    let encoded = encodeDropSpaceResponse(resp)
    check encoded.len > 2

  test "decodeDropSpaceResponse roundtrip success empty":
    let spaceId = genSpaceID()
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[]
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.spaceId == spaceId
    check decoded.value.deletedGroupIds.len == 0

  test "decodeDropSpaceResponse roundtrip success with groups":
    let spaceId = genSpaceID()
    let groupId1 = groupIDFromULID(genULID())
    let groupId2 = groupIDFromULID(genULID())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupId1, groupId2]
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.spaceId == spaceId
    check decoded.value.deletedGroupIds.len == 2
    check decoded.value.deletedGroupIds[0] == groupId1
    check decoded.value.deletedGroupIds[1] == groupId2

  test "decodeDropSpaceResponse roundtrip failure":
    let resp = DropSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      deletedGroupIds: @[],
      error: "Space is in use"
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Space is in use"

  test "decodeDropSpaceResponse truncated":
    let truncated = "\x07\x09\x01" # MT + success only
    let decoded = decodeDropSpaceResponse(truncated)
    check decoded.isErr

  test "decodeDropSpaceResponse truncated after success":
    let truncated = "\x07\x09\x01\x00\x00" # MT + success + partial spaceId
    let decoded = decodeDropSpaceResponse(truncated)
    check decoded.isErr

# =============================================================================
# Integration Tests
# =============================================================================

suite "Space Message Integration":

  test "CreateSpace full roundtrip":
    let req = CreateSpaceRequest(name: "production-space", replicas: 5'i32)
    let reqEncoded = encodeCreateSpaceRequest(req)
    let reqDecoded = decodeCreateSpaceRequest(reqEncoded)
    check reqDecoded.isOk
    check reqDecoded.value.name == "production-space"
    check reqDecoded.value.replicas == 5'i32

    let spaceId = genSpaceID()
    let groupId = genULID()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 1'i32,
      spaceRecord: "prod-space-record",
      groupRecords: @[GroupRecordData(groupId: groupId, record: "prod-group")]
    )
    let respEncoded = encodeCreateSpaceResponse(resp)
    let respDecoded = decodeCreateSpaceResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == true
    check respDecoded.value.spaceId == spaceId

  test "DropSpace full roundtrip":
    let req = DropSpaceRequest(name: "old-space")
    let reqEncoded = encodeDropSpaceRequest(req)
    let reqDecoded = decodeDropSpaceRequest(reqEncoded)
    check reqDecoded.isOk
    check reqDecoded.value.name == "old-space"

    let spaceId = genSpaceID()
    let groupId = groupIDFromULID(genULID())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupId]
    )
    let respEncoded = encodeDropSpaceResponse(resp)
    let respDecoded = decodeDropSpaceResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == true

  test "CreateSpace failure scenario":
    let req = CreateSpaceRequest(name: "duplicate-space", replicas: 3'i32)
    let reqEncoded = encodeCreateSpaceRequest(req)
    let reqDecoded = decodeCreateSpaceRequest(reqEncoded)
    check reqDecoded.isOk

    let resp = CreateSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      groupCount: 0'i32,
      spaceRecord: "",
      groupRecords: @[],
      error: "Space name already exists"
    )
    let respEncoded = encodeCreateSpaceResponse(resp)
    let respDecoded = decodeCreateSpaceResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == false
    check respDecoded.value.error == "Space name already exists"

  test "DropSpace failure scenario":
    let req = DropSpaceRequest(name: "nonexistent-space")
    let reqEncoded = encodeDropSpaceRequest(req)
    let reqDecoded = decodeDropSpaceRequest(reqEncoded)
    check reqDecoded.isOk

    let resp = DropSpaceResponse(
      success: false,
      spaceId: zeroSpaceID(),
      deletedGroupIds: @[],
      error: "Space does not exist"
    )
    let respEncoded = encodeDropSpaceResponse(resp)
    let respDecoded = decodeDropSpaceResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.success == false
    check respDecoded.value.error == "Space does not exist"
