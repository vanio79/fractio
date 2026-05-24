# Unit tests for fractio/protocol/messages/space.nim
# Tests CreateSpace and DropSpace encoding/decoding

import std/[unittest, strutils]
import fractio/protocol/messages/space
import fractio/protocol/types
import fractio/protocol/codec
import fractio/core/types
import fractio/distributed/raft/group_types

suite "CreateSpace Messages":

  test "encodeCreateSpaceRequest basic":
    let req = CreateSpaceRequest(name: "test_space", replicas: 3'i32)
    let encoded = encodeCreateSpaceRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtCreateSpace)
    let name = readBytes16(encoded, pos)
    check name.isOk
    check name.value == "test_space"
    let replicas = readInt32BE(encoded, pos)
    check replicas.isOk
    check replicas.value == 3'i32

  test "encodeCreateSpaceRequest zero replicas":
    let req = CreateSpaceRequest(name: "default_space", replicas: 0'i32)
    let encoded = encodeCreateSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readBytes16(encoded, pos)
    let replicas = readInt32BE(encoded, pos)
    check replicas.value == 0'i32

  test "encodeCreateSpaceRequest negative replicas":
    # -1 means "all nodes" in some implementations
    let req = CreateSpaceRequest(name: "all_nodes_space", replicas: -1'i32)
    let encoded = encodeCreateSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    discard readBytes16(encoded, pos)
    let replicas = readInt32BE(encoded, pos)
    check replicas.value == -1'i32

  test "encodeCreateSpaceRequest empty name":
    let req = CreateSpaceRequest(name: "", replicas: 1'i32)
    let encoded = encodeCreateSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let name = readBytes16(encoded, pos)
    check name.value == ""

  test "encodeCreateSpaceRequest long name":
    let longName = "x".repeat(255)
    let req = CreateSpaceRequest(name: longName, replicas: 5'i32)
    let encoded = encodeCreateSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let name = readBytes16(encoded, pos)
    check name.value == longName

  test "decodeCreateSpaceRequest valid":
    let req = CreateSpaceRequest(name: "my_space", replicas: 3'i32)
    let encoded = encodeCreateSpaceRequest(req)
    let decoded = decodeCreateSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == "my_space"
    check decoded.value.replicas == 3'i32

  test "decodeCreateSpaceRequest truncated name":
    let invalid = "\x07\x08" # Just message type
    let decoded = decodeCreateSpaceRequest(invalid)
    check decoded.isErr

  test "decodeCreateSpaceRequest truncated replicas":
    let name = "test"
    let nameLen = uint16(name.len)
    let invalid = "\x07\x08" &
      char(nameLen shr 8) & char(nameLen and 0xFF) & name
    let decoded = decodeCreateSpaceRequest(invalid)
    check decoded.isErr

  test "encodeCreateSpaceResponse success":
    let spaceId = SpaceID(genULIDLocal())
    let groupId = genULIDLocal()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 1'i32,
      spaceRecord: "space_record_data",
      groupRecords: @[GroupRecordData(groupId: groupId,
          record: "group_record_data")]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtCreateSpace)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeCreateSpaceResponse success no groups":
    let spaceId = SpaceID(genULIDLocal())
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 0'i32,
      spaceRecord: "",
      groupRecords: @[]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeCreateSpaceResponse failure":
    let resp = CreateSpaceResponse(
      success: false,
      error: "Space already exists"
    )
    let encoded = encodeCreateSpaceResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8
    let err = readBytes16(encoded, pos)
    check err.isOk
    check err.value == "Space already exists"

  test "encodeCreateSpaceResponse failure empty error":
    let resp = CreateSpaceResponse(
      success: false,
      error: ""
    )
    let encoded = encodeCreateSpaceResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8
    let err = readBytes16(encoded, pos)
    check err.value == ""

  test "decodeCreateSpaceResponse success":
    let spaceId = SpaceID(genULIDLocal())
    let groupId = genULIDLocal()
    let resp = CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 2'i32,
      spaceRecord: "record1",
      groupRecords: @[
        GroupRecordData(groupId: groupId, record: "g1"),
        GroupRecordData(groupId: genULIDLocal(), record: "g2")
      ]
    )
    let encoded = encodeCreateSpaceResponse(resp)
    let decoded = decodeCreateSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.groupCount == 2'i32
    check decoded.value.spaceRecord == "record1"
    check decoded.value.groupRecords.len == 2

  test "decodeCreateSpaceResponse failure":
    let resp = CreateSpaceResponse(
      success: false,
      error: "Invalid space name"
    )
    let encoded = encodeCreateSpaceResponse(resp)
    let decoded = decodeCreateSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Invalid space name"

  test "decodeCreateSpaceResponse truncated success":
    let invalid = "\x07\x08" # Just message type
    let decoded = decodeCreateSpaceResponse(invalid)
    check decoded.isErr

  test "decodeCreateSpaceResponse truncated spaceId":
    let invalid = "\x07\x08\x01" # Message type + success byte
    let decoded = decodeCreateSpaceResponse(invalid)
    check decoded.isErr

  test "decodeCreateSpaceResponse truncated error":
    let invalid = "\x07\x08\x00" # Message type + success=0, no error
    let decoded = decodeCreateSpaceResponse(invalid)
    check decoded.isErr

  test "CreateSpace request roundtrip":
    for name in ["", "test", "my_space_123"]:
      for replicas in [-1'i32, 0'i32, 1'i32, 3'i32, 10'i32]:
        let req = CreateSpaceRequest(name: name, replicas: replicas)
        let encoded = encodeCreateSpaceRequest(req)
        let decoded = decodeCreateSpaceRequest(encoded)
        check decoded.isOk
        check decoded.value.name == name
        check decoded.value.replicas == replicas

suite "DropSpace Messages":

  test "encodeDropSpaceRequest":
    let req = DropSpaceRequest(name: "old_space")
    let encoded = encodeDropSpaceRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtDropSpace)
    let name = readBytes16(encoded, pos)
    check name.isOk
    check name.value == "old_space"

  test "encodeDropSpaceRequest empty name":
    let req = DropSpaceRequest(name: "")
    let encoded = encodeDropSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let name = readBytes16(encoded, pos)
    check name.value == ""

  test "encodeDropSpaceRequest long name":
    let longName = "space_" & "x".repeat(250)
    let req = DropSpaceRequest(name: longName)
    let encoded = encodeDropSpaceRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let name = readBytes16(encoded, pos)
    check name.value == longName

  test "decodeDropSpaceRequest valid":
    let req = DropSpaceRequest(name: "to_delete")
    let encoded = encodeDropSpaceRequest(req)
    let decoded = decodeDropSpaceRequest(encoded)
    check decoded.isOk
    check decoded.value.name == "to_delete"

  test "decodeDropSpaceRequest truncated":
    let invalid = "\x07\x09" # Just message type
    let decoded = decodeDropSpaceRequest(invalid)
    check decoded.isErr

  test "encodeDropSpaceResponse success with groups":
    let spaceId = SpaceID(genULIDLocal())
    let groupId1 = groupIDFromULID(genULIDLocal())
    let groupId2 = groupIDFromULID(genULIDLocal())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupId1, groupId2]
    )
    let encoded = encodeDropSpaceResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtDropSpace)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeDropSpaceResponse success no groups":
    let spaceId = SpaceID(genULIDLocal())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[]
    )
    let encoded = encodeDropSpaceResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x01'u8

  test "encodeDropSpaceResponse failure":
    let resp = DropSpaceResponse(
      success: false,
      error: "Space not found"
    )
    let encoded = encodeDropSpaceResponse(resp)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let success = readUint8(encoded, pos)
    check success.value == 0x00'u8
    let err = readBytes16(encoded, pos)
    check err.value == "Space not found"

  test "decodeDropSpaceResponse success":
    let spaceId = SpaceID(genULIDLocal())
    let groupId = groupIDFromULID(genULIDLocal())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupId]
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == true
    check decoded.value.deletedGroupIds.len == 1

  test "decodeDropSpaceResponse success multiple groups":
    let spaceId = SpaceID(genULIDLocal())
    let resp = DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[groupIDFromULID(genULIDLocal()), groupIDFromULID(genULIDLocal()),
          GroupID(genULIDLocal())]
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.deletedGroupIds.len == 3

  test "decodeDropSpaceResponse failure":
    let resp = DropSpaceResponse(
      success: false,
      error: "Cannot drop default space"
    )
    let encoded = encodeDropSpaceResponse(resp)
    let decoded = decodeDropSpaceResponse(encoded)
    check decoded.isOk
    check decoded.value.success == false
    check decoded.value.error == "Cannot drop default space"

  test "decodeDropSpaceResponse truncated success":
    let invalid = "\x07\x09" # Just message type
    let decoded = decodeDropSpaceResponse(invalid)
    check decoded.isErr

  test "decodeDropSpaceResponse truncated spaceId":
    let invalid = "\x07\x09\x01" # Message type + success, no spaceId
    let decoded = decodeDropSpaceResponse(invalid)
    check decoded.isErr

  test "decodeDropSpaceResponse truncated groupId":
    let spaceId = SpaceID(genULIDLocal())
    let spaceBytes = spaceIDToBytes(spaceId)
    let invalid = "\x07\x09\x01" & spaceBytes &
        "\x00\x00\x00\x01" # success + spaceId + count=1, but no groupId
    let decoded = decodeDropSpaceResponse(invalid)
    check decoded.isErr

  test "DropSpace request roundtrip":
    for name in ["", "test", "space_to_drop"]:
      let req = DropSpaceRequest(name: name)
      let encoded = encodeDropSpaceRequest(req)
      let decoded = decodeDropSpaceRequest(encoded)
      check decoded.isOk
      check decoded.value.name == name

suite "GroupRecordData":

  test "GroupRecordData construction":
    let groupId = genULIDLocal()
    let gr = GroupRecordData(groupId: groupId, record: "test_record")
    check gr.record == "test_record"

  test "GroupRecordData empty record":
    let groupId = genULIDLocal()
    let gr = GroupRecordData(groupId: groupId, record: "")
    check gr.record == ""
