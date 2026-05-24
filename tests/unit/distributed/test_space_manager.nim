# Unit tests for space_manager module and related message types
#
# Tests for:
# - System schema record encoding/decoding
# - Space message encoding/decoding
# - Constants and helper functions
#
# Note: Integration tests for full space creation/deletion workflows
# require real infrastructure and are in tests/integration/

import std/[unittest, options, tables, strutils, json]

import fractio/core/types # SpaceID, genSpaceID, genULID, ULID
from fractio/distributed/raft/group_types import GroupID, genGroupID, NodeID
import fractio/distributed/meta/system_tables
import fractio/distributed/meta/system_schemas as schemas
import fractio/protocol/messages/space as spaceMsgs

suite "Node Record Encoding/Decoding":
  test "encode/decode node record":
    let rec = NodeRecord(
      nodeId: 42,
      host: "192.168.1.100",
      raftPort: 9000,
      clientPort: 9001,
      status: nsAlive
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)

    check decoded.nodeId == 42
    check decoded.host == "192.168.1.100"
    check decoded.raftPort == 9000
    check decoded.clientPort == 9001
    check decoded.status == nsAlive

  test "node record with different status":
    let rec = NodeRecord(
      nodeId: 10,
      host: "localhost",
      raftPort: 8080,
      clientPort: 8081,
      status: nsDraining
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)

    check decoded.status == nsDraining

  test "node record roundtrip preserves all fields":
    let rec = NodeRecord(
      nodeId: 100,
      host: "test.host",
      raftPort: 1234,
      clientPort: 5678,
      status: nsDecommissioned
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)

    check decoded.nodeId == rec.nodeId
    check decoded.host == rec.host
    check decoded.raftPort == rec.raftPort
    check decoded.clientPort == rec.clientPort
    check decoded.status == rec.status

suite "Group Record Encoding/Decoding":
  test "encode/decode group record":
    let spaceId = genSpaceIDLocal()
    let rec = GroupRecord(
      groupId: genULIDLocal(),
      spaceId: spaceId,
      preferredLeader: 1,
      leader: 0,
      replicas: @[
        GroupReplicaBin(nodeId: 1, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 2, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 3, replicaType: schemas.rtLearner)
      ]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)

    check decoded.groupId == rec.groupId
    check decoded.spaceId == spaceId
    check decoded.preferredLeader == 1
    check decoded.replicas.len == 3
    check decoded.replicas[0].nodeId == 1
    check decoded.replicas[0].replicaType == schemas.rtVoter
    check decoded.replicas[2].replicaType == schemas.rtLearner

  test "group record with zero leader":
    let rec = GroupRecord(
      groupId: genULIDLocal(),
      spaceId: genSpaceIDLocal(),
      preferredLeader: 5,
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 5,
          replicaType: schemas.rtVoter)]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)

    check decoded.leader == 0
    check decoded.preferredLeader == 5

  test "group record with assigned leader":
    let rec = GroupRecord(
      groupId: genULIDLocal(),
      spaceId: genSpaceIDLocal(),
      preferredLeader: 1,
      leader: 2,
      replicas: @[
        GroupReplicaBin(nodeId: 1, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 2, replicaType: schemas.rtVoter)
      ]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)

    check decoded.leader == 2

  test "group record with many replicas":
    let rec = GroupRecord(
      groupId: genULIDLocal(),
      spaceId: genSpaceIDLocal(),
      preferredLeader: 1,
      leader: 1,
      replicas: @[
        GroupReplicaBin(nodeId: 1, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 2, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 3, replicaType: schemas.rtVoter),
        GroupReplicaBin(nodeId: 4, replicaType: schemas.rtLearner),
        GroupReplicaBin(nodeId: 5, replicaType: schemas.rtLearner)
      ]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)

    check decoded.replicas.len == 5

suite "Space Record Encoding/Decoding":
  test "encode/decode space record":
    let spaceId = genSpaceIDLocal()
    let groupIds = @[genGroupIDLocal(), genGroupIDLocal(), genGroupIDLocal()]
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "test_space",
      replicas: 3,
      groupCount: 3,
      groupIds: groupIds,
      oldGroupIds: @[],
      rebalancing: false,
      createdAtNs: nowNs()
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)

    check decoded.spaceId == spaceId
    check decoded.name == "test_space"
    check decoded.replicas == 3
    check decoded.groupCount == 3
    check decoded.groupIds.len == 3
    check decoded.rebalancing == false

  test "space record with rebalancing":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "rebalancing_space",
      replicas: 3,
      groupCount: 5,
      groupIds: @[genGroupIDLocal(), genGroupIDLocal()],
      oldGroupIds: @[genGroupIDLocal(), genGroupIDLocal(), genGroupIDLocal()],
      rebalancing: true,
      rebalanceWorker: 2,
      rebalanceHeartbeat: 12345,
      rebalanceCursor: "key123",
      createdAtNs: nowNs()
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)

    check decoded.rebalancing == true
    check decoded.rebalanceWorker == 2
    check decoded.rebalanceHeartbeat == 12345
    check decoded.rebalanceCursor == "key123"
    check decoded.oldGroupIds.len == 3

  test "space record with empty groupIds":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "empty_space",
      replicas: 1,
      groupCount: 0,
      groupIds: @[],
      oldGroupIds: @[],
      rebalancing: false,
      createdAtNs: nowNs()
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)

    check decoded.groupIds.len == 0
    check decoded.groupCount == 0

suite "CreateSpaceRequest/Response":
  test "encode/decode create space request":
    let req = spaceMsgs.CreateSpaceRequest(
      name: "my_space",
      replicas: 3
    )
    let encoded = spaceMsgs.encodeCreateSpaceRequest(req)
    let decoded = spaceMsgs.decodeCreateSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.name == "my_space"
    check decoded.val.replicas == 3

  test "encode/decode create space request - zero replicas":
    let req = spaceMsgs.CreateSpaceRequest(
      name: "all_nodes_space",
      replicas: 0
    )
    let encoded = spaceMsgs.encodeCreateSpaceRequest(req)
    let decoded = spaceMsgs.decodeCreateSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.replicas == 0

  test "encode/decode create space request - empty name":
    let req = spaceMsgs.CreateSpaceRequest(
      name: "",
      replicas: 3
    )
    let encoded = spaceMsgs.encodeCreateSpaceRequest(req)
    let decoded = spaceMsgs.decodeCreateSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.name == ""

  test "encode/decode create space request - large replicas":
    let req = spaceMsgs.CreateSpaceRequest(
      name: "large_replicas",
      replicas: 100
    )
    let encoded = spaceMsgs.encodeCreateSpaceRequest(req)
    let decoded = spaceMsgs.decodeCreateSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.replicas == 100

  test "encode/decode create space response - success":
    let spaceId = genSpaceIDLocal()
    let resp = spaceMsgs.CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 3,
      spaceRecord: "encoded_record",
      groupRecords: @[
        spaceMsgs.GroupRecordData(groupId: genULIDLocal(), record: "group1"),
        spaceMsgs.GroupRecordData(groupId: genULIDLocal(), record: "group2")
      ]
    )
    let encoded = spaceMsgs.encodeCreateSpaceResponse(resp)
    let decoded = spaceMsgs.decodeCreateSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.success == true
    check decoded.val.spaceId == spaceId
    check decoded.val.groupCount == 3
    check decoded.val.groupRecords.len == 2

  test "encode/decode create space response - failure":
    let resp = spaceMsgs.CreateSpaceResponse(
      success: false,
      error: "not the leader"
    )
    let encoded = spaceMsgs.encodeCreateSpaceResponse(resp)
    let decoded = spaceMsgs.decodeCreateSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.success == false
    check decoded.val.error == "not the leader"

  test "encode/decode create space response - empty groupRecords":
    let spaceId = genSpaceIDLocal()
    let resp = spaceMsgs.CreateSpaceResponse(
      success: true,
      spaceId: spaceId,
      groupCount: 0,
      spaceRecord: "encoded",
      groupRecords: @[]
    )
    let encoded = spaceMsgs.encodeCreateSpaceResponse(resp)
    let decoded = spaceMsgs.decodeCreateSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.groupRecords.len == 0

suite "DropSpaceRequest/Response":
  test "encode/decode drop space request":
    let req = spaceMsgs.DropSpaceRequest(name: "old_space")
    let encoded = spaceMsgs.encodeDropSpaceRequest(req)
    let decoded = spaceMsgs.decodeDropSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.name == "old_space"

  test "encode/decode drop space request - empty name":
    let req = spaceMsgs.DropSpaceRequest(name: "")
    let encoded = spaceMsgs.encodeDropSpaceRequest(req)
    let decoded = spaceMsgs.decodeDropSpaceRequest(encoded)

    check decoded.isOk
    check decoded.val.name == ""

  test "encode/decode drop space response - success":
    let spaceId = genSpaceIDLocal()
    let deletedIds = @[genGroupIDLocal(), genGroupIDLocal(), genGroupIDLocal()]
    let resp = spaceMsgs.DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: deletedIds
    )
    let encoded = spaceMsgs.encodeDropSpaceResponse(resp)
    let decoded = spaceMsgs.decodeDropSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.success == true
    check decoded.val.spaceId == spaceId
    check decoded.val.deletedGroupIds.len == 3

  test "encode/decode drop space response - failure":
    let resp = spaceMsgs.DropSpaceResponse(
      success: false,
      error: "space not found"
    )
    let encoded = spaceMsgs.encodeDropSpaceResponse(resp)
    let decoded = spaceMsgs.decodeDropSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.success == false
    check decoded.val.error == "space not found"

  test "encode/decode drop space response - empty deletedGroupIds":
    let spaceId = genSpaceIDLocal()
    let resp = spaceMsgs.DropSpaceResponse(
      success: true,
      spaceId: spaceId,
      deletedGroupIds: @[]
    )
    let encoded = spaceMsgs.encodeDropSpaceResponse(resp)
    let decoded = spaceMsgs.decodeDropSpaceResponse(encoded)

    check decoded.isOk
    check decoded.val.deletedGroupIds.len == 0

suite "System Table Constants":
  test "SYS_SPACES_TABLE_ID is valid ULID":
    let tableId = SYS_SPACES_TABLE_ID
    check ($tableId).len == 26 # ULID is 26 characters

  test "SYS_GROUPS_TABLE_ID is valid ULID":
    let tableId = SYS_GROUPS_TABLE_ID
    check ($tableId).len == 26

  test "SYS_NODES_TABLE_ID is valid ULID":
    let tableId = SYS_NODES_TABLE_ID
    check ($tableId).len == 26

suite "Space Key Encoding":
  test "encodeSpaceKey produces valid key":
    let spaceId = genSpaceIDLocal()
    let key = encodeSpaceKey(spaceId)
    check key.startsWith("/t/")
    check key.contains($spaceId)

  test "space key uses SYS_SPACES_TABLE_ID":
    let spaceId = genSpaceIDLocal()
    let key = encodeSpaceKey(spaceId)
    let (tableId, _) = decodeTableKey(key)
    check tableId == SYS_SPACES_TABLE_ID

suite "Helper Functions":
  test "nowNs returns positive value":
    let t = nowNs()
    check t > 0

  test "nowNs increases over time":
    let t1 = nowNs()
    # Small delay to ensure time difference
    let t2 = nowNs()
    check t2 >= t1

suite "Record Types":
  test "NodeStatus enum values":
    check int(nsUnknown) == 0
    check int(nsAlive) == 1
    check int(nsDraining) == 2
    check int(nsDecommissioned) == 3

  test "ReplicaType enum values":
    check int(schemas.rtVoter) == 0
    check int(schemas.rtLearner) == 1

  test "GroupReplicaBin construction":
    let rep = GroupReplicaBin(nodeId: 42, replicaType: schemas.rtVoter)
    check rep.nodeId == 42
    check rep.replicaType == schemas.rtVoter

  test "NodeRecord default values":
    let rec = NodeRecord()
    check rec.nodeId == 0
    check rec.host == ""
    check rec.status == nsUnknown # Default status is nsUnknown

suite "MVCC Helpers":
  test "stripMVCCHeader with proper MVCC header":
    # MVCC format: "MVCC" + 8 bytes timestamp + 16 bytes txn_id + 1 byte delete flag + payload
    let payload = "test_data"
    let header = "MVCC" & "\x00\x00\x00\x00\x00\x00\x00\x00" & # timestamp
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" & # txn_id
      "0" & payload # delete flag (0 = not deleted)
    let (stripped, isDeleted) = stripMVCCHeader(header)
    check stripped == payload
    check isDeleted == false

  test "stripMVCCHeader with deleted flag":
    let payload = "deleted_data"
    let header = "MVCC" & "\x00\x00\x00\x00\x00\x00\x00\x00" &
                 "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" &
                 "1" & payload # delete flag (1 = deleted)
    let (stripped, isDeleted) = stripMVCCHeader(header)
    check stripped == payload
    check isDeleted == true

  test "stripMVCCHeader without MVCC header":
    let payload = "raw_data"
    let (stripped, isDeleted) = stripMVCCHeader(payload)
    check stripped == payload
    check isDeleted == false

suite "JSON Serialization":
  test "NodeRecord toJson":
    let rec = NodeRecord(
      nodeId: 1,
      host: "localhost",
      raftPort: 9000,
      clientPort: 9001,
      status: nsAlive
    )
    let json = toJson(rec)
    check json{"nodeId"}.getInt == 1
    check json{"host"}.getStr == "localhost"
    check json{"status"}.getStr == "alive"

  test "GroupRecord toJson":
    let rec = GroupRecord(
      groupId: genULIDLocal(),
      spaceId: genSpaceIDLocal(),
      preferredLeader: 1,
      leader: 0,
      replicas: @[GroupReplicaBin(nodeId: 1,
          replicaType: schemas.rtVoter)]
    )
    let json = toJson(rec)
    check json{"preferredLeader"}.getInt == 1
    check json{"leader"}.getInt == 0

  test "SpaceRecord toJson":
    let rec = SpaceRecord(
      spaceId: genSpaceIDLocal(),
      name: "test",
      replicas: 3,
      groupCount: 1,
      groupIds: @[genGroupIDLocal()],
      oldGroupIds: @[],
      rebalancing: false,
      createdAtNs: nowNs()
    )
    let json = toJson(rec)
    check json{"name"}.getStr == "test"
    check json{"replicas"}.getInt == 3
    check json{"rebalancing"}.getBool == false
