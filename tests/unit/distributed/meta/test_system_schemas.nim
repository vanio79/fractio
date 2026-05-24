# Unit tests for system_schemas module
#
# Tests:
# - DatabaseRecord encode/decode roundtrip
# - SchemaRecord encode/decode roundtrip
# - ColumnDefBin encode/decode roundtrip
# - TableRecord encode/decode roundtrip
# - GroupReplicaBin encode/decode roundtrip
# - GroupRecord encode/decode roundtrip
# - NodeRecord encode/decode roundtrip
# - SpaceRecord encode/decode roundtrip
# - SettingRecord encode/decode roundtrip
# - MVCC-aware decoders (decode*FromMVCC)
# - stripMVCCHeader helper
# - nowNs timestamp helper
# - toJson conversion functions

import std/[unittest, strutils, json]
import fractio/distributed/meta/system_schemas
import fractio/distributed/raft/group_types
import fractio/core/types
import fractio/utils/binary

suite "DatabaseRecord":
  test "encode/decode roundtrip basic":
    let rec = DatabaseRecord(name: "mydb", createdAtNs: 1234567890000000000'i64)
    let encoded = encode(rec)
    let decoded = decodeDatabaseRecord(encoded)
    check decoded.name == "mydb"
    check decoded.createdAtNs == 1234567890000000000'i64

  test "encode/decode roundtrip empty name":
    let rec = DatabaseRecord(name: "", createdAtNs: 0'i64)
    let encoded = encode(rec)
    let decoded = decodeDatabaseRecord(encoded)
    check decoded.name == ""
    check decoded.createdAtNs == 0'i64

  test "encode/decode roundtrip max name length":
    let longName = "a".repeat(64)
    let rec = DatabaseRecord(name: longName, createdAtNs: 1000000000'i64)
    let encoded = encode(rec)
    let decoded = decodeDatabaseRecord(encoded)
    check decoded.name == longName

  test "encode/decode roundtrip special characters":
    let rec = DatabaseRecord(name: "test_db-with.dots", createdAtNs: -1'i64)
    let encoded = encode(rec)
    let decoded = decodeDatabaseRecord(encoded)
    check decoded.name == "test_db-with.dots"
    check decoded.createdAtNs == -1'i64

suite "SchemaRecord":
  test "encode/decode roundtrip basic":
    let rec = SchemaRecord(name: "public", database: "mydb",
        createdAtNs: 999999'i64)
    let encoded = encode(rec)
    let decoded = decodeSchemaRecord(encoded)
    check decoded.name == "public"
    check decoded.database == "mydb"
    check decoded.createdAtNs == 999999'i64

  test "encode/decode roundtrip empty fields":
    let rec = SchemaRecord(name: "", database: "", createdAtNs: 0'i64)
    let encoded = encode(rec)
    let decoded = decodeSchemaRecord(encoded)
    check decoded.name == ""
    check decoded.database == ""
    check decoded.createdAtNs == 0'i64

  test "encode/decode roundtrip with dots in name":
    let rec = SchemaRecord(name: "schema.v2", database: "db.prod",
        createdAtNs: 42'i64)
    let encoded = encode(rec)
    let decoded = decodeSchemaRecord(encoded)
    check decoded.name == "schema.v2"
    check decoded.database == "db.prod"

suite "ColumnDefBin":
  test "encodeColumnDef/decodeColumnDef roundtrip basic":
    let col = ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0'u16,
        flags: 0x01'u8)
    var w = initBinaryWriter()
    encodeColumnDef(col, w)
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeColumnDef(r)
    check decoded.name == "id"
    check decoded.dataType == cdtInt
    check decoded.maxLen == 0'u16
    check decoded.flags == 0x01'u8

  test "encodeColumnDef/decodeColumnDef roundtrip all data types":
    for dt in [cdtInt, cdtFloat, cdtString, cdtBool, cdtBytes, cdtDate,
        cdtDateTime, cdtULID]:
      let col = ColumnDefBin(name: "col_" & $dt, dataType: dt, maxLen: 255'u16,
          flags: 0x07'u8)
      var w = initBinaryWriter()
      encodeColumnDef(col, w)
      let encoded = w.finish()
      var r = initBinaryReader(encoded)
      let decoded = decodeColumnDef(r)
      check decoded.name == "col_" & $dt
      check decoded.dataType == dt
      check decoded.maxLen == 255'u16
      check decoded.flags == 0x07'u8

  test "encodeColumnDef/decodeColumnDef roundtrip empty name":
    let col = ColumnDefBin(name: "", dataType: cdtString, maxLen: 1000'u16, flags: 0'u8)
    var w = initBinaryWriter()
    encodeColumnDef(col, w)
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeColumnDef(r)
    check decoded.name == ""
    check decoded.maxLen == 1000'u16

  test "encodeColumnDef/decodeColumnDef roundtrip flag combinations":
    # Test all flag combinations: primaryKey (bit 0), notNull (bit 1), unique (bit 2)
    let flagValues = [0'u8, 0x01'u8, 0x02'u8, 0x03'u8, 0x04'u8, 0x05'u8,
        0x06'u8, 0x07'u8]
    for flags in flagValues:
      let col = ColumnDefBin(name: "col", dataType: cdtBool, maxLen: 0'u16, flags: flags)
      var w = initBinaryWriter()
      encodeColumnDef(col, w)
      let encoded = w.finish()
      var r = initBinaryReader(encoded)
      let decoded = decodeColumnDef(r)
      check decoded.flags == flags

suite "TableRecord":
  test "encode/decode roundtrip basic":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let col = ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0'u16,
        flags: 0x01'u8)
    let rec = TableRecord(
      tableId: tableId,
      name: "users",
      schema: "public",
      database: "mydb",
      spaceId: spaceId,
      primaryKey: @["id"],
      columns: @[col]
    )
    let encoded = encode(rec)
    let decoded = decodeTableRecord(encoded)
    check decoded.tableId == tableId
    check decoded.name == "users"
    check decoded.schema == "public"
    check decoded.database == "mydb"
    check decoded.spaceId == spaceId
    check decoded.primaryKey.len == 1
    check decoded.primaryKey[0] == "id"
    check decoded.columns.len == 1
    check decoded.columns[0].name == "id"
    check decoded.columns[0].dataType == cdtInt

  test "encode/decode roundtrip multiple columns":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let cols = @[
      ColumnDefBin(name: "id", dataType: cdtULID, maxLen: 0'u16,
          flags: 0x01'u8),
      ColumnDefBin(name: "email", dataType: cdtString, maxLen: 255'u16,
          flags: 0x06'u8),
      ColumnDefBin(name: "created", dataType: cdtDateTime, maxLen: 0'u16,
          flags: 0'u8),
      ColumnDefBin(name: "active", dataType: cdtBool, maxLen: 0'u16,
          flags: 0x02'u8)
    ]
    let rec = TableRecord(
      tableId: tableId,
      name: "users",
      schema: "public",
      database: "testdb",
      spaceId: spaceId,
      primaryKey: @["id", "email"],
      columns: cols
    )
    let encoded = encode(rec)
    let decoded = decodeTableRecord(encoded)
    check decoded.columns.len == 4
    check decoded.columns[0].dataType == cdtULID
    check decoded.columns[1].maxLen == 255'u16
    check decoded.columns[2].dataType == cdtDateTime
    check decoded.columns[3].dataType == cdtBool
    check decoded.primaryKey.len == 2
    check decoded.primaryKey[0] == "id"
    check decoded.primaryKey[1] == "email"

  test "encode/decode roundtrip empty columns":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let rec = TableRecord(
      tableId: tableId,
      name: "empty_table",
      schema: "test",
      database: "db",
      spaceId: spaceId,
      primaryKey: @[],
      columns: @[]
    )
    let encoded = encode(rec)
    let decoded = decodeTableRecord(encoded)
    check decoded.columns.len == 0
    check decoded.primaryKey.len == 0

  test "encode/decode roundtrip zero ULIDs":
    let tableId = TableId(ZeroULID())
    let spaceId = SpaceID(ZeroULID())
    let rec = TableRecord(
      tableId: tableId,
      name: "sys_table",
      schema: "sys",
      database: "sys",
      spaceId: spaceId,
      primaryKey: @[],
      columns: @[]
    )
    let encoded = encode(rec)
    let decoded = decodeTableRecord(encoded)
    check decoded.tableId == tableId
    check decoded.spaceId == spaceId

suite "GroupReplicaBin":
  test "encodeGroupReplica/decodeGroupReplica roundtrip voter":
    let rep = GroupReplicaBin(nodeId: 42'u32, replicaType: rtVoter)
    var w = initBinaryWriter()
    encodeGroupReplica(rep, w)
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeGroupReplica(r)
    check decoded.nodeId == 42'u32
    check decoded.replicaType == rtVoter

  test "encodeGroupReplica/decodeGroupReplica roundtrip learner":
    let rep = GroupReplicaBin(nodeId: 100'u32, replicaType: rtLearner)
    var w = initBinaryWriter()
    encodeGroupReplica(rep, w)
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeGroupReplica(r)
    check decoded.nodeId == 100'u32
    check decoded.replicaType == rtLearner

  test "decodeGroupReplica handles unknown type as voter":
    # Manually construct data with unknown replica type byte
    var w = initBinaryWriter()
    w.writeU32(1'u32)
    w.writeU8(99'u8) # Unknown type value > 1
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeGroupReplica(r)
    check decoded.nodeId == 1'u32
    check decoded.replicaType == rtVoter # Defaults to voter for unknown values

  test "decodeGroupReplica handles NuRaft LEARNER_FLAG (0x1)":
    # NuRaft LEARNER_FLAG = 0x1, which maps to our rtLearner (1)
    var w = initBinaryWriter()
    w.writeU32(5'u32)
    w.writeU8(0x01'u8) # NuRaft LEARNER_FLAG = our rtLearner
    let encoded = w.finish()
    var r = initBinaryReader(encoded)
    let decoded = decodeGroupReplica(r)
    check decoded.replicaType == rtLearner

suite "GroupRecord":
  test "encode/decode roundtrip basic":
    let groupId = genULIDLocal()
    let spaceId = genSpaceIDLocal()
    let replicas = @[
      GroupReplicaBin(nodeId: 1'u32, replicaType: rtVoter),
      GroupReplicaBin(nodeId: 2'u32, replicaType: rtVoter),
      GroupReplicaBin(nodeId: 3'u32, replicaType: rtLearner)
    ]
    let rec = GroupRecord(
      groupId: groupId,
      spaceId: spaceId,
      preferredLeader: 1'u32,
      leader: 2'u32,
      replicas: replicas
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)
    check decoded.groupId == groupId
    check decoded.spaceId == spaceId
    check decoded.preferredLeader == 1'u32
    check decoded.leader == 2'u32
    check decoded.replicas.len == 3
    check decoded.replicas[0].nodeId == 1'u32
    check decoded.replicas[0].replicaType == rtVoter
    check decoded.replicas[2].replicaType == rtLearner

  test "encode/decode roundtrip empty replicas":
    let groupId = genULIDLocal()
    let spaceId = genSpaceIDLocal()
    let rec = GroupRecord(
      groupId: groupId,
      spaceId: spaceId,
      preferredLeader: 0'u32,
      leader: 0'u32,
      replicas: @[]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)
    check decoded.replicas.len == 0

  test "encode/decode roundtrip zero ULIDs":
    let rec = GroupRecord(
      groupId: ZeroULID(),
      spaceId: SpaceID(ZeroULID()),
      preferredLeader: 0'u32,
      leader: 0'u32,
      replicas: @[]
    )
    let encoded = encode(rec)
    let decoded = decodeGroupRecord(encoded)
    check decoded.groupId == ZeroULID()
    check decoded.spaceId == SpaceID(ZeroULID())

suite "NodeRecord":
  test "encode/decode roundtrip basic":
    let rec = NodeRecord(
      nodeId: 1'u32,
      host: "localhost",
      raftPort: 7000'u16,
      clientPort: 8000'u16,
      status: nsAlive
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)
    check decoded.nodeId == 1'u32
    check decoded.host == "localhost"
    check decoded.raftPort == 7000'u16
    check decoded.clientPort == 8000'u16
    check decoded.status == nsAlive

  test "encode/decode roundtrip all statuses":
    for st in [nsUnknown, nsAlive, nsDraining, nsDecommissioned]:
      let rec = NodeRecord(
        nodeId: 5'u32,
        host: "node.example.com",
        raftPort: 9000'u16,
        clientPort: 9001'u16,
        status: st
      )
      let encoded = encode(rec)
      let decoded = decodeNodeRecord(encoded)
      check decoded.status == st

  test "encode/decode roundtrip empty host":
    let rec = NodeRecord(
      nodeId: 0'u32,
      host: "",
      raftPort: 0'u16,
      clientPort: 0'u16,
      status: nsUnknown
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)
    check decoded.host == ""
    check decoded.nodeId == 0'u32

  test "encode/decode roundtrip max host length":
    let longHost = "n".repeat(64)
    let rec = NodeRecord(
      nodeId: 100'u32,
      host: longHost,
      raftPort: 65535'u16,
      clientPort: 1'u16,
      status: nsAlive
    )
    let encoded = encode(rec)
    let decoded = decodeNodeRecord(encoded)
    check decoded.host == longHost

suite "SpaceRecord":
  test "encode/decode roundtrip basic":
    let spaceId = genSpaceIDLocal()
    let groupIds = @[genGroupIDLocal(), genGroupIDLocal()]
    let oldGroupIds = @[genGroupIDLocal()]
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "default_space",
      replicas: 3'i32,
      groupCount: 2'i32,
      groupIds: groupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
      rebalanceWorker: 5'i32,
      rebalanceHeartbeat: 1700000000'i64,
      rebalanceCursor: "key_123",
      createdAtNs: 1600000000'i64
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)
    check decoded.spaceId == spaceId
    check decoded.name == "default_space"
    check decoded.replicas == 3'i32
    check decoded.groupCount == 2'i32
    check decoded.groupIds.len == 2
    check decoded.oldGroupIds.len == 1
    check decoded.rebalancing == true
    check decoded.rebalanceWorker == 5'i32
    check decoded.rebalanceHeartbeat == 1700000000'i64
    check decoded.rebalanceCursor == "key_123"
    check decoded.createdAtNs == 1600000000'i64

  test "encode/decode roundtrip not rebalancing":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "stable_space",
      replicas: 0'i32, # 0 = ALL nodes
      groupCount: 10'i32,
      groupIds: @[genGroupIDLocal()],
      oldGroupIds: @[],
      rebalancing: false,
      rebalanceWorker: -1'i32,
      rebalanceHeartbeat: 0'i64,
      rebalanceCursor: "",
      createdAtNs: 0'i64
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)
    check decoded.rebalancing == false
    check decoded.oldGroupIds.len == 0
    check decoded.rebalanceCursor == ""

  test "encode/decode roundtrip empty groupIds":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "empty_space",
      replicas: 1'i32,
      groupCount: 0'i32,
      groupIds: @[],
      oldGroupIds: @[],
      rebalancing: false,
      rebalanceWorker: 0'i32,
      rebalanceHeartbeat: 0'i64,
      rebalanceCursor: "",
      createdAtNs: 100'i64
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)
    check decoded.groupIds.len == 0
    check decoded.oldGroupIds.len == 0

  test "encode/decode roundtrip negative values":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "test",
      replicas: -1'i32,
      groupCount: -5'i32,
      groupIds: @[],
      oldGroupIds: @[],
      rebalancing: false,
      rebalanceWorker: -100'i32,
      rebalanceHeartbeat: -999999'i64,
      rebalanceCursor: "",
      createdAtNs: -1'i64
    )
    let encoded = encode(rec)
    let decoded = decodeSpaceRecord(encoded)
    check decoded.replicas == -1'i32
    check decoded.groupCount == -5'i32
    check decoded.rebalanceWorker == -100'i32

suite "SettingRecord":
  test "encode/decode roundtrip basic":
    let rec = SettingRecord(value: "replication_factor=3")
    let encoded = encode(rec)
    let decoded = decodeSettingRecord(encoded)
    check decoded.value == "replication_factor=3"

  test "encode/decode roundtrip empty value":
    let rec = SettingRecord(value: "")
    let encoded = encode(rec)
    let decoded = decodeSettingRecord(encoded)
    check decoded.value == ""

  test "encode/decode roundtrip JSON value":
    let rec = SettingRecord(value: "{\"key\":\"value\",\"num\":42}")
    let encoded = encode(rec)
    let decoded = decodeSettingRecord(encoded)
    check decoded.value == "{\"key\":\"value\",\"num\":42}"

suite "stripMVCCHeader":
  test "strips valid MVCC header":
    # MVCC format: <MAGIC (4 bytes)><8 bytes timestamp><16 bytes txn_id ULID><1 byte delete flag><payload>
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(12345'i64) # timestamp
    w.writeBytes(ulidToBytes(genULIDLocal())) # txn_id
    w.writeU8(ord('0')) # delete flag = '0' (not deleted)
    w.writeBytes("payload_data") # raw bytes, no length prefix
    let data = w.finish()

    let (payload, isDeleted) = stripMVCCHeader(data)
    check isDeleted == false
    check payload == "payload_data"

  test "returns deleted flag true when byte is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(999'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1')) # delete flag = '1' (deleted)
    w.writeBytes("deleted_payload") # raw bytes, no length prefix
    let data = w.finish()

    let (payload, isDeleted) = stripMVCCHeader(data)
    check isDeleted == true
    check payload == "deleted_payload"

  test "returns original data when no MVCC header":
    let data = "raw_data_without_header"
    let (payload, isDeleted) = stripMVCCHeader(data)
    check isDeleted == false
    check payload == data

  test "returns original data when too short":
    let data = "MV" # Too short for header
    let (payload, isDeleted) = stripMVCCHeader(data)
    check isDeleted == false
    check payload == data

  test "returns original data when magic mismatch":
    let data = "XXXX" & "\x00\x00\x00\x00\x00\x00\x00\x00" &
        "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" & "0payload"
    let (payload, isDeleted) = stripMVCCHeader(data)
    check isDeleted == false
    check payload == data

suite "decodeDatabaseRecordFromMVCC":
  test "decodes valid MVCC data":
    let rec = DatabaseRecord(name: "test_db", createdAtNs: 1000'i64)
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(5000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeDatabaseRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "test_db"
    check decoded.createdAtNs == 1000'i64

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1')) # deleted
    w.writeString("any_payload")
    let data = w.finish()

    let (decoded, isDeleted) = decodeDatabaseRecordFromMVCC(data)
    check isDeleted == true
    check decoded.name == "" # Default empty record

  test "decodes raw data without MVCC header":
    let rec = DatabaseRecord(name: "raw_db", createdAtNs: 777'i64)
    let data = encode(rec)

    let (decoded, isDeleted) = decodeDatabaseRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "raw_db"

suite "decodeSchemaRecordFromMVCC":
  test "decodes valid MVCC data":
    let rec = SchemaRecord(name: "myschema", database: "mydb",
        createdAtNs: 2000'i64)
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(3000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeSchemaRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "myschema"
    check decoded.database == "mydb"

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1'))
    w.writeString("any")
    let data = w.finish()

    let (decoded, isDeleted) = decodeSchemaRecordFromMVCC(data)
    check isDeleted == true
    check decoded.name == ""

suite "decodeTableRecordFromMVCC":
  test "decodes valid MVCC data":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let rec = TableRecord(
      tableId: tableId,
      name: "test_table",
      schema: "public",
      database: "testdb",
      spaceId: spaceId,
      primaryKey: @["id"],
      columns: @[ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0'u16,
          flags: 0x01'u8)]
    )
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(4000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeTableRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "test_table"
    check decoded.tableId == tableId

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1'))
    w.writeString("payload")
    let data = w.finish()

    let (decoded, isDeleted) = decodeTableRecordFromMVCC(data)
    check isDeleted == true

  test "returns empty record when payload too short (< 4 bytes after header)":
    # Create MVCC data with payload that's exactly 3 bytes (less than the 4-byte threshold)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes("abc") # Exactly 3 bytes (< 4 byte threshold)
    let data = w.finish()

    let (decoded, isDeleted) = decodeTableRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "" # Default empty record

  test "raises exception when payload passes threshold but is invalid":
    # Create MVCC data with payload that passes length check but is invalid TableRecord
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeU32(100'u32) # 4 bytes, passes threshold
    let data = w.finish()

    expect ValueError:
      discard decodeTableRecordFromMVCC(data)

suite "decodeGroupRecordFromMVCC":
  test "decodes valid MVCC data":
    let groupId = genULIDLocal()
    let spaceId = genSpaceIDLocal()
    let rec = GroupRecord(
      groupId: groupId,
      spaceId: spaceId,
      preferredLeader: 1'u32,
      leader: 2'u32,
      replicas: @[GroupReplicaBin(nodeId: 1'u32, replicaType: rtVoter)]
    )
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(5000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeGroupRecordFromMVCC(data)
    check isDeleted == false
    check decoded.groupId == groupId
    check decoded.replicas.len == 1

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1'))
    w.writeString("any")
    let data = w.finish()

    let (decoded, isDeleted) = decodeGroupRecordFromMVCC(data)
    check isDeleted == true

  test "returns empty record when payload too short":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeString("abc") # Too short (< 8 bytes)
    let data = w.finish()

    let (decoded, isDeleted) = decodeGroupRecordFromMVCC(data)
    check isDeleted == false
    check decoded.replicas.len == 0

suite "decodeNodeRecordFromMVCC":
  test "decodes valid MVCC data":
    let rec = NodeRecord(
      nodeId: 10'u32,
      host: "node10.example.com",
      raftPort: 7001'u16,
      clientPort: 8001'u16,
      status: nsAlive
    )
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(6000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeNodeRecordFromMVCC(data)
    check isDeleted == false
    check decoded.nodeId == 10'u32
    check decoded.host == "node10.example.com"

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1'))
    w.writeString("any")
    let data = w.finish()

    let (decoded, isDeleted) = decodeNodeRecordFromMVCC(data)
    check isDeleted == true

  test "returns empty record when payload too short":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeString("short") # Too short (< 10 bytes)
    let data = w.finish()

    let (decoded, isDeleted) = decodeNodeRecordFromMVCC(data)
    check isDeleted == false
    check decoded.nodeId == 0'u32

suite "decodeSpaceRecordFromMVCC":
  test "decodes valid MVCC data":
    let spaceId = genSpaceIDLocal()
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "test_space",
      replicas: 3'i32,
      groupCount: 5'i32,
      groupIds: @[genGroupIDLocal()],
      oldGroupIds: @[],
      rebalancing: false,
      rebalanceWorker: 0'i32,
      rebalanceHeartbeat: 0'i64,
      rebalanceCursor: "",
      createdAtNs: 7000'i64
    )
    let payload = encode(rec)
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(8000'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('0'))
    w.writeBytes(payload)
    let data = w.finish()

    let (decoded, isDeleted) = decodeSpaceRecordFromMVCC(data)
    check isDeleted == false
    check decoded.name == "test_space"
    check decoded.spaceId == spaceId

  test "returns deleted record when flag is '1'":
    var w = initBinaryWriter()
    w.writeBytes("MVCC")
    w.writeI64(1'i64)
    w.writeBytes(ulidToBytes(genULIDLocal()))
    w.writeU8(ord('1'))
    w.writeString("any")
    let data = w.finish()

    let (decoded, isDeleted) = decodeSpaceRecordFromMVCC(data)
    check isDeleted == true

suite "nowNs":
  test "returns positive timestamp":
    let ts = nowNs()
    check ts > 0'i64
    # Should be roughly current Unix epoch in nanoseconds
    # (within reasonable bounds: year 2020-2030 range)
    check ts > 1577836800000000000'i64 # Jan 1, 2020
    check ts < 1893456000000000000'i64 # Jan 1, 2030

suite "toJson DatabaseRecord":
  test "converts to JSON":
    let rec = DatabaseRecord(name: "mydb", createdAtNs: 1609459200000000000'i64) # Jan 1, 2021
    let json = toJson(rec)
    check json["name"].getStr() == "mydb"
    check json.hasKey("createdAt")

suite "toJson SchemaRecord":
  test "converts to JSON":
    let rec = SchemaRecord(name: "public", database: "mydb",
        createdAtNs: 1609459200000000000'i64)
    let json = toJson(rec)
    check json["name"].getStr() == "public"
    check json["database"].getStr() == "mydb"
    check json.hasKey("createdAt")

suite "toJson TableRecord":
  test "converts to JSON with columns":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let rec = TableRecord(
      tableId: tableId,
      name: "users",
      schema: "public",
      database: "mydb",
      spaceId: spaceId,
      primaryKey: @["id"],
      columns: @[
        ColumnDefBin(name: "id", dataType: cdtInt, maxLen: 0'u16,
            flags: 0x01'u8),
        ColumnDefBin(name: "email", dataType: cdtString, maxLen: 255'u16,
            flags: 0x06'u8)
      ]
    )
    let json = toJson(rec)
    check json["name"].getStr() == "users"
    check json["schema"].getStr() == "public"
    check json["database"].getStr() == "mydb"
    check json["primaryKey"].len == 1
    check json["columns"].len == 2
    check json["columns"][0]["name"].getStr() == "id"
    check json["columns"][0]["type"].getStr() == "INT"
    check json["columns"][0]["primaryKey"].getBool() == true
    check json["columns"][1]["type"].getStr() == "TEXT"

  test "converts all data types correctly":
    let tableId = genTableIdLocal()
    let spaceId = genSpaceIDLocal()
    let rec = TableRecord(
      tableId: tableId,
      name: "test",
      schema: "public",
      database: "db",
      spaceId: spaceId,
      primaryKey: @[],
      columns: @[
        ColumnDefBin(name: "c_int", dataType: cdtInt, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_float", dataType: cdtFloat, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_string", dataType: cdtString, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_bool", dataType: cdtBool, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_bytes", dataType: cdtBytes, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_date", dataType: cdtDate, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_datetime", dataType: cdtDateTime, maxLen: 0'u16,
            flags: 0'u8),
        ColumnDefBin(name: "c_ulid", dataType: cdtULID, maxLen: 0'u16, flags: 0'u8)
      ]
    )
    let json = toJson(rec)
    check json["columns"][0]["type"].getStr() == "INT"
    check json["columns"][1]["type"].getStr() == "FLOAT"
    check json["columns"][2]["type"].getStr() == "TEXT"
    check json["columns"][3]["type"].getStr() == "BOOL"
    check json["columns"][4]["type"].getStr() == "BLOB"
    check json["columns"][5]["type"].getStr() == "DATE"
    check json["columns"][6]["type"].getStr() == "DATETIME"
    check json["columns"][7]["type"].getStr() == "ULID"

suite "toJson GroupRecord":
  test "converts to JSON with replicas":
    let groupId = genULIDLocal()
    let spaceId = genSpaceIDLocal()
    let rec = GroupRecord(
      groupId: groupId,
      spaceId: spaceId,
      preferredLeader: 1'u32,
      leader: 2'u32,
      replicas: @[
        GroupReplicaBin(nodeId: 1'u32, replicaType: rtVoter),
        GroupReplicaBin(nodeId: 2'u32, replicaType: rtLearner)
      ]
    )
    let json = toJson(rec)
    check json.hasKey("groupId")
    check json.hasKey("spaceId")
    check json["preferredLeader"].getInt() == 1
    check json["leader"].getInt() == 2
    check json["replicas"].len == 2
    check json["replicas"][0]["nodeId"].getInt() == 1
    check json["replicas"][0]["type"].getStr() == "voter"
    check json["replicas"][1]["type"].getStr() == "learner"

suite "toJson NodeRecord":
  test "converts to JSON with all statuses":
    for st in [nsUnknown, nsAlive, nsDraining, nsDecommissioned]:
      let rec = NodeRecord(
        nodeId: 1'u32,
        host: "localhost",
        raftPort: 7000'u16,
        clientPort: 8000'u16,
        status: st
      )
      let json = toJson(rec)
      check json["nodeId"].getInt() == 1
      check json["host"].getStr() == "localhost"
      check json["raftPort"].getInt() == 7000
      check json["clientPort"].getInt() == 8000

      let expectedStatus = case st:
        of nsUnknown: "unknown"
        of nsAlive: "alive"
        of nsDraining: "draining"
        of nsDecommissioned: "decommissioned"
      check json["status"].getStr() == expectedStatus

suite "toJson SettingRecord":
  test "converts to JSON":
    let rec = SettingRecord(value: "my_setting_value")
    let json = toJson(rec)
    check json["value"].getStr() == "my_setting_value"

suite "toJson SpaceRecord":
  test "converts to JSON with all fields":
    let spaceId = genSpaceIDLocal()
    let groupIds = @[genGroupIDLocal(), genGroupIDLocal()]
    let oldGroupIds = @[genGroupIDLocal()]
    let rec = SpaceRecord(
      spaceId: spaceId,
      name: "my_space",
      replicas: 3'i32,
      groupCount: 2'i32,
      groupIds: groupIds,
      oldGroupIds: oldGroupIds,
      rebalancing: true,
      rebalanceWorker: 5'i32,
      rebalanceHeartbeat: 1700000000'i64,
      rebalanceCursor: "cursor_key"
    )
    let json = toJson(rec)
    check json.hasKey("spaceId")
    check json["name"].getStr() == "my_space"
    check json["replicas"].getInt() == 3
    check json["groupCount"].getInt() == 2
    check json["groupIds"].len == 2
    check json["oldGroupIds"].len == 1
    check json["rebalancing"].getBool() == true
    check json["rebalanceWorker"].getInt() == 5
    check json["rebalanceHeartbeat"].getInt() == 1700000000
    check json["rebalanceCursor"].getStr() == "cursor_key"
