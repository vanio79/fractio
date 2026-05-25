# Unit tests for fractio/protocol/messages/admin.nim
# Tests ServerInfo, Metrics, Health encoding/decoding

import std/unittest
import fractio/protocol/messages/admin
import fractio/protocol/types
import fractio/protocol/codec

suite "ServerInfo Messages":

  test "encodeServerInfoRequest":
    let encoded = encodeServerInfoRequest()
    check encoded.len == 2 # Just message type
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtServerInfo)

  test "decodeServerInfoRequest valid":
    let encoded = encodeServerInfoRequest()
    let decoded = decodeServerInfoRequest(encoded)
    check decoded.isOk

  test "decodeServerInfoRequest truncated":
    let invalid = "" # Empty
    let decoded = decodeServerInfoRequest(invalid)
    check decoded.isErr

  test "encodeServerInfoResponse":
    let resp = ServerInfoResponse(
      nodeId: 1'u16,
      version: "1.0.0",
      uptimeSecs: 3600'u64,
      role: RoleLeader,
      groupCount: 10'u32,
      clientCount: 5'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtServerInfo)
    let nodeId = readUint16BE(encoded, pos)
    check nodeId.value == 1'u16

  test "encodeServerInfoResponse follower":
    let resp = ServerInfoResponse(
      nodeId: 2'u16,
      version: "1.0.0",
      uptimeSecs: 7200'u64,
      role: RoleFollower,
      groupCount: 10'u32,
      clientCount: 3'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.role == RoleFollower

  test "encodeServerInfoResponse candidate":
    let resp = ServerInfoResponse(
      nodeId: 3'u16,
      version: "1.0.0",
      uptimeSecs: 100'u64,
      role: RoleCandidate,
      groupCount: 0'u32,
      clientCount: 0'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.role == RoleCandidate

  test "encodeServerInfoResponse unknown role":
    let resp = ServerInfoResponse(
      nodeId: 0'u16,
      version: "test",
      uptimeSecs: 0'u64,
      role: RoleUnknown,
      groupCount: 0'u32,
      clientCount: 0'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.role == RoleUnknown

  test "decodeServerInfoResponse valid":
    let resp = ServerInfoResponse(
      nodeId: 42'u16,
      version: "2.0.1",
      uptimeSecs: 86400'u64,
      role: RoleLeader,
      groupCount: 100'u32,
      clientCount: 50'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.nodeId == 42'u16
    check decoded.value.version == "2.0.1"
    check decoded.value.uptimeSecs == 86400'u64
    check decoded.value.role == RoleLeader
    check decoded.value.groupCount == 100'u32
    check decoded.value.clientCount == 50'u32

  test "decodeServerInfoResponse truncated nodeId":
    let invalid = "\x07\x00" # Just message type
    let decoded = decodeServerInfoResponse(invalid)
    check decoded.isErr

  test "decodeServerInfoResponse truncated version":
    let invalid = "\x07\x00\x00\x01" # Message type + nodeId, no version
    let decoded = decodeServerInfoResponse(invalid)
    check decoded.isErr

  test "decodeServerInfoResponse truncated uptime":
    let invalid = "\x07\x00\x00\x01\x05\x31\x2e\x30\x2e\x30" # MT + nodeId + version, no uptime
    let decoded = decodeServerInfoResponse(invalid)
    check decoded.isErr

  test "ServerInfo roundtrip":
    for role in [RoleLeader, RoleFollower, RoleCandidate, RoleUnknown]:
      let resp = ServerInfoResponse(
        nodeId: 1'u16,
        version: "1.0",
        uptimeSecs: 1000'u64,
        role: role,
        groupCount: 5'u32,
        clientCount: 2'u32
      )
      let encoded = encodeServerInfoResponse(resp)
      let decoded = decodeServerInfoResponse(encoded)
      check decoded.isOk
      check decoded.value.role == role

suite "Metrics Messages":

  test "encodeMetricsRequest":
    let req = MetricsRequest(flags: 0x00'u8)
    let encoded = encodeMetricsRequest(req)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtMetrics)
    let flags = readUint8(encoded, pos)
    check flags.value == 0x00'u8

  test "encodeMetricsRequest with reset flag":
    let req = MetricsRequest(flags: MetricsFlagReset)
    let encoded = encodeMetricsRequest(req)
    var pos = 0
    discard readUint16BE(encoded, pos)
    let flags = readUint8(encoded, pos)
    check flags.value == MetricsFlagReset

  test "decodeMetricsRequest valid":
    let req = MetricsRequest(flags: 0x00'u8)
    let encoded = encodeMetricsRequest(req)
    let decoded = decodeMetricsRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == 0x00'u8

  test "decodeMetricsRequest truncated":
    let invalid = "\x07\x01" # Just message type
    let decoded = decodeMetricsRequest(invalid)
    check decoded.isErr

  test "encodeMetricsResponse":
    let resp = MetricsResponse(
      requestsTotal: 1000'u64,
      requestsOK: 900'u64,
      requestsErr: 100'u64,
      bytesIn: 5000'u64,
      bytesOut: 6000'u64,
      kvGets: 300'u64,
      kvPuts: 200'u64,
      kvDeletes: 50'u64,
      activeTxns: 10'u32,
      committedTxns: 500'u64,
      abortedTxns: 20'u64
    )
    let encoded = encodeMetricsResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtMetrics)

  test "encodeMetricsResponse zero values":
    let resp = MetricsResponse()
    let encoded = encodeMetricsResponse(resp)
    let decoded = decodeMetricsResponse(encoded)
    check decoded.isOk
    check decoded.value.requestsTotal == 0'u64

  test "encodeMetricsResponse large values":
    let resp = MetricsResponse(
      requestsTotal: 0xFFFFFFFFFFFFFFFF'u64,
      requestsOK: 0'u64,
      requestsErr: 0'u64,
      bytesIn: 0'u64,
      bytesOut: 0'u64,
      kvGets: 0'u64,
      kvPuts: 0'u64,
      kvDeletes: 0'u64,
      activeTxns: 0xFFFFFFFF'u32,
      committedTxns: 0'u64,
      abortedTxns: 0'u64
    )
    let encoded = encodeMetricsResponse(resp)
    let decoded = decodeMetricsResponse(encoded)
    check decoded.isOk
    check decoded.value.requestsTotal == 0xFFFFFFFFFFFFFFFF'u64
    check decoded.value.activeTxns == 0xFFFFFFFF'u32

  test "decodeMetricsResponse valid":
    let resp = MetricsResponse(
      requestsTotal: 5000'u64,
      requestsOK: 4500'u64,
      requestsErr: 500'u64,
      bytesIn: 100000'u64,
      bytesOut: 200000'u64,
      kvGets: 1000'u64,
      kvPuts: 500'u64,
      kvDeletes: 100'u64,
      activeTxns: 20'u32,
      committedTxns: 1000'u64,
      abortedTxns: 50'u64
    )
    let encoded = encodeMetricsResponse(resp)
    let decoded = decodeMetricsResponse(encoded)
    check decoded.isOk
    check decoded.value.requestsTotal == 5000'u64
    check decoded.value.requestsOK == 4500'u64
    check decoded.value.requestsErr == 500'u64
    check decoded.value.bytesIn == 100000'u64
    check decoded.value.bytesOut == 200000'u64
    check decoded.value.kvGets == 1000'u64
    check decoded.value.kvPuts == 500'u64
    check decoded.value.kvDeletes == 100'u64
    check decoded.value.activeTxns == 20'u32
    check decoded.value.committedTxns == 1000'u64
    check decoded.value.abortedTxns == 50'u64

  test "decodeMetricsResponse truncated":
    let invalid = "\x07\x01" # Just message type
    let decoded = decodeMetricsResponse(invalid)
    check decoded.isErr

  test "Metrics roundtrip":
    let resp = MetricsResponse(
      requestsTotal: 123'u64,
      requestsOK: 100'u64,
      requestsErr: 23'u64,
      bytesIn: 456'u64,
      bytesOut: 789'u64,
      kvGets: 50'u64,
      kvPuts: 30'u64,
      kvDeletes: 5'u64,
      activeTxns: 3'u32,
      committedTxns: 45'u64,
      abortedTxns: 2'u64
    )
    let encoded = encodeMetricsResponse(resp)
    let decoded = decodeMetricsResponse(encoded)
    check decoded.isOk
    check decoded.value.requestsTotal == resp.requestsTotal
    check decoded.value.requestsOK == resp.requestsOK
    check decoded.value.requestsErr == resp.requestsErr

suite "Health Messages":

  test "encodeHealthRequest":
    let encoded = encodeHealthRequest()
    check encoded.len == 2 # Just message type
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtHealth)

  test "decodeHealthRequest valid":
    let encoded = encodeHealthRequest()
    let decoded = decodeHealthRequest(encoded)
    check decoded.isOk

  test "decodeHealthRequest truncated":
    let invalid = "" # Empty
    let decoded = decodeHealthRequest(invalid)
    check decoded.isErr

  test "encodeHealthResponse OK":
    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 3'u16,
      healthyReplicas: 3'u16,
      clusterName: "test_cluster"
    )
    let encoded = encodeHealthResponse(resp)
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.value == uint16(mtHealth)
    let status = readUint8(encoded, pos)
    check status.value == HealthOK

  test "encodeHealthResponse Degraded":
    let resp = HealthResponse(
      status: HealthDegraded,
      leaderOK: true,
      replicaCount: 3'u16,
      healthyReplicas: 2'u16,
      clusterName: "degraded_cluster"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthDegraded

  test "encodeHealthResponse Critical":
    let resp = HealthResponse(
      status: HealthCritical,
      leaderOK: false,
      replicaCount: 3'u16,
      healthyReplicas: 0'u16,
      clusterName: "critical_cluster"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthCritical
    check decoded.value.leaderOK == false

  test "encodeHealthResponse leader not OK":
    let resp = HealthResponse(
      status: HealthDegraded,
      leaderOK: false,
      replicaCount: 5'u16,
      healthyReplicas: 4'u16,
      clusterName: "no_leader"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.leaderOK == false

  test "encodeHealthResponse empty cluster name":
    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 1'u16,
      healthyReplicas: 1'u16,
      clusterName: ""
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.clusterName == ""

  test "decodeHealthResponse valid":
    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 10'u16,
      healthyReplicas: 10'u16,
      clusterName: "full_health"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthOK
    check decoded.value.leaderOK == true
    check decoded.value.replicaCount == 10'u16
    check decoded.value.healthyReplicas == 10'u16
    check decoded.value.clusterName == "full_health"

  test "decodeHealthResponse truncated status":
    let invalid = "\x07\x02" # Just message type
    let decoded = decodeHealthResponse(invalid)
    check decoded.isErr

  test "decodeHealthResponse truncated cluster name":
    let invalid = "\x07\x02\x00\x01\x00\x03\x00\x03" # Missing cluster name length
    let decoded = decodeHealthResponse(invalid)
    check decoded.isErr

  test "Health roundtrip":
    for status in [HealthOK, HealthDegraded, HealthCritical]:
      for leaderOK in [true, false]:
        let resp = HealthResponse(
          status: status,
          leaderOK: leaderOK,
          replicaCount: 5'u16,
          healthyReplicas: if status == HealthOK: 5'u16 else: 3'u16,
          clusterName: "test"
        )
        let encoded = encodeHealthResponse(resp)
        let decoded = decodeHealthResponse(encoded)
        check decoded.isOk
        check decoded.value.status == status
        check decoded.value.leaderOK == leaderOK

suite "Admin Constants":

  test "RoleLeader value":
    check RoleLeader == 0x01'u8

  test "RoleFollower value":
    check RoleFollower == 0x02'u8

  test "RoleCandidate value":
    check RoleCandidate == 0x03'u8

  test "RoleUnknown value":
    check RoleUnknown == 0x00'u8

  test "HealthOK value":
    check HealthOK == 0x00'u8

  test "HealthDegraded value":
    check HealthDegraded == 0x01'u8

  test "HealthCritical value":
    check HealthCritical == 0x02'u8

  test "MetricsFlagReset value":
    check MetricsFlagReset == 0x01'u8
