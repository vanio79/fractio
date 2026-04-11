# Unit tests for fractio/protocol/messages/admin.nim
# Tests ServerInfo, Metrics, Health encoding/decoding

import std/unittest
import fractio/protocol/messages/admin
import fractio/protocol/types
import fractio/protocol/codec

suite "ServerInfoRequest/ServerInfoResponse":

  test "encodeServerInfoRequest":
    let encoded = encodeServerInfoRequest()
    check encoded.len == 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtServerInfo)

  test "decodeServerInfoRequest":
    let encoded = encodeServerInfoRequest()
    let decoded = decodeServerInfoRequest(encoded)
    check decoded.isOk

  test "decodeServerInfoRequest truncated":
    let truncated = "\x07"
    let decoded = decodeServerInfoRequest(truncated)
    check decoded.isErr

  test "encodeServerInfoResponse":
    let resp = ServerInfoResponse(
      nodeId: 42'u16,
      version: "1.0.0",
      uptimeSecs: 3600'u64,
      role: RoleLeader,
      shardCount: 10'u32,
      clientCount: 5'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    check encoded.len > 2

  test "encodeServerInfoResponse with different roles":
    let respLeader = ServerInfoResponse(
      nodeId: 1'u16,
      version: "2.0.0",
      uptimeSecs: 100'u64,
      role: RoleLeader,
      shardCount: 1'u32,
      clientCount: 1'u32
    )
    let encodedLeader = encodeServerInfoResponse(respLeader)
    check encodedLeader.len > 2

    let respFollower = ServerInfoResponse(
      nodeId: 2'u16,
      version: "2.0.0",
      uptimeSecs: 100'u64,
      role: RoleFollower,
      shardCount: 1'u32,
      clientCount: 0'u32
    )
    let encodedFollower = encodeServerInfoResponse(respFollower)
    check encodedFollower.len > 2

  test "decodeServerInfoResponse roundtrip":
    let resp = ServerInfoResponse(
      nodeId: 123'u16,
      version: "3.5.2",
      uptimeSecs: 7200'u64,
      role: RoleFollower,
      shardCount: 25'u32,
      clientCount: 100'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.nodeId == resp.nodeId
    check decoded.value.version == resp.version
    check decoded.value.uptimeSecs == resp.uptimeSecs
    check decoded.value.role == resp.role
    check decoded.value.shardCount == resp.shardCount
    check decoded.value.clientCount == resp.clientCount

  test "decodeServerInfoResponse empty version":
    let resp = ServerInfoResponse(
      nodeId: 1'u16,
      version: "",
      uptimeSecs: 0'u64,
      role: RoleUnknown,
      shardCount: 0'u32,
      clientCount: 0'u32
    )
    let encoded = encodeServerInfoResponse(resp)
    let decoded = decodeServerInfoResponse(encoded)
    check decoded.isOk
    check decoded.value.version == ""

  test "decodeServerInfoResponse truncated":
    let truncated = "\x07\x00"
    let decoded = decodeServerInfoResponse(truncated)
    check decoded.isErr

suite "MetricsRequest/MetricsResponse":

  test "encodeMetricsRequest basic":
    let req = MetricsRequest(flags: 0'u8)
    let encoded = encodeMetricsRequest(req)
    check encoded.len == 3
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtMetrics)

  test "encodeMetricsRequest with reset flag":
    let req = MetricsRequest(flags: MetricsFlagReset)
    let encoded = encodeMetricsRequest(req)
    check encoded.len == 3

  test "decodeMetricsRequest roundtrip":
    let req = MetricsRequest(flags: MetricsFlagReset)
    let encoded = encodeMetricsRequest(req)
    let decoded = decodeMetricsRequest(encoded)
    check decoded.isOk
    check decoded.value.flags == MetricsFlagReset

  test "decodeMetricsRequest truncated":
    let truncated = "\x07\x01"
    let decoded = decodeMetricsRequest(truncated)
    check decoded.isErr

  test "encodeMetricsResponse":
    let resp = MetricsResponse(
      requestsTotal: 1000'u64,
      requestsOK: 950'u64,
      requestsErr: 50'u64,
      bytesIn: 10000'u64,
      bytesOut: 5000'u64,
      kvGets: 800'u64,
      kvPuts: 100'u64,
      kvDeletes: 50'u64,
      activeTxns: 10'u32,
      committedTxns: 200'u64,
      abortedTxns: 5'u64
    )
    let encoded = encodeMetricsResponse(resp)
    check encoded.len == 86

  test "encodeMetricsResponse zero values":
    let resp = MetricsResponse()
    let encoded = encodeMetricsResponse(resp)
    check encoded.len == 86

  test "decodeMetricsResponse roundtrip":
    let resp = MetricsResponse(
      requestsTotal: 5000'u64,
      requestsOK: 4500'u64,
      requestsErr: 500'u64,
      bytesIn: 100000'u64,
      bytesOut: 50000'u64,
      kvGets: 3000'u64,
      kvPuts: 500'u64,
      kvDeletes: 200'u64,
      activeTxns: 25'u32,
      committedTxns: 1000'u64,
      abortedTxns: 10'u64
    )
    let encoded = encodeMetricsResponse(resp)
    let decoded = decodeMetricsResponse(encoded)
    check decoded.isOk
    check decoded.value.requestsTotal == resp.requestsTotal
    check decoded.value.requestsOK == resp.requestsOK
    check decoded.value.requestsErr == resp.requestsErr
    check decoded.value.bytesIn == resp.bytesIn
    check decoded.value.bytesOut == resp.bytesOut
    check decoded.value.kvGets == resp.kvGets
    check decoded.value.kvPuts == resp.kvPuts
    check decoded.value.kvDeletes == resp.kvDeletes
    check decoded.value.activeTxns == resp.activeTxns
    check decoded.value.committedTxns == resp.committedTxns
    check decoded.value.abortedTxns == resp.abortedTxns

  test "decodeMetricsResponse truncated":
    let truncated = "\x07\x01"
    let decoded = decodeMetricsResponse(truncated)
    check decoded.isErr

suite "HealthRequest/HealthResponse":

  test "encodeHealthRequest":
    let encoded = encodeHealthRequest()
    check encoded.len == 2
    var pos = 0
    let mt = readUint16BE(encoded, pos)
    check mt.isOk
    check mt.value == uint16(mtHealth)

  test "decodeHealthRequest":
    let encoded = encodeHealthRequest()
    let decoded = decodeHealthRequest(encoded)
    check decoded.isOk

  test "decodeHealthRequest truncated":
    let truncated = "\x07"
    let decoded = decodeHealthRequest(truncated)
    check decoded.isErr

  test "encodeHealthResponse OK":
    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 3'u16,
      healthyReplicas: 3'u16,
      clusterName: "prod-cluster"
    )
    let encoded = encodeHealthResponse(resp)
    check encoded.len > 2

  test "encodeHealthResponse Degraded":
    let resp = HealthResponse(
      status: HealthDegraded,
      leaderOK: true,
      replicaCount: 5'u16,
      healthyReplicas: 3'u16,
      clusterName: "test-cluster"
    )
    let encoded = encodeHealthResponse(resp)
    check encoded.len > 2

  test "encodeHealthResponse Critical":
    let resp = HealthResponse(
      status: HealthCritical,
      leaderOK: false,
      replicaCount: 3'u16,
      healthyReplicas: 1'u16,
      clusterName: "dev-cluster"
    )
    let encoded = encodeHealthResponse(resp)
    check encoded.len > 2

  test "encodeHealthResponse no leader":
    let resp = HealthResponse(
      status: HealthCritical,
      leaderOK: false,
      replicaCount: 0'u16,
      healthyReplicas: 0'u16,
      clusterName: ""
    )
    let encoded = encodeHealthResponse(resp)
    check encoded.len > 2

  test "decodeHealthResponse roundtrip healthy":
    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 5'u16,
      healthyReplicas: 5'u16,
      clusterName: "my-cluster"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthOK
    check decoded.value.leaderOK == true
    check decoded.value.replicaCount == resp.replicaCount
    check decoded.value.healthyReplicas == resp.healthyReplicas
    check decoded.value.clusterName == resp.clusterName

  test "decodeHealthResponse roundtrip degraded":
    let resp = HealthResponse(
      status: HealthDegraded,
      leaderOK: true,
      replicaCount: 7'u16,
      healthyReplicas: 4'u16,
      clusterName: "degraded"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthDegraded
    check decoded.value.leaderOK == true

  test "decodeHealthResponse roundtrip critical":
    let resp = HealthResponse(
      status: HealthCritical,
      leaderOK: false,
      replicaCount: 3'u16,
      healthyReplicas: 0'u16,
      clusterName: "critical"
    )
    let encoded = encodeHealthResponse(resp)
    let decoded = decodeHealthResponse(encoded)
    check decoded.isOk
    check decoded.value.status == HealthCritical
    check decoded.value.leaderOK == false

  test "decodeHealthResponse truncated":
    let truncated = "\x07\x02"
    let decoded = decodeHealthResponse(truncated)
    check decoded.isErr

suite "Admin Constants":

  test "Role values":
    check RoleUnknown == 0x00'u8
    check RoleLeader == 0x01'u8
    check RoleFollower == 0x02'u8
    check RoleCandidate == 0x03'u8

  test "Health status values":
    check HealthOK == 0x00'u8
    check HealthDegraded == 0x01'u8
    check HealthCritical == 0x02'u8

  test "Metrics flag values":
    check MetricsFlagReset == 0x01'u8

suite "Admin Message Roundtrip Integration":

  test "ServerInfo full roundtrip":
    let req = encodeServerInfoRequest()
    let reqDecoded = decodeServerInfoRequest(req)
    check reqDecoded.isOk

    let resp = ServerInfoResponse(
      nodeId: 999'u16,
      version: "test-version",
      uptimeSecs: 12345'u64,
      role: RoleLeader,
      shardCount: 50'u32,
      clientCount: 75'u32
    )
    let respEncoded = encodeServerInfoResponse(resp)
    let respDecoded = decodeServerInfoResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.nodeId == resp.nodeId

  test "Metrics full roundtrip":
    let req = MetricsRequest(flags: 0'u8)
    let reqEncoded = encodeMetricsRequest(req)
    let reqDecoded = decodeMetricsRequest(reqEncoded)
    check reqDecoded.isOk

    let resp = MetricsResponse(
      requestsTotal: 100'u64,
      requestsOK: 90'u64,
      requestsErr: 10'u64,
      bytesIn: 1000'u64,
      bytesOut: 500'u64,
      kvGets: 50'u64,
      kvPuts: 30'u64,
      kvDeletes: 10'u64,
      activeTxns: 5'u32,
      committedTxns: 100'u64,
      abortedTxns: 2'u64
    )
    let respEncoded = encodeMetricsResponse(resp)
    let respDecoded = decodeMetricsResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.requestsTotal == resp.requestsTotal

  test "Health full roundtrip":
    let req = encodeHealthRequest()
    let reqDecoded = decodeHealthRequest(req)
    check reqDecoded.isOk

    let resp = HealthResponse(
      status: HealthOK,
      leaderOK: true,
      replicaCount: 3'u16,
      healthyReplicas: 3'u16,
      clusterName: "test"
    )
    let respEncoded = encodeHealthResponse(resp)
    let respDecoded = decodeHealthResponse(respEncoded)
    check respDecoded.isOk
    check respDecoded.value.status == HealthOK
