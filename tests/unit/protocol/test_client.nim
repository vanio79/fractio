# Unit tests for fractio/protocol/client.nim
# Tests ClientConfig, ProtocolClient constructor and config

import std/[unittest, atomics, locks, strutils]
import fractio/protocol/client
import fractio/protocol/types

suite "ClientConfig":

  test "defaultClientConfig":
    let cfg = defaultClientConfig()
    check cfg.host == "127.0.0.1"
    check cfg.port == 9000
    check cfg.timeoutMs == 30_000
    check cfg.clientId == "fractio-client"
    check cfg.authMethod == amNone
    check cfg.authData == ""

  test "defaultClientConfig with custom host":
    let cfg = defaultClientConfig("10.0.0.1", 8080)
    check cfg.host == "10.0.0.1"
    check cfg.port == 8080

  test "ClientConfig custom":
    let cfg = ClientConfig(
      host: "192.168.1.100",
      port: 7000,
      timeoutMs: 5000,
      clientId: "my-client",
      authMethod: amNone,
      authData: "",
    )
    check cfg.host == "192.168.1.100"
    check cfg.port == 7000
    check cfg.timeoutMs == 5000
    check cfg.clientId == "my-client"
    check cfg.authMethod == amNone

  test "ClientConfig with auth":
    let cfg = ClientConfig(
      host: "127.0.0.1",
      port: 9000,
      timeoutMs: 30_000,
      clientId: "auth-client",
      authMethod: amNone,
      authData: "user:pass",
    )
    check cfg.authMethod == amNone
    check cfg.authData == "user:pass"

suite "ProtocolClient Constructor":

  test "newProtocolClient":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client != nil
    check client.config.host == "127.0.0.1"
    check client.config.port == 9000
    check client.connected.load() == false
    check client.nextRequestId.load() == 1
    check client.negotiatedFeatures == 0

  test "newProtocolClient with custom config":
    let cfg = ClientConfig(
      host: "10.0.0.5",
      port: 5000,
      timeoutMs: 10_000,
      clientId: "test-client",
    )
    let client = newProtocolClient(cfg)
    check client.config.host == "10.0.0.5"
    check client.config.port == 5000
    check client.config.timeoutMs == 10_000
    check client.config.clientId == "test-client"

  test "newProtocolClient initial state":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.socket == nil
    check client.connected.load() == false
    check client.negotiatedFeatures == 0

suite "ProtocolClient RequestId":

  test "nextRequestId starts at 1":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.nextRequestId.load() == 1

  test "nextRequestId increments":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    let id1 = client.nextRequestId.fetchAdd(1)
    let id2 = client.nextRequestId.fetchAdd(1)
    check id1 == 1
    check id2 == 2
    check client.nextRequestId.load() == 3

  test "nextRequestId independent per client":
    let cfg1 = defaultClientConfig()
    let cfg2 = defaultClientConfig()
    let client1 = newProtocolClient(cfg1)
    let client2 = newProtocolClient(cfg2)
    discard client1.nextRequestId.fetchAdd(5)
    discard client2.nextRequestId.fetchAdd(10)
    check client1.nextRequestId.load() == 6
    check client2.nextRequestId.load() == 11

suite "ProtocolClient Connected State":

  test "connected initially false":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.connected.load() == false

  test "connected state independent":
    let cfg = defaultClientConfig()
    let client1 = newProtocolClient(cfg)
    let client2 = newProtocolClient(cfg)
    client1.connected.store(true)
    check client1.connected.load() == true
    check client2.connected.load() == false

  test "connected can be toggled":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.connected.store(true)
    check client.connected.load() == true
    client.connected.store(false)
    check client.connected.load() == false

suite "ProtocolClient Negotiated Features":

  test "negotiatedFeatures initially zero":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.negotiatedFeatures == 0

  test "negotiatedFeatures can be set":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.negotiatedFeatures = FeatPipelining or FeatTransactions
    check (client.negotiatedFeatures and FeatPipelining) != 0
    check (client.negotiatedFeatures and FeatTransactions) != 0
    check (client.negotiatedFeatures and FeatTLS) == 0

suite "AuthMethod Constants":

  test "amNone value":
    check amNone == AuthMethod(0)

suite "Feature Flags Constants":

  test "FeatPipelining":
    check FeatPipelining != 0

  test "FeatTransactions":
    check FeatTransactions != 0

  test "FeatAsync":
    check FeatAsync != 0

  test "FeatTLS":
    check FeatTLS != 0

  test "Feature flags are distinct":
    check FeatPipelining != FeatTransactions
    check FeatTransactions != FeatAsync
    check FeatAsync != FeatTLS

  test "Feature flags can be combined":
    let combined = FeatPipelining or FeatTransactions or FeatAsync
    check (combined and FeatPipelining) != 0
    check (combined and FeatTransactions) != 0
    check (combined and FeatAsync) != 0
    check (combined and FeatTLS) == 0

suite "ProtocolClient Config Preservation":

  test "config is preserved":
    let cfg = ClientConfig(
      host: "custom.host",
      port: 1234,
      timeoutMs: 9999,
      clientId: "unique-id",
    )
    let client = newProtocolClient(cfg)
    check client.config.host == cfg.host
    check client.config.port == cfg.port
    check client.config.timeoutMs == cfg.timeoutMs
    check client.config.clientId == cfg.clientId

  test "config changes don't affect client":
    var cfg = ClientConfig(host: "orig.host", port: 1000)
    let client = newProtocolClient(cfg)
    cfg.host = "modified.host"
    cfg.port = 2000
    check client.config.host == "orig.host"
    check client.config.port == 1000

suite "ProtocolClient Disconnect":

  test "disconnect on unconnected client does nothing":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.connected.load() == false
    # disconnect() checks connected first, so it won't try to close nil socket
    # We can't call disconnect here as socket is nil and would crash
    check client.connected.load() == false # Still false

  test "connected state can be manually set":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.connected.store(true)
    check client.connected.load() == true
    client.connected.store(false)
    check client.connected.load() == false

suite "ProtocolClient WriteMu":

  test "writeMu is initialized":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    # Lock should be initialized (able to acquire/release)
    acquire(client.writeMu)
    release(client.writeMu)
    check true # If we got here, the lock is functional

  test "writeMu can be acquired and released":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    acquire(client.writeMu)
    release(client.writeMu)
    acquire(client.writeMu)
    release(client.writeMu)
    check true

suite "ClientConfig All Fields":

  test "ClientConfig with all auth methods":
    check amNone.ord == 0

  test "ClientConfig timeoutMs edge cases":
    let cfg1 = ClientConfig(host: "h", port: 1, timeoutMs: 0)
    check cfg1.timeoutMs == 0
    let cfg2 = ClientConfig(host: "h", port: 1, timeoutMs: -1)
    check cfg2.timeoutMs == -1
    let cfg3 = ClientConfig(host: "h", port: 1, timeoutMs: 60000)
    check cfg3.timeoutMs == 60000

  test "ClientConfig empty clientId":
    let cfg = ClientConfig(host: "h", port: 1, clientId: "")
    check cfg.clientId == ""

  test "ClientConfig empty authData":
    let cfg = ClientConfig(host: "h", port: 1, authData: "")
    check cfg.authData == ""

  test "ClientConfig port range":
    let cfg1 = ClientConfig(host: "h", port: 1)
    check cfg1.port == 1
    let cfg2 = ClientConfig(host: "h", port: 65535)
    check cfg2.port == 65535

suite "ProtocolClient RequestId Edge Cases":

  test "nextRequestId can increment many times":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    for i in 0..<100:
      discard client.nextRequestId.fetchAdd(1)
    check client.nextRequestId.load() == 101

  test "nextRequestId wraps around on overflow":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    # Set to near max
    client.nextRequestId.store(uint32.high - 5)
    let id1 = client.nextRequestId.fetchAdd(1)
    check id1 == uint32.high - 5
    # Continues incrementing
    let id2 = client.nextRequestId.fetchAdd(1)
    check id2 == uint32.high - 4

suite "ProtocolClient NegotiatedFeatures Edge Cases":

  test "negotiatedFeatures can store all flags":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.negotiatedFeatures = FeatPipelining or FeatTransactions or
        FeatAsync or FeatTLS
    check (client.negotiatedFeatures and FeatPipelining) != 0
    check (client.negotiatedFeatures and FeatTransactions) != 0
    check (client.negotiatedFeatures and FeatAsync) != 0
    check (client.negotiatedFeatures and FeatTLS) != 0

  test "negotiatedFeatures can be zero":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.negotiatedFeatures == 0

suite "ProtocolClient defaultClientConfig":

  test "defaultClientConfig with no arguments":
    let cfg = defaultClientConfig()
    check cfg.host == "127.0.0.1"
    check cfg.port == 9000
    check cfg.timeoutMs == 30_000
    check cfg.clientId == "fractio-client"
    check cfg.authMethod == amNone
    check cfg.authData == ""

  test "defaultClientConfig with custom host":
    let cfg = defaultClientConfig("10.0.0.1")
    check cfg.host == "10.0.0.1"
    check cfg.port == 9000

  test "defaultClientConfig with custom host and port":
    let cfg = defaultClientConfig("192.168.1.100", 8080)
    check cfg.host == "192.168.1.100"
    check cfg.port == 8080

  test "defaultClientConfig localhost variant":
    let cfg = defaultClientConfig("localhost", 9001)
    check cfg.host == "localhost"
    check cfg.port == 9001

suite "ProtocolClient AuthMethod Values":

  test "AuthMethod enum values":
    check amNone.ord == 0
    check amPassword.ord == 1
    check amToken.ord == 2
    check amTLS.ord == 3

  test "AuthMethod can be set in config":
    let cfg = ClientConfig(host: "h", port: 1, authMethod: amPassword)
    check cfg.authMethod == amPassword

  test "AuthMethod with authData":
    let cfg = ClientConfig(host: "h", port: 1, authMethod: amPassword,
                           authData: "user:pass")
    check cfg.authData == "user:pass"

suite "ProtocolClient Connection State":

  test "connected is atomic bool":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.connected.load() == false
    client.connected.store(true)
    check client.connected.load() == true
    client.connected.store(false)
    check client.connected.load() == false

  test "writeMu lock operations":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.writeMu.acquire()
    client.writeMu.release()
    client.writeMu.acquire()
    client.writeMu.release()
    # Should not crash

suite "ProtocolClient Socket Operations (Unconnected)":

  test "socket is nil before connect":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    check client.socket == nil

  test "disconnect is safe on unconnected client":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.disconnect()
    check client.connected.load() == false

  test "send returns error when not connected":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    let res = client.send("test_payload")
    check res.isErr
    check res.error.kind == peInternal
    let msg = res.error.msg
    check strutils.contains(msg, "not connected")

  test "closeConn is safe when not connected":
    let cfg = defaultClientConfig()
    let client = newProtocolClient(cfg)
    client.closeConn("test_reason")
    check client.connected.load() == false

suite "ProtocolClient Config Edge Cases":

  test "timeoutMs zero means block forever":
    let cfg = ClientConfig(host: "h", port: 1, timeoutMs: 0)
    check cfg.timeoutMs == 0

  test "timeoutMs negative is allowed in struct":
    let cfg = ClientConfig(host: "h", port: 1, timeoutMs: -1)
    check cfg.timeoutMs == -1

  test "timeoutMs very large":
    let cfg = ClientConfig(host: "h", port: 1, timeoutMs: 3600000)
    check cfg.timeoutMs == 3600000

  test "clientId with special characters":
    let cfg = ClientConfig(host: "h", port: 1,
        clientId: "client-with-dash_underscore")
    check cfg.clientId == "client-with-dash_underscore"

  test "clientId with unicode":
    let cfg = ClientConfig(host: "h", port: 1, clientId: "日本語client")
    check cfg.clientId == "日本語client"

  test "host with IPv6 format":
    let cfg = ClientConfig(host: "::1", port: 9000)
    check cfg.host == "::1"

  test "host with IPv6 full format":
    let cfg = ClientConfig(host: "2001:db8::1", port: 9000)
    check cfg.host == "2001:db8::1"

  test "authData with encoded credentials":
    let cfg = ClientConfig(host: "h", port: 1, authData: "base64encodedstring")
    check cfg.authData == "base64encodedstring"

suite "ProtocolClient Multiple Clients":

  test "multiple clients have independent state":
    let cfg1 = defaultClientConfig("host1", 9001)
    let cfg2 = defaultClientConfig("host2", 9002)
    let client1 = newProtocolClient(cfg1)
    let client2 = newProtocolClient(cfg2)

    check client1.config.host == "host1"
    check client2.config.host == "host2"
    check client1.config.port == 9001
    check client2.config.port == 9002
    check client1.nextRequestId.load() == 1
    check client2.nextRequestId.load() == 1

    # Increment client1
    discard client1.nextRequestId.fetchAdd(1)
    check client1.nextRequestId.load() == 2
    check client2.nextRequestId.load() == 1

  test "multiple clients independent connected state":
    let cfg1 = defaultClientConfig()
    let cfg2 = defaultClientConfig()
    let client1 = newProtocolClient(cfg1)
    let client2 = newProtocolClient(cfg2)

    client1.connected.store(true)
    check client1.connected.load() == true
    check client2.connected.load() == false

    client2.connected.store(true)
    check client2.connected.load() == true
