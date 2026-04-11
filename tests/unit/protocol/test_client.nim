# Unit tests for fractio/protocol/client.nim
# Tests ClientConfig, ProtocolClient constructor and config

import std/[unittest, atomics, locks]
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
