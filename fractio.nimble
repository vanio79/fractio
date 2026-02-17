# Package settings
version       = "0.1.0"
author        = "Fractio Team"
description   = "A distributed SQL database with sharding and replication"
license       = "MIT"

# Dependencies
requires "nim >= 1.6.0"
requires "json"
requires "msgpack4nim"
requires "nimcrypto >= 0.2.0"
requires "asyncdispatch"
requires "asyncnet"
requires "times"
requires "unittest"
requires "test_stats"
requires "graphs"

# Build targets
skipDirs = @["docs", "tests", "benchmarks", "simulations", "tmp"]
