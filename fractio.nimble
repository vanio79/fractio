# Package settings
version       = "0.1.0"
author        = "Fractio Team"
description   = "A distributed SQL database with sharding and replication"
license       = "MIT"

# Dependencies
requires "nim >= 1.6.0"

# Source directory
srcDir = "src"

# Build targets
skipDirs = @[
  "docs",
  "tests",
  "benchmarks",
  "simulations",
  "tmp"
]

bin = @[]

import os


import os

task test, "Run all unit, integration, and concurrency tests":
  for file in walkDirRec("tests"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running tests: ", file
      exec "nim c -r --checks:on -p:src " & file

task test_storage, "Run only storage engine unit tests":
  # Run storage tests from tests/unit/storage/
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running storage test: ", file
      exec "nim c -r --checks:on -p:src " & file

task test_storage_integration, "Run storage integration tests including stress tests":
  # Run storage integration tests from tests/integration/storage/
  for file in walkDirRec("tests/integration/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running storage integration test: ", file
      exec "nim c -r --checks:on -p:src " & file
