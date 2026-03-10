# Package settings
version       = "0.1.0"
author        = "Fractio Team"
description   = "A distributed SQL database with sharding and replication"
license       = "MIT"

# Dependencies
requires "nim >= 2.0.0"
requires "happyx >= 3.0.0"
requires "zippy >= 0.10.0"
requires "parsetoml >= 0.7.0"

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

bin = @["fractio/cli/main"]

import os


import os

task test, "Run all unit, integration, and concurrency tests":
  for file in walkDirRec("tests"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running tests: ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src " & file

task test_storage, "Run only storage engine unit tests":
  # Run storage tests from tests/unit/storage/
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running storage test: ", file
      exec "nim c -r --checks:on -p:src " & file

task build_web, "Compile frontend SPA to JS, minify, then build server binary":
  exec "nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim"
  exec "npx terser src/fractio/web/static/app.js --compress --mangle -o src/fractio/web/static/app.js"
  exec "nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio_web src/fractio/cli/main.nim"

task test_storage_integration, "Run storage integration tests including stress tests":
  # Run storage integration tests from tests/integration/storage/
  for file in walkDirRec("tests/integration/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "Running storage integration test: ", file
      exec "nim c -r --checks:on -p:src " & file
