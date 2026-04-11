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

# Unit tests - fast, no external dependencies, test individual components
task test_unit, "Run all unit tests (fast, isolated component tests)":
  echo "Running unit tests..."
  var count = 0
  for file in walkDirRec("tests/unit"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  [unit] ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file
      inc count
  echo "Completed ", count, " unit tests"

# Integration tests - slower, require real infrastructure (Raft, network, storage)
task test_integration, "Run all integration tests (multi-component, real infrastructure)":
  echo "Running integration tests..."
  var count = 0
  for file in walkDirRec("tests/integration"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endswith(".nim"):
      echo "  [integration] ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file
      inc count
  echo "Completed ", count, " integration tests"

# Combined test task - runs both unit and integration
task test, "Run all tests (unit + integration)":
  echo "=== Running all tests ==="
  # Run unit tests first (fast, catch basic errors)
  for file in walkDirRec("tests/unit"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  [unit] ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file
  # Then run integration tests
  for file in walkDirRec("tests/integration"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  [integration] ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file
  echo "=== All tests completed ==="

# Specific subsystem unit tests
task test_unit_core, "Run core module unit tests":
  echo "Running core unit tests..."
  for file in walkDirRec("tests/unit/core"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_storage, "Run storage engine unit tests":
  echo "Running storage unit tests..."
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_sql, "Run SQL module unit tests":
  echo "Running SQL unit tests..."
  for file in walkDirRec("tests/unit/sql"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_distributed, "Run distributed module unit tests":
  echo "Running distributed unit tests..."
  for file in walkDirRec("tests/unit/distributed"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_protocol, "Run protocol module unit tests":
  echo "Running protocol unit tests..."
  for file in walkDirRec("tests/unit/protocol"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_di, "Run DI infrastructure unit tests":
  echo "Running DI unit tests..."
  for file in walkDirRec("tests/unit/di"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_app, "Run application/bootstrap unit tests":
  echo "Running app unit tests..."
  for file in walkDirRec("tests/unit/app"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_unit_network, "Run network module unit tests":
  echo "Running network unit tests..."
  for file in walkDirRec("tests/unit/network"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

# Specific subsystem integration tests
task test_integration_app, "Run app integration tests":
  echo "Running app integration tests..."
  for file in walkDirRec("tests/integration/app"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_integration_distributed, "Run distributed integration tests":
  echo "Running distributed integration tests..."
  for file in walkDirRec("tests/integration/distributed"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_integration_sql, "Run SQL integration tests":
  echo "Running SQL integration tests..."
  for file in walkDirRec("tests/integration/sql"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_integration_storage, "Run storage integration tests":
  echo "Running storage integration tests..."
  for file in walkDirRec("tests/integration/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

task test_integration_protocol, "Run protocol integration tests":
  echo "Running protocol integration tests..."
  for file in walkDirRec("tests/integration/protocol"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "  ", file
      exec "nim c -r --checks:on --mm:atomicArc -p:src -p:tests " & file

# Web frontend build
task build_web, "Compile frontend SPA to JS, minify, then build server binary":
  exec "nim js -d:release -o:src/fractio/web/static/app.js src/fractio/web/frontend.nim"
  exec "npx terser src/fractio/web/static/app.js --compress --mangle -o src/fractio/web/static/app.js"
  exec "nim c -d:beast --mm:atomicArc --threads:on -p:src -o:bin/fractio_web src/fractio/cli/main.nim"

# =============================================================================
# Code Coverage Tasks
# =============================================================================
# Nim uses GCC/Clang instrumentation for coverage via --passc and --passl flags.
# IMPORTANT: Use --nimcache:/tmp/fractio-coverage/cache_<testname> per test to avoid checksum errors.
# Different tests compile different code paths, causing gcov checksum mismatches.
# Reports generated via lcov/genhtml, merging per-test coverage data.
# Coverage data is stored in /tmp/fractio-coverage to avoid polluting the project directory.

const COVERAGE_DIR = "/tmp/fractio-coverage"

task coverage_clean, "Clean coverage data files":
  echo "Cleaning coverage data..."
  exec "rm -rf " & COVERAGE_DIR
  echo "Coverage data cleaned."

task coverage_unit, "Run unit tests with coverage instrumentation and generate report":
  echo "=== Running unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR & " && mkdir -p " & COVERAGE_DIR & "/html"
  
  # Coverage compilation flags
  let covFlags = "--passc:-fprofile-arcs --passc:-ftest-coverage --passl:-lgcov --passl:-fprofile-arcs --passl:-ftest-coverage"
  
  # Run each test with unique cache directory to avoid checksum conflicts
  var testNum = 0
  for file in walkDirRec("tests/unit"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testNum = testNum + 1
      let cacheDir = COVERAGE_DIR & "/cache_" & $testNum
      echo "  [coverage] ", file
      exec "nim c -r --checks:on --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
      # Capture coverage from this test's cache
      exec "lcov --capture --directory " & cacheDir & " --output-file " & COVERAGE_DIR & "/test_" & $testNum & ".info --ignore-errors gcov 2>/dev/null || true"
  
  # Merge all coverage data
  echo "Merging coverage data from ", testNum, " tests..."
  if testNum > 0:
    # Build list of info files to merge
    var mergeCmd = "lcov"
    for i in 1..testNum:
      mergeCmd = mergeCmd & " --add-tracefile " & COVERAGE_DIR & "/test_" & $i & ".info"
    mergeCmd = mergeCmd & " --output-file " & COVERAGE_DIR & "/coverage.info --ignore-errors mismatch,gcov 2>/dev/null || true"
    exec mergeCmd
    
    # Filter out external dependencies
    exec "lcov --remove " & COVERAGE_DIR & "/coverage.info '/usr/*' '*@nrandom*' '*@nulid*' '*@mtest*' --output-file " & COVERAGE_DIR & "/coverage.filtered.info --ignore-errors unused,source 2>/dev/null || true"
    exec "genhtml " & COVERAGE_DIR & "/coverage.filtered.info --output-directory " & COVERAGE_DIR & "/html --ignore-errors source 2>/dev/null || true"
    
    echo "Coverage report generated in " & COVERAGE_DIR & "/html/"
    echo "Open " & COVERAGE_DIR & "/html/index.html to view the report."
    exec "lcov --summary " & COVERAGE_DIR & "/coverage.filtered.info --ignore-errors unused 2>/dev/null || true"
  else:
    echo "No unit tests found."

task coverage_unit_core, "Run core unit tests with coverage":
  echo "=== Running core unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR & " && mkdir -p " & COVERAGE_DIR & "/html_core"
  
  let covFlags = "--passc:-fprofile-arcs --passc:-ftest-coverage --passl:-lgcov --passl:-fprofile-arcs --passl:-ftest-coverage"
  
  var testNum = 0
  for file in walkDirRec("tests/unit/core"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testNum = testNum + 1
      let cacheDir = COVERAGE_DIR & "/cache_" & $testNum
      echo "  [coverage] ", file
      exec "nim c -r --checks:on --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
      exec "lcov --capture --directory " & cacheDir & " --output-file " & COVERAGE_DIR & "/test_" & $testNum & ".info --ignore-errors gcov 2>/dev/null || true"
  
  echo "Merging coverage data from ", testNum, " tests..."
  if testNum > 0:
    var mergeCmd = "lcov"
    for i in 1..testNum:
      mergeCmd = mergeCmd & " --add-tracefile " & COVERAGE_DIR & "/test_" & $i & ".info"
    mergeCmd = mergeCmd & " --output-file " & COVERAGE_DIR & "/core.info --ignore-errors mismatch,gcov 2>/dev/null || true"
    exec mergeCmd
    
    exec "lcov --remove " & COVERAGE_DIR & "/core.info '/usr/*' '*@nrandom*' '*@nulid*' '*@mtest*' --output-file " & COVERAGE_DIR & "/core.filtered.info --ignore-errors unused,source 2>/dev/null || true"
    exec "genhtml " & COVERAGE_DIR & "/core.filtered.info --output-directory " & COVERAGE_DIR & "/html_core --ignore-errors source 2>/dev/null || true"
    
    echo "Core coverage report: " & COVERAGE_DIR & "/html_core/"
    exec "lcov --summary " & COVERAGE_DIR & "/core.filtered.info --ignore-errors unused 2>/dev/null || echo 'No coverage data.'"

task coverage_unit_di, "Run DI unit tests with coverage":
  echo "=== Running DI unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR & " && mkdir -p " & COVERAGE_DIR & "/html_di"
  
  let covFlags = "--passc:-fprofile-arcs --passc:-ftest-coverage --passl:-lgcov --passl:-fprofile-arcs --passl:-ftest-coverage"
  
  var testNum = 0
  for file in walkDirRec("tests/unit/di"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testNum = testNum + 1
      let cacheDir = COVERAGE_DIR & "/cache_" & $testNum
      echo "  [coverage] ", file
      exec "nim c -r --checks:on --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
      exec "lcov --capture --directory " & cacheDir & " --output-file " & COVERAGE_DIR & "/test_" & $testNum & ".info --ignore-errors gcov 2>/dev/null || true"
  
  echo "Merging coverage data from ", testNum, " tests..."
  if testNum > 0:
    var mergeCmd = "lcov"
    for i in 1..testNum:
      mergeCmd = mergeCmd & " --add-tracefile " & COVERAGE_DIR & "/test_" & $i & ".info"
    mergeCmd = mergeCmd & " --output-file " & COVERAGE_DIR & "/di.info --ignore-errors mismatch,gcov 2>/dev/null || true"
    exec mergeCmd
    
    exec "lcov --remove " & COVERAGE_DIR & "/di.info '/usr/*' '*@nrandom*' '*@nulid*' '*@mtest*' --output-file " & COVERAGE_DIR & "/di.filtered.info --ignore-errors unused,source 2>/dev/null || true"
    exec "genhtml " & COVERAGE_DIR & "/di.filtered.info --output-directory " & COVERAGE_DIR & "/html_di --ignore-errors source 2>/dev/null || true"
    
    echo "DI coverage report: " & COVERAGE_DIR & "/html_di/"
    exec "lcov --summary " & COVERAGE_DIR & "/di.filtered.info --ignore-errors unused 2>/dev/null || echo 'No coverage data.'"

task coverage_report, "Generate HTML coverage report from existing coverage data":
  echo "Generating coverage report..."
  exec "mkdir -p " & COVERAGE_DIR & "/html"
  # Find all existing test info files and merge
  var mergeCmd = "lcov --initial --capture --directory " & COVERAGE_DIR & " --output-file " & COVERAGE_DIR & "/coverage.info --ignore-errors gcov 2>/dev/null || true"
  exec mergeCmd
  exec "lcov --remove " & COVERAGE_DIR & "/coverage.info '/usr/*' '*@nrandom*' '*@nulid*' '*@mtest*' --output-file " & COVERAGE_DIR & "/coverage.filtered.info --ignore-errors unused,source 2>/dev/null || true"
  exec "genhtml " & COVERAGE_DIR & "/coverage.filtered.info --output-directory " & COVERAGE_DIR & "/html --ignore-errors source 2>/dev/null || true"
  echo "Report generated: " & COVERAGE_DIR & "/html/index.html"

task coverage_summary, "Show coverage summary (text report)":
  exec "lcov --summary " & COVERAGE_DIR & "/coverage.filtered.info --ignore-errors unused 2>/dev/null || echo 'No coverage data found. Run nimble coverage_unit first.'"

# =============================================================================
# Deprecated aliases (kept for backward compatibility)
# =============================================================================
task test_storage, "Run storage unit tests (deprecated: use test_unit_storage)":
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "Running storage test: ", file
      exec "nim c -r --checks:on -p:src -p:tests " & file

task test_storage_integration, "Run storage integration tests (deprecated: use test_integration_storage)":
  for file in walkDirRec("tests/integration/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      echo "Running storage integration test: ", file
      exec "nim c -r --checks:on -p:src -p:tests " & file