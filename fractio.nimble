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
# Nim coverage workflow using --debugger:native:
# 1. Compile tests with gcov instrumentation and DWARF debug info
#    (--debugger:native --passC:--coverage --passL:--coverage)
# 2. Run tests to generate .gcda files
# 3. Capture with lcov/geninfo (needs --ignore-errors mismatch,gcov,source)
# 4. Extract only Fractio source files using lcov --extract
# 5. Generate HTML report with genhtml
#
# This approach uses DWARF debug info to map C coverage back to Nim source lines.
# The coverage percentages reflect actual Nim source code, not generated C code.
# All output goes to /tmp/fractio-coverage to avoid cluttering project directory.

const COVERAGE_DIR = "/tmp/fractio-coverage"
const PROJECT_ROOT = "/home/ingrid/devel/fractio"

task coverage_clean, "Clean coverage data files":
  echo "Cleaning coverage data..."
  exec "rm -rf " & COVERAGE_DIR
  echo "Coverage data cleaned."

task coverage_unit, "Run all unit tests with coverage":
  echo "=== Running all unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR
  exec "mkdir -p " & COVERAGE_DIR
  
  let covFlags = "--debugger:native --passC:--coverage --passL:--coverage"
  
  var testFiles: seq[string] = @[]
  for file in walkDirRec("tests/unit"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  
  # Run each test with its own nimcache to avoid stamp mismatches
  var infoFiles: seq[string] = @[]
  for i, file in testFiles:
    let testName = extractFilename(file).replace(".nim", "")
    let cacheDir = COVERAGE_DIR & "/cache_" & testName
    echo "  [", i+1, "/", testFiles.len, "] ", file
    exec "nim c -r --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
    # Capture coverage for this test
    let infoFile = COVERAGE_DIR & "/test_" & testName & ".info"
    exec "geninfo " & cacheDir & " --output-file " & infoFile & " --branch-coverage --ignore-errors mismatch,gcov,source --keep-going --base-directory . || true"
    if fileExists(infoFile):
      infoFiles.add(infoFile)
  
  # Merge all coverage data
  echo "Merging coverage data from ", infoFiles.len, " tests..."
  if infoFiles.len > 0:
    var mergeCmd = "lcov"
    for infoFile in infoFiles:
      mergeCmd = mergeCmd & " --add-tracefile " & infoFile
    mergeCmd = mergeCmd & " --output-file " & COVERAGE_DIR & "/coverage_merged.info --branch-coverage --ignore-errors mismatch,gcov,format,corrupt"
    exec mergeCmd
    
    echo "Generating HTML report (with branch coverage)..."
    # Use --include to filter files in genhtml (preserves branch coverage)
    # Note: lcov --extract removes branch data, so we use genhtml filtering instead
    exec "genhtml " & COVERAGE_DIR & "/coverage_merged.info --output-directory " & COVERAGE_DIR & "/html --title 'Fractio Coverage' --legend --branch-coverage --include '" & PROJECT_ROOT & "/src/*' --include '" & PROJECT_ROOT & "/tests/*' --ignore-errors unmapped,empty"
    
    echo ""
    echo "=========================================="
    echo "Coverage report: " & COVERAGE_DIR & "/html/index.html"
    echo "=========================================="
  else:
    echo "No coverage data collected."

task coverage_unit_core, "Run core unit tests with coverage":
  echo "=== Running core unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR
  exec "mkdir -p " & COVERAGE_DIR
  
  let covFlags = "--debugger:native --passC:--coverage --passL:--coverage"
  
  # Collect tests from multiple directories to maximize coverage
  var testFiles: seq[string] = @[]
  # Core tests
  for file in walkDirRec("tests/unit/core"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Network tests (packetcodec)
  for file in walkDirRec("tests/unit/network"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Distributed tests (all subdirectories)
  for file in walkDirRec("tests/unit/distributed"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Storage tests
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # DI tests
  for file in walkDirRec("tests/unit/di"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # App tests
  for file in walkDirRec("tests/unit/app"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Utils tests
  for file in walkDirRec("tests/unit/utils"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Protocol tests
  for file in walkDirRec("tests/unit/protocol"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # CLI tests
  for file in walkDirRec("tests/unit/cli"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # Client tests
  for file in walkDirRec("tests/unit/client"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  # SQL tests
  for file in walkDirRec("tests/unit/sql"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  
  # Run each test with its own nimcache to avoid stamp mismatches
  var infoFiles: seq[string] = @[]
  for i, file in testFiles:
    let testName = extractFilename(file).replace(".nim", "")
    let cacheDir = COVERAGE_DIR & "/cache_" & testName
    echo "  [", i+1, "/", testFiles.len, "] ", file
    exec "nim c -r --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
    # Capture coverage for this test (including branch coverage)
    let infoFile = COVERAGE_DIR & "/test_" & testName & ".info"
    exec "geninfo " & cacheDir & " --branch-coverage --output-file " & infoFile & " --ignore-errors mismatch,gcov,source --keep-going --base-directory . || true"
    if fileExists(infoFile):
      infoFiles.add(infoFile)
  
  # Merge all coverage data incrementally
  echo "Merging coverage data from ", infoFiles.len, " tests..."
  if infoFiles.len > 0:
    # Start with first file as base
    exec "cp " & infoFiles[0] & " " & COVERAGE_DIR & "/core_merged.info"
    
    # Add remaining files one at a time to avoid command line length limits
    for i, infoFile in infoFiles:
      if i > 0:
        let remaining = infoFiles.len - i
        echo "Merging " & infoFile & ".." & $remaining & " remaining"
        exec "lcov --branch-coverage --add-tracefile " & COVERAGE_DIR & "/core_merged.info --add-tracefile " & infoFile & " --output-file " & COVERAGE_DIR & "/core_merged.info --branch-coverage --ignore-errors mismatch,gcov"
    
    echo "Generating HTML report (with branch coverage)..."
    # Use --include to filter files in genhtml (preserves branch coverage)
    exec "genhtml " & COVERAGE_DIR & "/core_merged.info --branch-coverage --output-directory " & COVERAGE_DIR & "/html_core --title 'Fractio Core Coverage' --legend --include '" & PROJECT_ROOT & "/src/*' --include '" & PROJECT_ROOT & "/tests/*' --ignore-errors unmapped,empty"
    
    echo ""
    echo "=========================================="
    echo "Core coverage: " & COVERAGE_DIR & "/html_core/index.html"
    echo "=========================================="
  else:
    echo "No coverage data collected."

task coverage_unit_storage, "Run storage unit tests with coverage":
  echo "=== Running storage unit tests with coverage ==="
  exec "rm -rf " & COVERAGE_DIR
  exec "mkdir -p " & COVERAGE_DIR
  
  let covFlags = "--debugger:native --passC:--coverage --passL:--coverage"
  
  var testFiles: seq[string] = @[]
  for file in walkDirRec("tests/unit/storage"):
    let name = extractFilename(file)
    if name.startsWith("test_") and name.endsWith(".nim"):
      testFiles.add(file)
  
  # Run each test with its own nimcache to avoid stamp mismatches
  var infoFiles: seq[string] = @[]
  for i, file in testFiles:
    let testName = extractFilename(file).replace(".nim", "")
    let cacheDir = COVERAGE_DIR & "/cache_" & testName
    echo "  [", i+1, "/", testFiles.len, "] ", file
    exec "nim c -r --mm:atomicArc --nimcache:" & cacheDir & " -p:src -p:tests " & covFlags & " " & file
    # Capture coverage for this test
    let infoFile = COVERAGE_DIR & "/test_" & testName & ".info"
    exec "geninfo " & cacheDir & " --output-file " & infoFile & " --branch-coverage --ignore-errors mismatch,gcov,source --keep-going --base-directory . || true"
    if fileExists(infoFile):
      infoFiles.add(infoFile)
  
  # Merge all coverage data
  echo "Merging coverage data from ", infoFiles.len, " tests..."
  if infoFiles.len > 0:
    var mergeCmd = "lcov"
    for infoFile in infoFiles:
      mergeCmd = mergeCmd & " --add-tracefile " & infoFile
    mergeCmd = mergeCmd & " --output-file " & COVERAGE_DIR & "/storage_merged.info --branch-coverage --ignore-errors mismatch,gcov"
    exec mergeCmd
    
    echo "Generating HTML report (with branch coverage)..."
    # Use --include to filter files in genhtml (preserves branch coverage)
    exec "genhtml " & COVERAGE_DIR & "/storage_merged.info --output-directory " & COVERAGE_DIR & "/html_storage --title 'Fractio Storage Coverage' --legend --branch-coverage --include '" & PROJECT_ROOT & "/src/*' --include '" & PROJECT_ROOT & "/tests/*' --ignore-errors unmapped,empty"
    
    echo ""
    echo "=========================================="
    echo "Storage coverage: " & COVERAGE_DIR & "/html_storage/index.html"
    echo "=========================================="
  else:
    echo "No coverage data collected."

task coverage_summary, "Show coverage summary":
  var fractioInfo = COVERAGE_DIR & "/core_filtered.info"
  if not fileExists(fractioInfo):
    fractioInfo = COVERAGE_DIR & "/core_fractio.info"
  if not fileExists(fractioInfo):
    # Try other merged files
    for f in ["storage_fractio.info", "coverage_fractio.info"]:
      if fileExists(COVERAGE_DIR & "/" & f):
        fractioInfo = COVERAGE_DIR & "/" & f
        break
  if not fileExists(fractioInfo):
    echo "No coverage data found. Run nimble coverage_unit_core first."
  else:
    exec "lcov --summary " & fractioInfo

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