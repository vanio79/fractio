#!/bin/bash

# Script to run storage unit tests

echo "Running storage unit tests..."

# Change to project root
cd /home/ingrid/devel/fractio

# Run storage tests
echo "Testing version module..."
nim c -r --checks:on -p:src tests/unit/storage/test_version.nim

echo "Testing error module..."
nim c -r --checks:on -p:src tests/unit/storage/test_error.nim

echo "Testing types module..."
nim c -r --checks:on -p:src tests/unit/storage/test_types.nim

echo "Testing snapshot tracker module..."
nim c -r --checks:on -p:src tests/unit/storage/test_snapshot_tracker.nim

echo "Testing journal entry module..."
nim c -r --checks:on -p:src tests/unit/storage/test_journal_entry.nim

echo "Testing write buffer manager module..."
nim c -r --checks:on -p:src tests/unit/storage/test_write_buffer_manager.nim

echo "Testing stats module..."
nim c -r --checks:on -p:src tests/unit/storage/test_stats.nim

echo "Testing keyspace name module..."
nim c -r --checks:on -p:src tests/unit/storage/test_keyspace_name.nim

echo "Testing keyspace options module..."
nim c -r --checks:on -p:src tests/unit/storage/test_keyspace_options.nim

echo "Testing batch item module..."
nim c -r --checks:on -p:src tests/unit/storage/test_batch_item.nim

echo "Testing file module..."
nim c -r --checks:on -p:src tests/unit/storage/test_file.nim

echo "Testing path module..."
nim c -r --checks:on -p:src tests/unit/storage/test_path.nim

echo "Testing snapshot module..."
nim c -r --checks:on -p:src tests/unit/storage/test_snapshot.nim

echo "Testing poison dart module..."
nim c -r --checks:on -p:src tests/unit/storage/test_poison_dart.nim

echo "Testing journal error module..."
nim c -r --checks:on -p:src tests/unit/storage/test_journal_error.nim

echo "Testing journal manager module..."
nim c -r --checks:on -p:src tests/unit/storage/journal/test_manager.nim

echo "Testing flush manager module..."
nim c -r --checks:on -p:src tests/unit/storage/flush/test_manager.nim

echo "Testing flush worker module..."
nim c -r --checks:on -p:src tests/unit/storage/flush/test_worker.nim

echo "Testing compaction worker module..."
nim c -r --checks:on -p:src tests/unit/storage/compaction/test_worker.nim

echo "All storage tests completed!"