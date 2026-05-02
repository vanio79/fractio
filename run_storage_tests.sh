#!/bin/bash

# Script to run storage unit tests
# Uses nimble for cross-platform compatibility

echo "Running storage unit tests..."

# Run via nimble (handles paths and flags consistently)
nimble test_unit_storage

echo "All storage tests completed!"
