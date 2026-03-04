# Unit tests for path module
# Tests for path utilities

import unittest
import std/strutils
import fractio/storage/path

suite "Path Unit Tests":

  test "Absolute path conversion":
    # Test with relative path
    let relPath = "test/file.txt"
    let absPath = absolutePath(relPath)
    check absPath.len > 0
    check absPath.contains("file.txt")

    # Test with already absolute path
    let alreadyAbsPath = "/tmp/test.txt"
    let resultAbsPath = absolutePath(alreadyAbsPath)
    check resultAbsPath.len > 0

    # Test with current directory
    let currentPath = "."
    let resultCurrentPath = absolutePath(currentPath)
    check resultCurrentPath.len > 0

  test "Path normalization":
    # Test path with dots
    let dottedPath = "./test/../file.txt"
    let normalizedPath = absolutePath(dottedPath)
    check normalizedPath.len > 0
    check normalizedPath.contains("file.txt")

    # Test path with double slashes
    let doubleSlashPath = "test//file.txt"
    let normalizedDoublePath = absolutePath(doubleSlashPath)
    check normalizedDoublePath.len > 0
