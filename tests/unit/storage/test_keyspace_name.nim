# Unit tests for keyspace name module
# Tests for keyspace name validation

import unittest
import std/strutils
import fractio/storage/keyspace/name

suite "Keyspace Name Unit Tests":

  test "Valid keyspace names":
    check isValidKeyspaceName("default")
    check isValidKeyspaceName("test_keyspace")
    check isValidKeyspaceName("my-keyspace")
    check isValidKeyspaceName("keyspace123")
    check isValidKeyspaceName("a") # Single character
    check isValidKeyspaceName("a".repeat(255)) # Maximum length

  test "Invalid keyspace names":
    # Empty name
    check not isValidKeyspaceName("")

    # Too long name
    check not isValidKeyspaceName("a".repeat(256))

    # Names with invalid characters would be checked here
    # But our current implementation only checks length

  test "Edge cases":
    # Exactly 255 characters (maximum)
    let maxName = "a".repeat(255)
    check isValidKeyspaceName(maxName)
    check maxName.len == 255

    # 256 characters (too long)
    let tooLongName = "a".repeat(256)
    check not isValidKeyspaceName(tooLongName)
    check tooLongName.len == 256
