import unittest
import std/options
import fractio/storage/version

suite "Minimal Test":
  test "test isSome":
    let bytes = @[byte('F'), byte('J'), byte('L'), 1.byte]
    let version = parseFileHeader(bytes)
    check version.isSome
