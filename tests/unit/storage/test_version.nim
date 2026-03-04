# Unit tests for version module
# Tests for FormatVersion serialization/deserialization

import unittest
import std/options
import fractio/storage/version

suite "Version Unit Tests":

  test "Version serialization V1":
    var buffer: seq[byte] = @[]
    writeFileHeader(fvV1, buffer)
    check buffer == @[byte('F'), byte('J'), byte('L'), 1.byte]

  test "Version serialization V2":
    var buffer: seq[byte] = @[]
    writeFileHeader(fvV2, buffer)
    check buffer == @[byte('F'), byte('J'), byte('L'), 2.byte]

  test "Version serialization V3":
    var buffer: seq[byte] = @[]
    writeFileHeader(fvV3, buffer)
    check buffer == @[byte('F'), byte('J'), byte('L'), 3.byte]

  test "Version deserialization success V1":
    let bytes = @[byte('F'), byte('J'), byte('L'), 1.byte]
    let version = parseFileHeader(bytes)
    check version.isSome
    check version.get == fvV1

  test "Version deserialization success V2":
    let bytes = @[byte('F'), byte('J'), byte('L'), 2.byte]
    let version = parseFileHeader(bytes)
    check version.isSome
    check version.get == fvV2

  test "Version deserialization success V3":
    let bytes = @[byte('F'), byte('J'), byte('L'), 3.byte]
    let version = parseFileHeader(bytes)
    check version.isSome
    check version.get == fvV3

  test "Version deserialization fail invalid magic":
    let bytes = @[byte('F'), byte('J'), byte('X'), 1.byte]
    let version = parseFileHeader(bytes)
    check version.isNone

  test "Version deserialization fail invalid version":
    let bytes = @[byte('F'), byte('J'), byte('L'), 5.byte]
    let version = parseFileHeader(bytes)
    check version.isNone

  test "Version serde round trip":
    var buf: seq[byte] = @[]
    writeFileHeader(fvV1, buf)
    let version = parseFileHeader(buf)
    check version.isSome
    check version.get == fvV1

  test "Version header length":
    var buf: seq[byte] = @[]
    writeFileHeader(fvV1, buf)
    check buf.len == 4
