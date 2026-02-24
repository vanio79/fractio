# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## LSM Tree v2 - Error Handling
##
## This module provides error types for the LSM tree implementation.

import std/[strutils, strformat]

# ============================================================================
# Error Types
# ============================================================================

type
  CompressionType* = enum
    ctNone = 0
    ctLz4 = 1

  LsmTreeErrorKind* = enum
    lsmIo               ## I/O error
    lsmDecompress       ## Decompression failed
    lsmInvalidVersion   ## Invalid or unparsable data format version
    lsmUnrecoverable    ## Required files could not be recovered
    lsmChecksumMismatch ## Checksum mismatch
    lsmInvalidTag       ## Invalid enum tag
    lsmInvalidTrailer   ## Invalid block trailer
    lsmInvalidHeader    ## Invalid block header
    lsmUtf8             ## UTF-8 error

  LsmTreeError* = ref object of CatchableError
    case kind*: LsmTreeErrorKind
    of lsmIo:
      ioError*: string
    of lsmDecompress:
      decompressType*: CompressionType
    of lsmInvalidVersion:
      invalidVersion*: uint8
    of lsmChecksumMismatch:
      gotChecksum*: uint64
      expectedChecksum*: uint64
    of lsmInvalidTag:
      tagName*: string
      tagValue*: uint8
    of lsmInvalidTrailer, lsmInvalidHeader, lsmUnrecoverable, lsmUtf8:
      message*: string

# ============================================================================
# Error Constructors
# ============================================================================

proc newIoError*(msg: string): LsmTreeError =
  LsmTreeError(kind: lsmIo, ioError: msg)

proc newDecompressError*(compressType: CompressionType): LsmTreeError =
  LsmTreeError(kind: lsmDecompress, decompressType: compressType)

proc newInvalidVersionError*(version: uint8): LsmTreeError =
  LsmTreeError(kind: lsmInvalidVersion, invalidVersion: version)

proc newUnrecoverableError*(msg: string): LsmTreeError =
  LsmTreeError(kind: lsmUnrecoverable, message: msg)

proc newChecksumMismatchError*(got, expected: uint64): LsmTreeError =
  LsmTreeError(kind: lsmChecksumMismatch, gotChecksum: got,
               expectedChecksum: expected)

proc newInvalidTagError*(tagName: string, tagValue: uint8): LsmTreeError =
  LsmTreeError(kind: lsmInvalidTag, tagName: tagName, tagValue: tagValue)

proc newInvalidTrailerError*(msg: string): LsmTreeError =
  LsmTreeError(kind: lsmInvalidTrailer, message: msg)

proc newInvalidHeaderError*(msg: string): LsmTreeError =
  LsmTreeError(kind: lsmInvalidHeader, message: msg)

proc newUtf8Error*(msg: string): LsmTreeError =
  LsmTreeError(kind: lsmUtf8, message: msg)

# ============================================================================
# Error Display
# ============================================================================

proc `$`*(e: LsmTreeError): string =
  case e.kind
  of lsmIo:
    "LsmTreeError(Io): " & e.ioError
  of lsmDecompress:
    "LsmTreeError(Decompress): " & $e.decompressType
  of lsmInvalidVersion:
    "LsmTreeError(InvalidVersion): " & $e.invalidVersion
  of lsmUnrecoverable:
    "LsmTreeError(Unrecoverable): " & e.message
  of lsmChecksumMismatch:
    "LsmTreeError(ChecksumMismatch): got=" & $e.gotChecksum & ", expected=" &
        $e.expectedChecksum
  of lsmInvalidTag:
    "LsmTreeError(InvalidTag): " & e.tagName & "=" & $e.tagValue
  of lsmInvalidTrailer:
    "LsmTreeError(InvalidTrailer): " & e.message
  of lsmInvalidHeader:
    "LsmTreeError(InvalidHeader): " & e.message
  of lsmUtf8:
    "LsmTreeError(Utf8): " & e.message

# ============================================================================
# Result Type
# ============================================================================

type
  LsmResult*[T] = object
    ## Result type for LSM tree operations
    isOk*: bool
    error*: LsmTreeError
    when T isnot void:
      value*: T

proc ok*[T](val: T): LsmResult[T] =
  LsmResult[T](isOk: true, value: val)

proc okVoid*(): LsmResult[void] =
  LsmResult[void](isOk: true)

proc errVoid*(err: LsmTreeError): LsmResult[void] =
  LsmResult[void](isOk: false, error: err)

proc err*[T](err: LsmTreeError): LsmResult[T] =
  LsmResult[T](isOk: false, error: err)

proc isOk*[T](r: LsmResult[T]): bool {.inline.} =
  r.isOk

proc isErr*[T](r: LsmResult[T]): bool {.inline.} =
  not r.isOk

proc value*[T](r: LsmResult[T]): T =
  assert(r.isOk, "Cannot get value from error result")
  r.value

proc error*[T](r: LsmResult[T]): LsmTreeError =
  assert(not r.isOk, "Cannot get error from ok result")
  r.error

proc `?`*[T](r: LsmResult[T]): T =
  ## Unwrap result or raise
  if r.isOk:
    return r.value
  raise newException(LsmTreeError, $r.error)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing errors..."

  let ioErr = newIoError("file not found")
  echo "IoError: ", ioErr

  let versionErr = newInvalidVersionError(42)
  echo "VersionError: ", versionErr

  let checksumErr = newChecksumMismatchError(123, 456)
  echo "ChecksumError: ", checksumErr

  # Test Result
  let okResult = ok(42)
  echo "Ok result isOk: ", okResult.isOk
  echo "Ok result value: ", okResult.value

  let errResult = err[int](newIoError("test error"))
  echo "Err result isOk: ", errResult.isOk
  echo "Err result error: ", errResult.error

  echo "Error tests passed!"
