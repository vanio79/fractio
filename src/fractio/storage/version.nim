# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/error

# Disk format version
type
  FormatVersion* = enum
    fvV1 # Version for 1.x.x releases
    fvV2 # Version for 2.x.x releases
    fvV3 # Version for 3.x.x releases

         # Convert FormatVersion to uint8
proc toUint8*(version: FormatVersion): uint8 =
  case version
  of fvV1: 1
  of fvV2: 2
  of fvV3: 3

# Convert uint8 to FormatVersion
proc fromUint8*(value: uint8): StorageResult[FormatVersion] =
  case value
  of 1: ok(fvV1)
  of 2: ok(fvV2)
  of 3: ok(fvV3)
  else: err(StorageError(kind: seInvalidVersion, invalidVersion: some(uint32(value))))

# String representation
proc `$`*(version: FormatVersion): string =
  $version.toUint8()

const MAGIC_BYTES*: array[3, byte] = [byte('F'), byte('J'), byte('L')]

# Parse file header
proc parseFileHeader*(bytes: seq[byte]): Option[FormatVersion] =
  if bytes.len < 4:
    return none(FormatVersion)

  # Check magic bytes
  if bytes[0] != MAGIC_BYTES[0] or bytes[1] != MAGIC_BYTES[1] or bytes[2] !=
      MAGIC_BYTES[2]:
    return none(FormatVersion)

  # Get version byte
  let versionByte = bytes[3]

  # Try to convert to FormatVersion
  let versionResult = fromUint8(versionByte)
  if versionResult.isOk():
    return some(versionResult.get())
  else:
    return none(FormatVersion)

# Write file header
proc writeFileHeader*(version: FormatVersion, buffer: var seq[byte]) =
  # Add magic bytes
  buffer.add(MAGIC_BYTES)

  # Add version byte
  buffer.add(version.toUint8())
