# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Format Version
##
## Disk format version enum.

type
  FormatVersion* = enum
    ## Disk format version
    fvV1 = 1 ## Version 1.x.x
    fvV2 = 2 ## Version 2.x.x
    fvV3 = 3 ## Version 3.x.x

proc `$`*(v: FormatVersion): string =
  case v
  of fvV1: "1"
  of fvV2: "2"
  of fvV3: "3"

proc toUint8*(v: FormatVersion): uint8 =
  case v
  of fvV1: 1
  of fvV2: 2
  of fvV3: 3

proc fromUint8*(v: uint8): FormatVersion =
  case v
  of 1: fvV1
  of 2: fvV2
  of 3: fvV3
  else: fvV3 # Default to latest

when isMainModule:
  echo "Format version: ", fvV3
  echo "To uint8: ", fvV3.toUint8()
