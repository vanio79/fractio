# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/error

# Traits for encoding and decoding configuration
type
  EncodeConfig* = concept self
    proc encode*(self): string

  DecodeConfig* = concept T
    proc decode*(bytes: string): StorageResult[T]
