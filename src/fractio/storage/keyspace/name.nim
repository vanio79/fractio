# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

# Keyspace names can be up to 255 characters long, can not be empty and
# can only contain alphanumerics, underscore (_), dash (-), dot (.), hash tag (#) and dollar ($).

proc isValidKeyspaceName*(s: string): bool =
  if s.len == 0:
    return false

  # Check if length fits in uint8
  return s.len <= 255
