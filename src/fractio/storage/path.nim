# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import std/os

proc absolutePath*(path: string): string =
  # Convert path to absolute path without requiring file to exist
  # First normalize the path (resolve . and ..)
  let normalized = normalizedPath(path)
  # If already absolute, return normalized
  if isAbsolute(normalized):
    result = normalized
  else:
    # Prepend current directory to make it absolute
    result = normalizedPath(joinPath(getCurrentDir(), normalized))


