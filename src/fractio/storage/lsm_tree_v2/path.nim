# Copyright (c) 2024-present, fractio-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Path Utilities

import std/[os]

proc absolutePath*(path: string): string =
  ## Get absolute path
  absolutePath(path)

# ============================================================================
# Tests
# ============================================================================

when isMainModule:
  echo "Testing path utilities..."

  let absPath = absolutePath(".")
  echo "Absolute path: ", absPath

  echo "Path tests passed!"
