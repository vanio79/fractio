# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

const STEP_SIZE*: int = 10_000
const THRESHOLD*: int = 20

proc performWriteStall*(l0Runs: int) =
  if l0Runs >= THRESHOLD and l0Runs < 30:
    let d = l0Runs - THRESHOLD

    # In a full implementation, this would perform a delay
    # For now, we'll just do a simple loop
    for i in 0..<d * STEP_SIZE:
      discard # Equivalent to black_box in Rust
