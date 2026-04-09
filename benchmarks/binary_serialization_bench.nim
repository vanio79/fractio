# Microbenchmark: Binary Serialization Performance
#
# Compares byte-by-byte encoding vs direct memory copy on LE systems.
#
# Run: nim c -r -d:release --mm:atomicArc -p:src benchmarks/binary_serialization_bench.nim

import std/[times, strformat, strutils]
import fractio/utils/binary

# =============================================================================
# Optimized LE implementations (for comparison)
# =============================================================================

proc writeU32LE(w: var BinaryWriter, value: uint32) {.inline.} =
  ## Optimized uint32 write for little-endian systems
  w.ensureCapacity(4)
  when defined(littleEndian):
    copyMem(addr w.data[w.pos], addr value, 4)
  else:
    w.data[w.pos] = byte(value and 0xFF'u32)
    w.data[w.pos + 1] = byte((value shr 8) and 0xFF'u32)
    w.data[w.pos + 2] = byte((value shr 16) and 0xFF'u32)
    w.data[w.pos + 3] = byte((value shr 24) and 0xFF'u32)
  inc w.pos, 4

proc writeU64LE(w: var BinaryWriter, value: uint64) {.inline.} =
  ## Optimized uint64 write for little-endian systems
  w.ensureCapacity(8)
  when defined(littleEndian):
    copyMem(addr w.data[w.pos], addr value, 8)
  else:
    w.data[w.pos] = byte(value and 0xFF'u64)
    w.data[w.pos + 1] = byte((value shr 8) and 0xFF'u64)
    w.data[w.pos + 2] = byte((value shr 16) and 0xFF'u64)
    w.data[w.pos + 3] = byte((value shr 24) and 0xFF'u64)
    w.data[w.pos + 4] = byte((value shr 32) and 0xFF'u64)
    w.data[w.pos + 5] = byte((value shr 40) and 0xFF'u64)
    w.data[w.pos + 6] = byte((value shr 48) and 0xFF'u64)
    w.data[w.pos + 7] = byte((value shr 56) and 0xFF'u64)
  inc w.pos, 8

proc readU32LE(r: var BinaryReader): uint32 {.inline.} =
  ## Optimized uint32 read for little-endian systems
  if r.pos + 4 > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  when defined(littleEndian):
    copyMem(addr result, addr r.data[r.pos], 4)
  else:
    result = uint32(uint8(r.data[r.pos])) or
             (uint32(uint8(r.data[r.pos + 1])) shl 8) or
             (uint32(uint8(r.data[r.pos + 2])) shl 16) or
             (uint32(uint8(r.data[r.pos + 3])) shl 24)
  inc r.pos, 4

proc readU64LE(r: var BinaryReader): uint64 {.inline.} =
  ## Optimized uint64 read for little-endian systems
  if r.pos + 8 > r.data.len:
    raise newException(ValueError, "BinaryReader: unexpected end of data")
  when defined(littleEndian):
    copyMem(addr result, addr r.data[r.pos], 8)
  else:
    result = uint64(uint8(r.data[r.pos])) or
             (uint64(uint8(r.data[r.pos + 1])) shl 8) or
             (uint64(uint8(r.data[r.pos + 2])) shl 16) or
             (uint64(uint8(r.data[r.pos + 3])) shl 24) or
             (uint64(uint8(r.data[r.pos + 4])) shl 32) or
             (uint64(uint8(r.data[r.pos + 5])) shl 40) or
             (uint64(uint8(r.data[r.pos + 6])) shl 48) or
             (uint64(uint8(r.data[r.pos + 7])) shl 56)
  inc r.pos, 8

# =============================================================================
# Benchmark helpers
# =============================================================================

proc formatThroughput(ops: int, durationNs: int64): string =
  let opsPerSec = float(ops) / (float(durationNs) / 1_000_000_000.0)
  if opsPerSec >= 1_000_000_000.0:
    &"{opsPerSec / 1_000_000_000.0:.2f} B ops/sec"
  elif opsPerSec >= 1_000_000.0:
    &"{opsPerSec / 1_000_000.0:.2f} M ops/sec"
  else:
    &"{opsPerSec / 1_000.0:.2f} K ops/sec"

proc formatBytes(bytes: int, durationNs: int64): string =
  let bytesPerSec = float(bytes) / (float(durationNs) / 1_000_000_000.0)
  if bytesPerSec >= 1_000_000_000.0:
    &"{bytesPerSec / 1_000_000_000.0:.2f} GB/sec"
  elif bytesPerSec >= 1_000_000.0:
    &"{bytesPerSec / 1_000_000.0:.2f} MB/sec"
  else:
    &"{bytesPerSec / 1_000.0:.2f} KB/sec"

# =============================================================================
# Benchmarks
# =============================================================================

const
  Iterations = 10_000_000 # 10M iterations for stable timing
  Warmup = 1_000_000      # Warmup iterations

template bench(name: string, body: untyped): (int64, int) =
  # Warmup
  for _ in 0..<Warmup:
    body

  # Actual benchmark
  let start = getTime()
  for _ in 0..<Iterations:
    body
  let elapsed = getTime() - start
  (elapsed.inNanoseconds, Iterations)

proc benchWriteU32(): tuple[current, optimized: int64] =
  echo "\n=== writeU32 Benchmark ==="

  # Current implementation
  var w1 = initBinaryWriter(Iterations * 4 + 1000)
  let (duration1, _) = bench("writeU32 current"):
    w1.pos = 0
    w1.writeU32(0x12345678'u32)
  result.current = duration1

  # Optimized implementation
  var w2 = initBinaryWriter(Iterations * 4 + 1000)
  let (duration2, _) = bench("writeU32 optimized"):
    w2.pos = 0
    w2.writeU32LE(0x12345678'u32)
  result.optimized = duration2

  let bytesProcessed = Iterations * 4
  echo &"  Current:    {formatThroughput(Iterations, duration1):>18}  {formatBytes(bytesProcessed, duration1):>12}"
  echo &"  Optimized:  {formatThroughput(Iterations, duration2):>18}  {formatBytes(bytesProcessed, duration2):>12}"
  let speedup = float(duration1) / float(duration2)
  echo &"  Speedup:    {speedup:.2f}x"

proc benchWriteU64(): tuple[current, optimized: int64] =
  echo "\n=== writeU64 Benchmark ==="

  # Current implementation
  var w1 = initBinaryWriter(Iterations * 8 + 1000)
  let (duration1, _) = bench("writeU64 current"):
    w1.pos = 0
    w1.writeU64(0x0123456789ABCDEF'u64)
  result.current = duration1

  # Optimized implementation
  var w2 = initBinaryWriter(Iterations * 8 + 1000)
  let (duration2, _) = bench("writeU64 optimized"):
    w2.pos = 0
    w2.writeU64LE(0x0123456789ABCDEF'u64)
  result.optimized = duration2

  let bytesProcessed = Iterations * 8
  echo &"  Current:    {formatThroughput(Iterations, duration1):>18}  {formatBytes(bytesProcessed, duration1):>12}"
  echo &"  Optimized:  {formatThroughput(Iterations, duration2):>18}  {formatBytes(bytesProcessed, duration2):>12}"
  let speedup = float(duration1) / float(duration2)
  echo &"  Speedup:    {speedup:.2f}x"

proc benchReadU32(): tuple[current, optimized: int64] =
  echo "\n=== readU32 Benchmark ==="

  # Prepare data
  var w = initBinaryWriter(Iterations * 4 + 1000)
  for _ in 0..<Iterations:
    w.writeU32(0x12345678'u32)
  let data = w.finish()

  # Current implementation
  var r1 = initBinaryReader(data)
  var v1: uint32
  let (duration1, _) = bench("readU32 current"):
    r1.pos = 0
    v1 = r1.readU32()
  result.current = duration1

  # Optimized implementation
  var r2 = initBinaryReader(data)
  var v2: uint32
  let (duration2, _) = bench("readU32 optimized"):
    r2.pos = 0
    v2 = r2.readU32LE()
  result.optimized = duration2

  let bytesProcessed = Iterations * 4
  echo &"  Current:    {formatThroughput(Iterations, duration1):>18}  {formatBytes(bytesProcessed, duration1):>12}"
  echo &"  Optimized:  {formatThroughput(Iterations, duration2):>18}  {formatBytes(bytesProcessed, duration2):>12}"
  let speedup = float(duration1) / float(duration2)
  echo &"  Speedup:    {speedup:.2f}x"

proc benchReadU64(): tuple[current, optimized: int64] =
  echo "\n=== readU64 Benchmark ==="

  # Prepare data
  var w = initBinaryWriter(Iterations * 8 + 1000)
  for _ in 0..<Iterations:
    w.writeU64(0x0123456789ABCDEF'u64)
  let data = w.finish()

  # Current implementation
  var r1 = initBinaryReader(data)
  var v1: uint64
  let (duration1, _) = bench("readU64 current"):
    r1.pos = 0
    v1 = r1.readU64()
  result.current = duration1

  # Optimized implementation
  var r2 = initBinaryReader(data)
  var v2: uint64
  let (duration2, _) = bench("readU64 optimized"):
    r2.pos = 0
    v2 = r2.readU64LE()
  result.optimized = duration2

  let bytesProcessed = Iterations * 8
  echo &"  Current:    {formatThroughput(Iterations, duration1):>18}  {formatBytes(bytesProcessed, duration1):>12}"
  echo &"  Optimized:  {formatThroughput(Iterations, duration2):>18}  {formatBytes(bytesProcessed, duration2):>12}"
  let speedup = float(duration1) / float(duration2)
  echo &"  Speedup:    {speedup:.2f}x"

proc benchRealisticMessage(): tuple[current, optimized: int64] =
  echo "\n=== Realistic Message Benchmark (mixed fields) ==="
  echo "  Simulating a typical cluster message with 10 uint32 and 5 uint64 fields"

  const MsgIterations = 1_000_000

  # Current implementation
  let (duration1, _) = bench("message current"):
    var w = initBinaryWriter(80)
    w.writeU32(1'u32)
    w.writeU32(2'u32)
    w.writeU32(3'u32)
    w.writeU32(4'u32)
    w.writeU32(5'u32)
    w.writeU64(100'u64)
    w.writeU64(200'u64)
    w.writeU32(6'u32)
    w.writeU32(7'u32)
    w.writeU32(8'u32)
    w.writeU64(300'u64)
    w.writeU64(400'u64)
    w.writeU32(9'u32)
    w.writeU32(10'u32)
    w.writeU64(500'u64)
  result.current = duration1

  # Optimized implementation
  let (duration2, _) = bench("message optimized"):
    var w = initBinaryWriter(80)
    w.writeU32LE(1'u32)
    w.writeU32LE(2'u32)
    w.writeU32LE(3'u32)
    w.writeU32LE(4'u32)
    w.writeU32LE(5'u32)
    w.writeU64LE(100'u64)
    w.writeU64LE(200'u64)
    w.writeU32LE(6'u32)
    w.writeU32LE(7'u32)
    w.writeU32LE(8'u32)
    w.writeU64LE(300'u64)
    w.writeU64LE(400'u64)
    w.writeU32LE(9'u32)
    w.writeU32LE(10'u32)
    w.writeU64LE(500'u64)
  result.optimized = duration2

  let bytesProcessed = MsgIterations * 70
  echo &"  Current:    {formatThroughput(MsgIterations, duration1):>18}  {formatBytes(bytesProcessed, duration1):>12}"
  echo &"  Optimized:  {formatThroughput(MsgIterations, duration2):>18}  {formatBytes(bytesProcessed, duration2):>12}"
  let speedup = float(duration1) / float(duration2)
  echo &"  Speedup:    {speedup:.2f}x"

# =============================================================================
# Main
# =============================================================================

when isMainModule:
  echo "=========================================="
  echo "Binary Serialization Microbenchmark"
  echo "=========================================="
  echo "Iterations per test: " & $Iterations
  echo "Architecture: " & (when defined(
      littleEndian): "Little-Endian" else: "Big-Endian")
  echo "Compiler: " & (when defined(release): "Release" else: "Debug")

  let w32 = benchWriteU32()
  let w64 = benchWriteU64()
  let r32 = benchReadU32()
  let r64 = benchReadU64()
  let msg = benchRealisticMessage()

  echo "\n=========================================="
  echo "Summary"
  echo "=========================================="

  proc pctImprovement(current, optimized: int64): float =
    100.0 * (float(current) - float(optimized)) / float(current)

  echo &"  writeU32:  {pctImprovement(w32.current, w32.optimized):+.1f}% improvement"
  echo &"  writeU64:  {pctImprovement(w64.current, w64.optimized):+.1f}% improvement"
  echo &"  readU32:   {pctImprovement(r32.current, r32.optimized):+.1f}% improvement"
  echo &"  readU64:   {pctImprovement(r64.current, r64.optimized):+.1f}% improvement"
  echo &"  Message:   {pctImprovement(msg.current, msg.optimized):+.1f}% improvement"

  let avgImprovement = (
    pctImprovement(w32.current, w32.optimized) +
    pctImprovement(w64.current, w64.optimized) +
    pctImprovement(r32.current, r32.optimized) +
    pctImprovement(r64.current, r64.optimized)
  ) / 4.0

  echo &"\n  Average improvement for fixed-size types: {avgImprovement:+.1f}%"

  echo "\n=========================================="
  echo "Recommendation"
  echo "=========================================="
  if avgImprovement > 20.0:
    echo "  -> OPTIMIZATION RECOMMENDED: Significant performance gain"
  elif avgImprovement > 10.0:
    echo "  -> OPTIMIZATION OPTIONAL: Moderate performance gain"
  elif avgImprovement > 5.0:
    echo "  -> OPTIMIZATION LOW PRIORITY: Small performance gain"
  else:
    echo "  -> OPTIMIZATION NOT RECOMMENDED: Negligible performance gain"
