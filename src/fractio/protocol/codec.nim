# Low-level binary encoding/decoding primitives for the Fractio wire protocol.
# All integers are big-endian. Strings are length-prefixed with a uint32 BE length.
# All procs are gcsafe and raise no exceptions; errors are returned via Result/PResult.

import std/strformat
import ./types

# ---------------------------------------------------------------------------
# Write helpers — append to a var string buffer
# ---------------------------------------------------------------------------

proc writeUint8*(buf: var string, v: uint8) {.inline.} =
  buf.add(char(v))

proc writeUint16BE*(buf: var string, v: uint16) {.inline.} =
  buf.add(char((v shr 8) and 0xFF))
  buf.add(char(v and 0xFF))

proc writeUint32BE*(buf: var string, v: uint32) {.inline.} =
  buf.add(char((v shr 24) and 0xFF))
  buf.add(char((v shr 16) and 0xFF))
  buf.add(char((v shr 8) and 0xFF))
  buf.add(char(v and 0xFF))

proc writeUint64BE*(buf: var string, v: uint64) {.inline.} =
  buf.add(char((v shr 56) and 0xFF))
  buf.add(char((v shr 48) and 0xFF))
  buf.add(char((v shr 40) and 0xFF))
  buf.add(char((v shr 32) and 0xFF))
  buf.add(char((v shr 24) and 0xFF))
  buf.add(char((v shr 16) and 0xFF))
  buf.add(char((v shr 8) and 0xFF))
  buf.add(char(v and 0xFF))

proc writeBytes*(buf: var string, data: string) {.inline.} =
  ## Write a length-prefixed byte string (uint32 BE length + raw bytes).
  buf.writeUint32BE(uint32(data.len))
  buf.add(data)

proc writeBytes8*(buf: var string, data: string) {.inline.} =
  ## Write a length-prefixed byte string with uint8 length (max 255 bytes).
  buf.writeUint8(uint8(data.len))
  buf.add(data)

# ---------------------------------------------------------------------------
# Bounds check — returns PResult (void success / ProtocolError failure)
# ---------------------------------------------------------------------------

proc checkBounds*(buf: string, pos: int, need: int): PResult {.inline.} =
  if pos + need > buf.len:
    return pErr(newProtocolError(peBoundsOverflow,
      &"need {need} bytes at pos {pos} but buffer has {buf.len}"))
  pOk()

# ---------------------------------------------------------------------------
# Read helpers — advance pos, return Result[T, ProtocolError]
# Use peErr() template to propagate errors without T-inference issues.
# ---------------------------------------------------------------------------

proc readUint8*(buf: string, pos: var int): Result[uint8, ProtocolError] =
  let r = checkBounds(buf, pos, 1)
  if r.isErr: return peErr(r.error)
  result = peOk(uint8(buf[pos]))
  inc pos

proc readUint16BE*(buf: string, pos: var int): Result[uint16, ProtocolError] =
  let r = checkBounds(buf, pos, 2)
  if r.isErr: return peErr(r.error)
  let v = (uint16(buf[pos]) shl 8) or uint16(buf[pos + 1])
  pos += 2
  peOk(v)

proc readUint32BE*(buf: string, pos: var int): Result[uint32, ProtocolError] =
  let r = checkBounds(buf, pos, 4)
  if r.isErr: return peErr(r.error)
  let v = (uint32(buf[pos]) shl 24) or
          (uint32(buf[pos + 1]) shl 16) or
          (uint32(buf[pos + 2]) shl 8) or
           uint32(buf[pos + 3])
  pos += 4
  peOk(v)

proc readUint64BE*(buf: string, pos: var int): Result[uint64, ProtocolError] =
  let r = checkBounds(buf, pos, 8)
  if r.isErr: return peErr(r.error)
  let v = (uint64(buf[pos]) shl 56) or
          (uint64(buf[pos + 1]) shl 48) or
          (uint64(buf[pos + 2]) shl 40) or
          (uint64(buf[pos + 3]) shl 32) or
          (uint64(buf[pos + 4]) shl 24) or
          (uint64(buf[pos + 5]) shl 16) or
          (uint64(buf[pos + 6]) shl 8) or
           uint64(buf[pos + 7])
  pos += 8
  peOk(v)

proc readBytes*(buf: string, pos: var int): Result[string, ProtocolError] =
  ## Read a uint32-length-prefixed byte string.
  let lenR = readUint32BE(buf, pos)
  if lenR.isErr: return peErr(lenR.error)
  let length = int(lenR.value)
  let r = checkBounds(buf, pos, length)
  if r.isErr: return peErr(r.error)
  result = peOk(buf[pos ..< pos + length])
  pos += length

proc readBytes8*(buf: string, pos: var int): Result[string, ProtocolError] =
  ## Read a uint8-length-prefixed byte string.
  let lenR = readUint8(buf, pos)
  if lenR.isErr: return peErr(lenR.error)
  let length = int(lenR.value)
  let r = checkBounds(buf, pos, length)
  if r.isErr: return peErr(r.error)
  result = peOk(buf[pos ..< pos + length])
  pos += length


