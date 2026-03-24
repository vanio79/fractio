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

proc writeBytes16*(buf: var string, data: string) {.inline.} =
  ## Write a length-prefixed byte string with uint16 length (max 65535 bytes).
  buf.writeUint16BE(uint16(data.len))
  buf.add(data)

proc writeBytes32*(buf: var string, data: string) {.inline.} =
  ## Write a length-prefixed byte string with uint32 length.
  buf.writeUint32BE(uint32(data.len))
  buf.add(data)

proc writeInt32BE*(buf: var string, v: int32) {.inline.} =
  ## Write a signed 32-bit integer in big-endian.
  buf.writeUint32BE(uint32(v))

proc writeInt64BE*(buf: var string, v: int64) {.inline.} =
  ## Write a signed 64-bit integer in big-endian.
  buf.writeUint64BE(uint64(v))

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

proc readBytes16*(buf: string, pos: var int): Result[string, ProtocolError] =
  ## Read a uint16-length-prefixed byte string.
  let lenR = readUint16BE(buf, pos)
  if lenR.isErr: return peErr(lenR.error)
  let length = int(lenR.value)
  let r = checkBounds(buf, pos, length)
  if r.isErr: return peErr(r.error)
  result = peOk(buf[pos ..< pos + length])
  pos += length

proc readBytes32*(buf: string, pos: var int): Result[string, ProtocolError] =
  ## Read a uint32-length-prefixed byte string.
  let lenR = readUint32BE(buf, pos)
  if lenR.isErr: return peErr(lenR.error)
  let length = int(lenR.value)
  let r = checkBounds(buf, pos, length)
  if r.isErr: return peErr(r.error)
  result = peOk(buf[pos ..< pos + length])
  pos += length

proc readInt32BE*(buf: string, pos: var int): Result[int32, ProtocolError] =
  ## Read a signed 32-bit integer in big-endian.
  let r = checkBounds(buf, pos, 4)
  if r.isErr: return peErr(r.error)
  let v = (int32(buf[pos]) shl 24) or
          (int32(buf[pos + 1]) shl 16) or
          (int32(buf[pos + 2]) shl 8) or
           int32(buf[pos + 3])
  pos += 4
  peOk(v)

proc readInt64BE*(buf: string, pos: var int): Result[int64, ProtocolError] =
  ## Read a signed 64-bit integer in big-endian.
  let r = checkBounds(buf, pos, 8)
  if r.isErr: return peErr(r.error)
  let v = (int64(buf[pos]) shl 56) or
          (int64(buf[pos + 1]) shl 48) or
          (int64(buf[pos + 2]) shl 40) or
          (int64(buf[pos + 3]) shl 32) or
          (int64(buf[pos + 4]) shl 24) or
          (int64(buf[pos + 5]) shl 16) or
          (int64(buf[pos + 6]) shl 8) or
           int64(buf[pos + 7])
  pos += 8
  peOk(v)


