# Frame encoding/decoding for the Fractio wire protocol.
#
# Every message on the wire is wrapped in a 12-byte header:
#
#   +--------------------------------------------------------+
#   | Payload Length  (4 bytes uint32 BE)                    |
#   | Request ID      (4 bytes uint32 BE)                    |
#   | Flags           (2 bytes uint16 BE)                    |
#   | Checksum CRC16  (2 bytes uint16 BE)                    |
#   +--------------------------------------------------------+
#   | Message Type    (2 bytes uint16 BE)   \                |
#   | Message Data    (variable)            / payload        |
#   +--------------------------------------------------------+
#
# The checksum covers the raw payload bytes (after the header).
# When the Compressed flag is set, payload bytes are Snappy-compressed and
# the checksum is computed on the *uncompressed* payload.

import std/strformat
import ./types
import ./codec

# ---------------------------------------------------------------------------
# CRC-16/CCITT-FALSE  (poly 0x1021, init 0xFFFF, no reflection)
# ---------------------------------------------------------------------------

proc computeCRC16*(data: string): uint16 =
  result = 0xFFFF'u16
  for ch in data:
    result = result xor (uint16(ch) shl 8)
    for _ in 0 ..< 8:
      if (result and 0x8000'u16) != 0:
        result = (result shl 1) xor 0x1021'u16
      else:
        result = result shl 1

# ---------------------------------------------------------------------------
# Frame constants
# ---------------------------------------------------------------------------

const
  FRAME_HEADER_SIZE* = 12
  MAX_FRAME_SIZE* = 16 * 1024 * 1024 # 16 MB

# ---------------------------------------------------------------------------
# Frame types
# ---------------------------------------------------------------------------

type
  FrameHeader* = object
    payloadLen*: uint32
    requestId*: uint32
    flags*: uint16
    checksum*: uint16

  Frame* = object
    header*: FrameHeader
    payload*: string # raw (may be compressed); MessageType is first 2 bytes

# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------

proc encodeFrame*(payload: string, requestId: uint32,
    flags: uint16 = 0): string =
  ## Build a complete framed message (header + payload).
  ## Caller is responsible for compression before calling if FlagCompressed is set.
  var buf = ""
  buf.writeUint32BE(uint32(payload.len))
  buf.writeUint32BE(requestId)
  buf.writeUint16BE(flags)
  buf.writeUint16BE(computeCRC16(payload))
  buf.add(payload)
  buf

# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

proc decodeFrameHeader*(data: string,
    pos: var int): Result[FrameHeader, ProtocolError] =
  ## Decode only the 12-byte header from `data` starting at `pos`.
  let r = checkBounds(data, pos, FRAME_HEADER_SIZE)
  if r.isErr: return peErr(r.error)

  var hdr: FrameHeader
  let lenR = readUint32BE(data, pos)
  if lenR.isErr: return peErr(lenR.error)
  hdr.payloadLen = lenR.value

  let idR = readUint32BE(data, pos)
  if idR.isErr: return peErr(idR.error)
  hdr.requestId = idR.value

  let flagsR = readUint16BE(data, pos)
  if flagsR.isErr: return peErr(flagsR.error)
  hdr.flags = flagsR.value

  let crcR = readUint16BE(data, pos)
  if crcR.isErr: return peErr(crcR.error)
  hdr.checksum = crcR.value

  peOk(hdr)

proc decodeFrame*(data: string): Result[Frame, ProtocolError] =
  ## Decode a complete frame from `data`.  `data` must contain exactly one frame.
  if data.len < FRAME_HEADER_SIZE:
    return peErr(newProtocolError(peInvalidFrame,
      &"buffer too small: {data.len} < {FRAME_HEADER_SIZE}"))

  var pos = 0
  let hdrR = decodeFrameHeader(data, pos)
  if hdrR.isErr: return peErr(hdrR.error)
  let hdr = hdrR.value

  if hdr.payloadLen > MAX_FRAME_SIZE.uint32:
    return peErr(newProtocolError(peFrameTooLarge,
      &"payload length {hdr.payloadLen} exceeds maximum {MAX_FRAME_SIZE}"))

  let r = checkBounds(data, pos, int(hdr.payloadLen))
  if r.isErr: return peErr(r.error)

  let payload = data[pos ..< pos + int(hdr.payloadLen)]

  # Verify checksum against the raw payload bytes (before any decompression)
  let computed = computeCRC16(payload)
  if computed != hdr.checksum:
    return peErr(newProtocolError(peChecksumMismatch,
      &"CRC16 mismatch: got {hdr.checksum:#06x}, computed {computed:#06x}"))

  peOk(Frame(header: hdr, payload: payload))

# ---------------------------------------------------------------------------
# Convenience: build error frame payload
# ---------------------------------------------------------------------------

proc encodeErrorPayload*(errCode: uint32, category: uint8,
    msg: string, details: string = ""): string =
  var buf = ""
  buf.writeUint32BE(errCode)
  buf.writeUint8(category)
  buf.writeUint16BE(uint16(msg.len))
  buf.add(msg)
  buf.writeUint16BE(uint16(details.len))
  buf.add(details)
  buf

proc encodeErrorFrame*(requestId: uint32, errCode: uint32,
    category: uint8, msg: string, details: string = ""): string =
  var payload = ""
  # First 2 bytes of payload are MessageType — use 0x0000 for error frames
  payload.writeUint16BE(0x0000'u16)
  payload.add(encodeErrorPayload(errCode, category, msg, details))
  encodeFrame(payload, requestId, FlagIsResponse or FlagIsError)

# ---------------------------------------------------------------------------
# Error frame with leader redirect info (for ErrNotLeader)
# ---------------------------------------------------------------------------

proc encodeNotLeaderErrorFrame*(requestId: uint32, msg: string,
    redirect: LeaderRedirect): string =
  ## Encode an error frame with leader redirect info.
  ## Wire format (after MessageType prefix):
  ##   [errCode:4][category:1][msgLen:2][msg:N][detailsLen:2]
  ##   [leaderId:4][leaderHostLen:2][leaderHost:N][leaderClientPort:2]
  var payload = ""
  payload.writeUint16BE(0x0000'u16)
  payload.writeUint32BE(ErrNotLeader)
  payload.writeUint8(ErrCatSystem)
  payload.writeUint16BE(uint16(msg.len))
  payload.add(msg)
  # Details contains redirect info in binary format
  var details = ""
  details.writeUint32BE(redirect.leaderId)
  details.writeBytes16(redirect.leaderHost)
  details.writeUint16BE(redirect.leaderClientPort)
  payload.writeUint16BE(uint16(details.len))
  payload.add(details)
  encodeFrame(payload, requestId, FlagIsResponse or FlagIsError)

proc decodeLeaderRedirect*(details: string): LeaderRedirect =
  ## Decode leader redirect info from error frame details.
  ## Returns empty LeaderRedirect if details is malformed.
  if details.len < 8: # 4 (leaderId) + 2 (hostLen) + 2 (port) minimum
    return LeaderRedirect(leaderId: 0)
  var pos = 0
  let idR = readUint32BE(details, pos)
  if idR.isErr: return LeaderRedirect(leaderId: 0)
  result.leaderId = idR.value

  let hostR = readBytes16(details, pos)
  if hostR.isErr: return LeaderRedirect(leaderId: 0)
  result.leaderHost = hostR.value

  if pos + 2 > details.len: return LeaderRedirect(leaderId: 0)
  let portR = readUint16BE(details, pos)
  if portR.isErr: return LeaderRedirect(leaderId: 0)
  result.leaderClientPort = portR.value
