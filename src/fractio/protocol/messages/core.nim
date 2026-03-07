# Core control message encoding/decoding: Ping, Echo, Close, CancelStream.
# Each proc encodes a complete payload (MessageType prefix + fields).
# The caller wraps the payload in a Frame via frame.encodeFrame.

import ../types
import ../codec

# ---------------------------------------------------------------------------
# Ping  (0x0001)
# Request:  (no payload beyond message type)
# Response: Timestamp (8 bytes uint64 BE) — server time in microseconds
# ---------------------------------------------------------------------------

proc encodePingRequest*(): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtPing))
  buf

proc encodePingResponse*(timestampUs: uint64): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtPing))
  buf.writeUint64BE(timestampUs)
  buf

proc decodePingResponse*(payload: string): Result[uint64, ProtocolError] =
  ## Payload already has the 2-byte MessageType prefix; skip it.
  var pos = 2
  readUint64BE(payload, pos)

# ---------------------------------------------------------------------------
# Echo  (0x0002)
# Request:  Data (uint32 length-prefixed)
# Response: Data (uint32 length-prefixed) — echoed back
# ---------------------------------------------------------------------------

proc encodeEchoRequest*(data: string): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtEcho))
  buf.writeBytes(data)
  buf

proc encodeEchoResponse*(data: string): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtEcho))
  buf.writeBytes(data)
  buf

proc decodeEchoData*(payload: string): Result[string, ProtocolError] =
  var pos = 2 # skip MessageType
  readBytes(payload, pos)

# ---------------------------------------------------------------------------
# Close  (0x0003)
# Request:  Reason (uint8 length-prefixed string, optional)
# Response: none — connection closed by server after sending ack frame
# ---------------------------------------------------------------------------

proc encodeCloseRequest*(reason: string = ""): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtClose))
  buf.writeBytes8(reason)
  buf

proc decodeCloseReason*(payload: string): Result[string, ProtocolError] =
  var pos = 2 # skip MessageType
  readBytes8(payload, pos)

# ---------------------------------------------------------------------------
# CancelStream  (0x0004)
# Request:  Request ID (4 bytes uint32 BE) — the streaming request to cancel
# Response: Status (1 byte): 0x00 = cancelled, 0x01 = already complete
# ---------------------------------------------------------------------------

proc encodeCancelStreamRequest*(targetRequestId: uint32): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCancelStream))
  buf.writeUint32BE(targetRequestId)
  buf

proc decodeCancelStreamRequest*(payload: string): Result[uint32,
    ProtocolError] =
  var pos = 2
  readUint32BE(payload, pos)

proc encodeCancelStreamResponse*(alreadyComplete: bool): string =
  var buf = ""
  buf.writeUint16BE(uint16(mtCancelStream))
  buf.writeUint8(if alreadyComplete: 0x01'u8 else: 0x00'u8)
  buf

proc decodeCancelStreamResponse*(payload: string): Result[bool, ProtocolError] =
  var pos = 2
  let r = readUint8(payload, pos)
  if r.isErr: return peErr(r.error)
  peOk(r.value == 0x01'u8)
