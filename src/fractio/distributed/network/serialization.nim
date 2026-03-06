# Network Serialization - Binary encoding/decoding for network messages
# Uses big-endian byte order for network compatibility

import std/endians
import types
import ../../core/types

# =============================================================================
# CRC32 Implementation
# =============================================================================

# CRC32 lookup table (IEEE 802.3 polynomial: 0xEDB88320)
const CRC32_TABLE: array[256, uint32] = [
  0x00000000'u32, 0x77073096'u32, 0xee0e612c'u32, 0x990951ba'u32,
  0x076dc419'u32, 0x706af48f'u32, 0xe963a535'u32, 0x9e6495a3'u32,
  0x0edb8832'u32, 0x79dcb8a4'u32, 0xe0d5e91e'u32, 0x97d2d988'u32,
  0x09b64c2b'u32, 0x7eb17cbd'u32, 0xe7b82d07'u32, 0x90bf1d91'u32,
  0x1db71064'u32, 0x6ab020f2'u32, 0xf3b97148'u32, 0x84be41de'u32,
  0x1adad47d'u32, 0x6ddde4eb'u32, 0xf4d4b551'u32, 0x83d385c7'u32,
  0x136c9856'u32, 0x646ba8c0'u32, 0xfd62f97a'u32, 0x8a65c9ec'u32,
  0x14015c4f'u32, 0x63066cd9'u32, 0xfa0f3d63'u32, 0x8d080df5'u32,
  0x3b6e20c8'u32, 0x4c69105e'u32, 0xd56041e4'u32, 0xa2677172'u32,
  0x3c03e4d1'u32, 0x4b04d447'u32, 0xd20d85fd'u32, 0xa50ab56b'u32,
  0x35b5a8fa'u32, 0x42b2986c'u32, 0xdbbbc9d6'u32, 0xacbcf940'u32,
  0x32d86ce3'u32, 0x45df5c75'u32, 0xdcd60dcf'u32, 0xabd13d59'u32,
  0x26d930ac'u32, 0x51de003a'u32, 0xc8d75180'u32, 0xbfd06116'u32,
  0x21b4f4b5'u32, 0x56b3c423'u32, 0xcfba9599'u32, 0xb8bda50f'u32,
  0x2802b89e'u32, 0x5f058808'u32, 0xc60cd9b2'u32, 0xb10be924'u32,
  0x2f6f7c87'u32, 0x58684c11'u32, 0xc1611dab'u32, 0xb6662d3d'u32,
  0x76dc4190'u32, 0x01db7106'u32, 0x98d220bc'u32, 0xefd5102a'u32,
  0x71b18589'u32, 0x06b6b51f'u32, 0x9fbfe4a5'u32, 0xe8b8d433'u32,
  0x7807c9a2'u32, 0x0f00f934'u32, 0x9609a88e'u32, 0xe10e9818'u32,
  0x7f6a0dbb'u32, 0x086d3d2d'u32, 0x91646c97'u32, 0xe6635c01'u32,
  0x6b6b51f4'u32, 0x1c6c6162'u32, 0x856530d8'u32, 0xf262004e'u32,
  0x6c0695ed'u32, 0x1b01a57b'u32, 0x8208f4c1'u32, 0xf50fc457'u32,
  0x65b0d9c6'u32, 0x12b7e950'u32, 0x8bbeb8ea'u32, 0xfcb9887c'u32,
  0x62dd1ddf'u32, 0x15da2d49'u32, 0x8cd37cf3'u32, 0xfbd44c65'u32,
  0x4db26158'u32, 0x3ab551ce'u32, 0xa3bc0074'u32, 0xd4bb30e2'u32,
  0x4adfa541'u32, 0x3dd895d7'u32, 0xa4d1c46d'u32, 0xd3d6f4fb'u32,
  0x4369e96a'u32, 0x346ed9fc'u32, 0xad678846'u32, 0xda60b8d0'u32,
  0x44042d73'u32, 0x33031de5'u32, 0xaa0a4c5f'u32, 0xdd0d7cc9'u32,
  0x5005713c'u32, 0x270241aa'u32, 0xbe0b1010'u32, 0xc90c2086'u32,
  0x5768b525'u32, 0x206f85b3'u32, 0xb966d409'u32, 0xce61e49f'u32,
  0x5edef90e'u32, 0x29d9c998'u32, 0xb0d09822'u32, 0xc7d7a8b4'u32,
  0x59b33d17'u32, 0x2eb40d81'u32, 0xb7bd5c3b'u32, 0xc0ba6cad'u32,
  0xedb88320'u32, 0x9abfb3b6'u32, 0x03b6e20c'u32, 0x74b1d29a'u32,
  0xead54739'u32, 0x9dd277af'u32, 0x04db2615'u32, 0x73dc1683'u32,
  0xe3630b12'u32, 0x94643b84'u32, 0x0d6d6a3e'u32, 0x7a6a5aa8'u32,
  0xe40ecf0b'u32, 0x9309ff9d'u32, 0x0a00ae27'u32, 0x7d079eb1'u32,
  0xf00f9344'u32, 0x8708a3d2'u32, 0x1e01f268'u32, 0x6906c2fe'u32,
  0xf762575d'u32, 0x806567cb'u32, 0x196c3671'u32, 0x6e6b06e7'u32,
  0xfed41b76'u32, 0x89d32be0'u32, 0x10da7a5a'u32, 0x67dd4acc'u32,
  0xf9b9df6f'u32, 0x8ebeeff9'u32, 0x17b7be43'u32, 0x60b08ed5'u32,
  0xd6d6a3e8'u32, 0xa1d1937e'u32, 0x38d8c2c4'u32, 0x4fdff252'u32,
  0xd1bb67f1'u32, 0xa6bc5767'u32, 0x3fb506dd'u32, 0x48b2364b'u32,
  0xd80d2bda'u32, 0xaf0a1b4c'u32, 0x36034af6'u32, 0x41047a60'u32,
  0xdf60efc3'u32, 0xa867df55'u32, 0x316e8eef'u32, 0x4669be79'u32,
  0xcb61b38c'u32, 0xbc66831a'u32, 0x256fd2a0'u32, 0x5268e236'u32,
  0xcc0c7795'u32, 0xbb0b4703'u32, 0x220216b9'u32, 0x5505262f'u32,
  0xc5ba3bbe'u32, 0xb2bd0b28'u32, 0x2bb45a92'u32, 0x5cb36a04'u32,
  0xc2d7ffa7'u32, 0xb5d0cf31'u32, 0x2cd99e8b'u32, 0x5bdeae1d'u32,
  0x9b64c2b0'u32, 0xec63f226'u32, 0x756aa39c'u32, 0x026d930a'u32,
  0x9c0906a9'u32, 0xeb0e363f'u32, 0x72076785'u32, 0x05005713'u32,
  0x95bf4a82'u32, 0xe2b87a14'u32, 0x7bb12bae'u32, 0x0cb61b38'u32,
  0x92d28e9b'u32, 0xe5d5be0d'u32, 0x7cdcefb7'u32, 0x0bdbdf21'u32,
  0x86d3d2d4'u32, 0xf1d4e242'u32, 0x68ddb3f8'u32, 0x1fda836e'u32,
  0x81be16cd'u32, 0xf6b9265b'u32, 0x6fb077e1'u32, 0x18b74777'u32,
  0x88085ae6'u32, 0xff0f6a70'u32, 0x66063bca'u32, 0x11010b5c'u32,
  0x8f659eff'u32, 0xf862ae69'u32, 0x616bffd3'u32, 0x166ccf45'u32,
  0xa00ae278'u32, 0xd70dd2ee'u32, 0x4e048354'u32, 0x3903b3c2'u32,
  0xa7672661'u32, 0xd06016f7'u32, 0x4969474d'u32, 0x3e6e77db'u32,
  0xaed16a4a'u32, 0xd9d65adc'u32, 0x40df0b66'u32, 0x37d83bf0'u32,
  0xa9bcae53'u32, 0xdebb9ec5'u32, 0x47b2cf7f'u32, 0x30b5ffe9'u32,
  0xbdbdf21c'u32, 0xcabac28a'u32, 0x53b39330'u32, 0x24b4a3a6'u32,
  0xbad03605'u32, 0xcdd70693'u32, 0x54de5729'u32, 0x23d967bf'u32,
  0xb3667a2e'u32, 0xc4614ab8'u32, 0x5d681b02'u32, 0x2a6f2b94'u32,
  0xb40bbe37'u32, 0xc30c8ea1'u32, 0x5a05df1b'u32, 0x2d02ef8d'u32
]

proc computeCRC32*(data: openArray[byte]): uint32 =
  result = 0xFFFFFFFF'u32
  for b in data:
    result = CRC32_TABLE[(result xor b.uint32).byte] xor (result shr 8)
  result = not result

proc computeCRC32*(data: string): uint32 =
  result = computeCRC32(data.toOpenArrayByte(0, data.len - 1))

# =============================================================================
# Binary Writing Helpers
# =============================================================================

type
  BinaryWriter* = object
    data*: seq[byte]
    pos*: int

  BinaryReader* = object
    data*: string
    pos*: int

  SerializationError* = object of CatchableError

proc newBinaryWriter*(initialSize: int = 1024): BinaryWriter =
  result.data = newSeq[byte](initialSize)
  result.pos = 0

proc ensureCapacity(w: var BinaryWriter, needed: int) =
  if w.pos + needed > w.data.len:
    var newSize = max(w.data.len * 2, w.pos + needed)
    var newData = newSeq[byte](newSize)
    if w.pos > 0:
      copyMem(newData[0].addr, w.data[0].addr, w.pos)
    w.data = newData

proc writeUint8*(w: var BinaryWriter, val: uint8) =
  w.ensureCapacity(1)
  w.data[w.pos] = val
  w.pos += 1

proc writeUint16BE*(w: var BinaryWriter, val: uint16) =
  w.ensureCapacity(2)
  var v = val
  bigEndian16(w.data[w.pos].addr, v.addr)
  w.pos += 2

proc writeUint32BE*(w: var BinaryWriter, val: uint32) =
  w.ensureCapacity(4)
  var v = val
  bigEndian32(w.data[w.pos].addr, v.addr)
  w.pos += 4

proc writeUint64BE*(w: var BinaryWriter, val: uint64) =
  w.ensureCapacity(8)
  var v = val
  bigEndian64(w.data[w.pos].addr, v.addr)
  w.pos += 8

proc writeBool*(w: var BinaryWriter, val: bool) =
  w.writeUint8(if val: 1 else: 0)

proc writeString*(w: var BinaryWriter, val: string) =
  let len = val.len.uint32
  w.writeUint32BE(len)
  if len > 0:
    w.ensureCapacity(len.int)
    copyMem(w.data[w.pos].addr, val[0].unsafeAddr, len.int)
    w.pos += len.int

proc writeNodeID*(w: var BinaryWriter, val: NodeID) =
  w.writeString(string(val))

proc getBytes*(w: BinaryWriter): seq[byte] =
  result = newSeq[byte](w.pos)
  if w.pos > 0:
    copyMem(result[0].addr, w.data[0].addr, w.pos)

proc getString*(w: BinaryWriter): string =
  result = newString(w.pos)
  if w.pos > 0:
    copyMem(result[0].addr, w.data[0].addr, w.pos)

# =============================================================================
# Binary Reading Helpers
# =============================================================================

proc newBinaryReader*(data: string): BinaryReader =
  result.data = data
  result.pos = 0

proc remaining*(r: BinaryReader): int =
  result = r.data.len - r.pos

proc readUint8*(r: var BinaryReader): uint8 =
  if r.pos >= r.data.len:
    raise newException(SerializationError, "Not enough data to read uint8")
  result = r.data[r.pos].uint8
  r.pos += 1

proc readUint16BE*(r: var BinaryReader): uint16 =
  if r.pos + 2 > r.data.len:
    raise newException(SerializationError, "Not enough data to read uint16")
  bigEndian16(result.addr, r.data[r.pos].unsafeAddr)
  r.pos += 2

proc readUint32BE*(r: var BinaryReader): uint32 =
  if r.pos + 4 > r.data.len:
    raise newException(SerializationError, "Not enough data to read uint32")
  bigEndian32(result.addr, r.data[r.pos].unsafeAddr)
  r.pos += 4

proc readUint64BE*(r: var BinaryReader): uint64 =
  if r.pos + 8 > r.data.len:
    raise newException(SerializationError, "Not enough data to read uint64")
  bigEndian64(result.addr, r.data[r.pos].unsafeAddr)
  r.pos += 8

proc readBool*(r: var BinaryReader): bool =
  result = r.readUint8() != 0

proc readString*(r: var BinaryReader): string =
  let len = r.readUint32BE().int
  if r.pos + len > r.data.len:
    raise newException(SerializationError,
        "Not enough data to read string of length " & $len)
  if len == 0:
    result = ""
  else:
    result = newString(len)
    copyMem(result[0].addr, r.data[r.pos].unsafeAddr, len)
    r.pos += len

proc readNodeID*(r: var BinaryReader): NodeID =
  result = NodeID(r.readString())

# =============================================================================
# Frame Encoding/Decoding
# =============================================================================

proc encodeFrame*(payload: string): string =
  let checksum = computeCRC32(payload)
  var w = newBinaryWriter(FRAME_HEADER_SIZE + payload.len)
  w.writeUint32BE(payload.len.uint32)
  w.writeUint32BE(checksum)
  if payload.len > 0:
    w.ensureCapacity(payload.len)
    copyMem(w.data[w.pos].addr, payload[0].unsafeAddr, payload.len)
    w.pos += payload.len
  result = w.getString()

proc decodeFrameHeader*(data: string): tuple[header: FrameHeader,
    payloadStart: int] =
  if data.len < FRAME_HEADER_SIZE:
    raise newException(SerializationError, "Data too short for frame header")
  var r = newBinaryReader(data)
  result.header.payloadLen = r.readUint32BE()
  result.header.checksum = r.readUint32BE()
  result.payloadStart = r.pos

proc decodeFrame*(data: string): Frame =
  let (header, payloadStart) = decodeFrameHeader(data)
  if data.len < payloadStart + header.payloadLen.int:
    raise newException(SerializationError, "Data too short for payload")
  result.header = header
  let payloadLen = header.payloadLen.int
  result.payload = newString(payloadLen)
  if payloadLen > 0:
    copyMem(result.payload[0].addr, data[payloadStart].unsafeAddr, payloadLen)
  let computedChecksum = computeCRC32(result.payload)
  if computedChecksum != header.checksum:
    raise newException(SerializationError, "Checksum mismatch: expected " &
      $header.checksum & " got " & $computedChecksum)

proc verifyFrameChecksum*(data: string): bool =
  try:
    discard decodeFrame(data)
    result = true
  except SerializationError:
    result = false

# =============================================================================
# Message Header Encoding/Decoding
# =============================================================================

proc encodeHeader*(header: MessageHeader): string =
  var w = newBinaryWriter(MESSAGE_HEADER_SIZE)
  w.writeUint16BE(header.messageType)
  w.writeUint64BE(header.messageId)
  w.writeNodeID(header.sourceNodeId)
  w.writeNodeID(header.targetNodeId)
  w.writeUint64BE(header.term)
  w.writeUint64BE(header.timestamp)
  result = w.getString()

proc decodeHeader*(data: string): MessageHeader =
  if data.len < MESSAGE_HEADER_SIZE:
    raise newException(SerializationError, "Data too short for message header")
  var r = newBinaryReader(data)
  result.messageType = r.readUint16BE()
  result.messageId = r.readUint64BE()
  result.sourceNodeId = r.readNodeID()
  result.targetNodeId = r.readNodeID()
  result.term = r.readUint64BE()
  result.timestamp = r.readUint64BE()

# =============================================================================
# KVRequest/KVResponse Encoding/Decoding
# =============================================================================

proc encodeKVRequest*(req: KVRequest): string =
  var w = newBinaryWriter(64)
  w.writeUint8(req.kind.uint8)
  case req.kind
  of rkGet:
    w.writeString(req.getKey)
    w.writeUint64BE(req.getTimestamp)
  of rkPut:
    w.writeString(req.putKey)
    w.writeString(req.putValue)
  of rkDelete:
    w.writeString(req.deleteKey)
  of rkScan:
    w.writeString(req.scanStartKey)
    w.writeString(req.scanEndKey)
    w.writeUint32BE(req.scanLimit)
    w.writeUint64BE(req.scanTimestamp)
  result = w.getString()

proc decodeKVRequest*(data: string): KVRequest =
  var r = newBinaryReader(data)
  let kindByte = r.readUint8()
  let kind = RequestKind(kindByte)
  case kind
  of rkGet:
    result = KVRequest(kind: rkGet)
    result.getKey = r.readString()
    result.getTimestamp = r.readUint64BE()
  of rkPut:
    result = KVRequest(kind: rkPut)
    result.putKey = r.readString()
    result.putValue = r.readString()
  of rkDelete:
    result = KVRequest(kind: rkDelete)
    result.deleteKey = r.readString()
  of rkScan:
    result = KVRequest(kind: rkScan)
    result.scanStartKey = r.readString()
    result.scanEndKey = r.readString()
    result.scanLimit = r.readUint32BE()
    result.scanTimestamp = r.readUint64BE()

proc encodeKVResponse*(resp: KVResponse): string =
  var w = newBinaryWriter(128)
  w.writeBool(resp.success)
  w.writeString(resp.errorMessage)
  w.writeUint8(resp.kind.uint8)
  case resp.kind
  of rkGet:
    w.writeString(resp.getValue)
    w.writeUint64BE(resp.getValueTimestamp)
    w.writeBool(resp.getFound)
  of rkPut:
    w.writeUint64BE(resp.putCommitTimestamp)
  of rkDelete:
    w.writeUint64BE(resp.deleteCommitTimestamp)
  of rkScan:
    w.writeUint32BE(resp.scanKeyValues.len.uint32)
    for (key, value) in resp.scanKeyValues:
      w.writeString(key)
      w.writeString(value)
    w.writeBool(resp.scanHasMore)
  result = w.getString()

proc decodeKVResponse*(data: string): KVResponse =
  var r = newBinaryReader(data)
  let success = r.readBool()
  let errorMessage = r.readString()
  let kindByte = r.readUint8()
  let kind = RequestKind(kindByte)
  case kind
  of rkGet:
    result = KVResponse(kind: rkGet)
    result.getValue = r.readString()
    result.getValueTimestamp = r.readUint64BE()
    result.getFound = r.readBool()
  of rkPut:
    result = KVResponse(kind: rkPut)
    result.putCommitTimestamp = r.readUint64BE()
  of rkDelete:
    result = KVResponse(kind: rkDelete)
    result.deleteCommitTimestamp = r.readUint64BE()
  of rkScan:
    result = KVResponse(kind: rkScan)
    let numPairs = r.readUint32BE().int
    result.scanKeyValues = @[]
    for i in 0 ..< numPairs:
      let key = r.readString()
      let value = r.readString()
      result.scanKeyValues.add((key, value))
    result.scanHasMore = r.readBool()
  result.success = success
  result.errorMessage = errorMessage

# =============================================================================
# Raft Message Encoding/Decoding
# =============================================================================

proc encodeRequestVoteMsg*(msg: RequestVoteMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeNodeID(msg.candidateId)
  w.writeUint64BE(msg.lastLogIndex)
  w.writeUint64BE(msg.lastLogTerm)
  result = w.getString()

proc decodeRequestVoteMsg*(data: string): RequestVoteMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.candidateId = r.readNodeID()
  result.lastLogIndex = r.readUint64BE()
  result.lastLogTerm = r.readUint64BE()

proc encodeRequestVoteResponseMsg*(msg: RequestVoteResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeBool(msg.voteGranted)
  w.writeUint64BE(msg.term)
  result = w.getString()

proc decodeRequestVoteResponseMsg*(data: string): RequestVoteResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.voteGranted = r.readBool()
  result.term = r.readUint64BE()

proc encodeAppendEntriesMsg*(msg: AppendEntriesMsg): string =
  var w = newBinaryWriter(256 + msg.entriesData.len)
  w.writeString(encodeHeader(msg.header))
  w.writeNodeID(msg.leaderId)
  w.writeUint64BE(msg.prevLogIndex)
  w.writeUint64BE(msg.prevLogTerm)
  w.writeUint64BE(msg.commitIndex)
  w.writeUint32BE(msg.numEntries)
  w.writeString(msg.entriesData)
  result = w.getString()

proc decodeAppendEntriesMsg*(data: string): AppendEntriesMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.leaderId = r.readNodeID()
  result.prevLogIndex = r.readUint64BE()
  result.prevLogTerm = r.readUint64BE()
  result.commitIndex = r.readUint64BE()
  result.numEntries = r.readUint32BE()
  result.entriesData = r.readString()

proc encodeAppendEntriesResponseMsg*(msg: AppendEntriesResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeBool(msg.success)
  w.writeUint64BE(msg.term)
  w.writeUint64BE(msg.matchIndex)
  w.writeUint64BE(msg.rejectHint)
  result = w.getString()

proc decodeAppendEntriesResponseMsg*(data: string): AppendEntriesResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.success = r.readBool()
  result.term = r.readUint64BE()
  result.matchIndex = r.readUint64BE()
  result.rejectHint = r.readUint64BE()

proc encodeInstallSnapshotMsg*(msg: InstallSnapshotMsg): string =
  var w = newBinaryWriter(128 + msg.data.len)
  w.writeString(encodeHeader(msg.header))
  w.writeNodeID(msg.leaderId)
  w.writeUint64BE(msg.lastIncludedIndex)
  w.writeUint64BE(msg.lastIncludedTerm)
  w.writeUint64BE(msg.offset)
  w.writeBool(msg.done)
  w.writeString(msg.data)
  result = w.getString()

proc decodeInstallSnapshotMsg*(data: string): InstallSnapshotMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.leaderId = r.readNodeID()
  result.lastIncludedIndex = r.readUint64BE()
  result.lastIncludedTerm = r.readUint64BE()
  result.offset = r.readUint64BE()
  result.done = r.readBool()
  result.data = r.readString()

proc encodeInstallSnapshotResponseMsg*(msg: InstallSnapshotResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.term)
  w.writeUint64BE(msg.offset)
  result = w.getString()

proc decodeInstallSnapshotResponseMsg*(data: string): InstallSnapshotResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.term = r.readUint64BE()
  result.offset = r.readUint64BE()

proc encodeTimeoutNowMsg*(msg: TimeoutNowMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  result = w.getString()

proc decodeTimeoutNowMsg*(data: string): TimeoutNowMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)

proc encodeReadIndexMsg*(msg: ReadIndexMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.readRequestId)
  result = w.getString()

proc decodeReadIndexMsg*(data: string): ReadIndexMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.readRequestId = r.readUint64BE()

proc encodeReadIndexResponseMsg*(msg: ReadIndexResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.readRequestId)
  w.writeUint64BE(msg.index)
  result = w.getString()

proc decodeReadIndexResponseMsg*(data: string): ReadIndexResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.readRequestId = r.readUint64BE()
  result.index = r.readUint64BE()

# =============================================================================
# Client Message Encoding/Decoding
# =============================================================================

proc encodeBatchRequestMsg*(msg: BatchRequestMsg): string =
  var w = newBinaryWriter(256)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.requestId)
  w.writeUint32BE(msg.rangeId)
  w.writeUint64BE(msg.transactionId)
  w.writeUint32BE(msg.requests.len.uint32)
  for req in msg.requests:
    w.writeString(encodeKVRequest(req))
  result = w.getString()

proc decodeBatchRequestMsg*(data: string): BatchRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.requestId = r.readUint64BE()
  result.rangeId = r.readUint32BE()
  result.transactionId = r.readUint64BE()
  let numRequests = r.readUint32BE().int
  result.requests = @[]
  for i in 0 ..< numRequests:
    let reqData = r.readString()
    result.requests.add(decodeKVRequest(reqData))

proc encodeBatchResponseMsg*(msg: BatchResponseMsg): string =
  var w = newBinaryWriter(256)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.requestId)
  w.writeBool(msg.success)
  w.writeString(msg.errorMessage)
  w.writeUint32BE(msg.responses.len.uint32)
  for resp in msg.responses:
    w.writeString(encodeKVResponse(resp))
  result = w.getString()

proc decodeBatchResponseMsg*(data: string): BatchResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.requestId = r.readUint64BE()
  result.success = r.readBool()
  result.errorMessage = r.readString()
  let numResponses = r.readUint32BE().int
  result.responses = @[]
  for i in 0 ..< numResponses:
    let respData = r.readString()
    result.responses.add(decodeKVResponse(respData))

proc encodeScanRequestMsg*(msg: ScanRequestMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.requestId)
  w.writeUint32BE(msg.rangeId)
  w.writeString(msg.startKey)
  w.writeString(msg.endKey)
  w.writeUint32BE(msg.limit)
  w.writeUint64BE(msg.timestamp)
  result = w.getString()

proc decodeScanRequestMsg*(data: string): ScanRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.requestId = r.readUint64BE()
  result.rangeId = r.readUint32BE()
  result.startKey = r.readString()
  result.endKey = r.readString()
  result.limit = r.readUint32BE()
  result.timestamp = r.readUint64BE()

proc encodeScanResponseMsg*(msg: ScanResponseMsg): string =
  var w = newBinaryWriter(256)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.requestId)
  w.writeBool(msg.success)
  w.writeString(msg.errorMessage)
  w.writeUint32BE(msg.keyValues.len.uint32)
  for (key, value) in msg.keyValues:
    w.writeString(key)
    w.writeString(value)
  w.writeBool(msg.hasMore)
  w.writeString(msg.continuationToken)
  result = w.getString()

proc decodeScanResponseMsg*(data: string): ScanResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.requestId = r.readUint64BE()
  result.success = r.readBool()
  result.errorMessage = r.readString()
  let numPairs = r.readUint32BE().int
  result.keyValues = @[]
  for i in 0 ..< numPairs:
    let key = r.readString()
    let value = r.readString()
    result.keyValues.add((key, value))
  result.hasMore = r.readBool()
  result.continuationToken = r.readString()

# =============================================================================
# 2PC Message Encoding/Decoding
# =============================================================================

proc encodeTxnPrepareMsg*(msg: TxnPrepareMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  w.writeNodeID(msg.coordinatorId)
  w.writeUint32BE(msg.participantIds.len.uint32)
  for pid in msg.participantIds:
    w.writeNodeID(pid)
  w.writeUint64BE(msg.timestamp)
  result = w.getString()

proc decodeTxnPrepareMsg*(data: string): TxnPrepareMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()
  result.coordinatorId = r.readNodeID()
  let numParticipants = r.readUint32BE().int
  result.participantIds = @[]
  for i in 0 ..< numParticipants:
    result.participantIds.add(r.readNodeID())
  result.timestamp = r.readUint64BE()

proc encodeTxnPrepareResponseMsg*(msg: TxnPrepareResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  w.writeBool(msg.vote)
  w.writeString(msg.errorMessage)
  result = w.getString()

proc decodeTxnPrepareResponseMsg*(data: string): TxnPrepareResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()
  result.vote = r.readBool()
  result.errorMessage = r.readString()

proc encodeTxnCommitMsg*(msg: TxnCommitMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  w.writeUint64BE(msg.commitTimestamp)
  result = w.getString()

proc decodeTxnCommitMsg*(data: string): TxnCommitMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()
  result.commitTimestamp = r.readUint64BE()

proc encodeTxnCommitResponseMsg*(msg: TxnCommitResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  w.writeBool(msg.success)
  w.writeString(msg.errorMessage)
  result = w.getString()

proc decodeTxnCommitResponseMsg*(data: string): TxnCommitResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()
  result.success = r.readBool()
  result.errorMessage = r.readString()

proc encodeTxnRollbackMsg*(msg: TxnRollbackMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  result = w.getString()

proc decodeTxnRollbackMsg*(data: string): TxnRollbackMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()

proc encodeTxnRollbackResponseMsg*(msg: TxnRollbackResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeUint64BE(msg.txnId)
  w.writeBool(msg.success)
  result = w.getString()

proc decodeTxnRollbackResponseMsg*(data: string): TxnRollbackResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.txnId = r.readUint64BE()
  result.success = r.readBool()

proc encodeHeartbeatMsg*(msg: HeartbeatMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeBool(msg.ping)
  result = w.getString()

proc decodeHeartbeatMsg*(data: string): HeartbeatMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.ping = r.readBool()

proc encodeHeartbeatResponseMsg*(msg: HeartbeatResponseMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeBool(msg.pong)
  result = w.getString()

proc decodeHeartbeatResponseMsg*(data: string): HeartbeatResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.pong = r.readBool()

proc encodeErrorMsg*(msg: ErrorMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeUint32BE(msg.errorCode)
  w.writeString(msg.errorMessage)
  result = w.getString()

proc decodeErrorMsg*(data: string): ErrorMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.errorCode = r.readUint32BE()
  result.errorMessage = r.readString()

# =============================================================================
# Admin Message Encoding/Decoding
# =============================================================================

proc encodeMetricsRequestMsg*(msg: MetricsRequestMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  result = w.getString()

proc decodeMetricsRequestMsg*(data: string): MetricsRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)

proc encodeMetricsResponseMsg*(msg: MetricsResponseMsg): string =
  var w = newBinaryWriter(256 + msg.metricsJson.len)
  w.writeString(encodeHeader(msg.header))
  w.writeString(msg.metricsJson)
  result = w.getString()

proc decodeMetricsResponseMsg*(data: string): MetricsResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.metricsJson = r.readString()

proc encodeHealthRequestMsg*(msg: HealthRequestMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  result = w.getString()

proc decodeHealthRequestMsg*(data: string): HealthRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)

proc encodeHealthResponseMsg*(msg: HealthResponseMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeBool(msg.healthy)
  w.writeString(msg.details)
  result = w.getString()

proc decodeHealthResponseMsg*(data: string): HealthResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.healthy = r.readBool()
  result.details = r.readString()

proc encodeConfigRequestMsg*(msg: ConfigRequestMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  result = w.getString()

proc decodeConfigRequestMsg*(data: string): ConfigRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)

proc encodeConfigResponseMsg*(msg: ConfigResponseMsg): string =
  var w = newBinaryWriter(256 + msg.configJson.len)
  w.writeString(encodeHeader(msg.header))
  w.writeString(msg.configJson)
  result = w.getString()

proc decodeConfigResponseMsg*(data: string): ConfigResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.configJson = r.readString()

proc encodeNodeInfoRequestMsg*(msg: NodeInfoRequestMsg): string =
  var w = newBinaryWriter(64)
  w.writeString(encodeHeader(msg.header))
  w.writeNodeID(msg.targetNodeId)
  result = w.getString()

proc decodeNodeInfoRequestMsg*(data: string): NodeInfoRequestMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.targetNodeId = r.readNodeID()

proc encodeNodeInfoResponseMsg*(msg: NodeInfoResponseMsg): string =
  var w = newBinaryWriter(128)
  w.writeString(encodeHeader(msg.header))
  w.writeNodeID(msg.nodeId)
  w.writeString(msg.raftAddr)
  w.writeString(msg.clientAddr)
  w.writeString(msg.adminAddr)
  w.writeBool(msg.isHealthy)
  w.writeUint64BE(msg.uptime)
  result = w.getString()

proc decodeNodeInfoResponseMsg*(data: string): NodeInfoResponseMsg =
  var r = newBinaryReader(data)
  let headerData = r.readString()
  result.header = decodeHeader(headerData)
  result.nodeId = r.readNodeID()
  result.raftAddr = r.readString()
  result.clientAddr = r.readString()
  result.adminAddr = r.readString()
  result.isHealthy = r.readBool()
  result.uptime = r.readUint64BE()

# =============================================================================
# Generic Message Encoding (wraps message in frame)
# =============================================================================

proc encodeMessage*(msgType: uint16, payload: string): string =
  result = encodeFrame(payload)

proc getPayload*(frame: Frame): string =
  result = frame.payload
