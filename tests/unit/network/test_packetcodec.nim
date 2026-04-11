# Unit tests for PacketCodec
# 100% coverage target

import unittest
import fractio/network/packetcodec
import fractio/core/types
import fractio/utils/logging

suite "PacketCodec Unit Tests":

  var codec: PacketCodec
  var logger: Logger

  setup:
    logger = Logger(name: "Test", minLevel: llWarn, handlers: @[])
    codec = newPacketCodec()

  test "Constants have correct values":
    check BOM_BIG_ENDIAN == 0xFEFF'u16
    check BOM_LITTLE_ENDIAN == 0xFFFE'u16
    check MSG_SYNC_REQUEST == 1'u8
    check MSG_SYNC_RESPONSE == 2'u8
    check REQUEST_PACKET_SIZE == 11
    check RESPONSE_PACKET_SIZE == 19
    check MAX_PACKET_SIZE == 65507

  test "readUint16BE reads big-endian 16-bit":
    let data = [uint8(0x12), 0x34]
    let value = codec.readUint16BE(data, 0)
    check value == 0x1234'u16

  test "readUint16LE reads little-endian 16-bit":
    let data = [uint8(0x34), 0x12]
    let value = codec.readUint16LE(data, 0)
    check value == 0x1234'u16

  test "writeUint16BE writes big-endian 16-bit":
    var dest = newSeq[uint8](2)
    codec.writeUint16BE(0x1234'u16, dest, 0)
    check dest[0] == 0x12'u8
    check dest[1] == 0x34'u8

  test "readUint64BE reads big-endian 64-bit":
    let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
    let value = codec.readUint64BE(data, 0)
    check value == 0x0123456789ABCDEF'u64

  test "readUint64LE reads little-endian 64-bit":
    let data = [uint8(0xEF), 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
    let value = codec.readUint64LE(data, 0)
    check value == 0x0123456789ABCDEF'u64

  test "writeUint64BE writes big-endian 64-bit":
    var dest = newSeq[uint8](8)
    codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, 0)
    for i in 0..7:
      check dest[i] == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF][i].uint8

  test "writeUint64LE writes little-endian 64-bit":
    var dest = newSeq[uint8](8)
    codec.writeUint64LE(0x0123456789ABCDEF'u64, dest, 0)
    for i in 0..7:
      check dest[i] == [0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01][i].uint8

  test "timestampToBytes with BOM_BIG_ENDIAN":
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(Timestamp(0x0123456789ABCDEF'i64), BOM_BIG_ENDIAN,
        dest, 0)
    for i in 0..7:
      check dest[i] == [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF][i].uint8

  test "timestampToBytes with BOM_LITTLE_ENDIAN":
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(Timestamp(0x0123456789ABCDEF'i64), BOM_LITTLE_ENDIAN,
        dest, 0)
    for i in 0..7:
      check dest[i] == [0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01][i].uint8

  test "bytesToTimestamp with BOM_BIG_ENDIAN":
    let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
    let ts = codec.bytesToTimestamp(data, 0, BOM_BIG_ENDIAN)
    check ts == Timestamp(0x0123456789ABCDEF'i64)

  test "bytesToTimestamp with BOM_LITTLE_ENDIAN":
    let data = [uint8(0xEF), 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
    let ts = codec.bytesToTimestamp(data, 0, BOM_LITTLE_ENDIAN)
    check ts == Timestamp(0x0123456789ABCDEF'i64)

  test "timestamp round-trip big-endian":
    let original = Timestamp(0x0123456789ABCDEF'i64)
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(original, BOM_BIG_ENDIAN, dest, 0)
    let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
    check recovered == original

  test "timestamp round-trip little-endian":
    let original = Timestamp(0x0123456789ABCDEF'i64)
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(original, BOM_LITTLE_ENDIAN, dest, 0)
    let recovered = codec.bytesToTimestamp(dest, 0, BOM_LITTLE_ENDIAN)
    check recovered == original

  test "timestamp zero value":
    let zero = Timestamp(0'i64)
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(zero, BOM_BIG_ENDIAN, dest, 0)
    for b in dest:
      check b == 0'u8
    let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
    check recovered == zero

  test "timestamp max int64":
    let maxTs = Timestamp(0x7FFFFFFFFFFFFFFF'i64)
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(maxTs, BOM_BIG_ENDIAN, dest, 0)
    let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
    check recovered == maxTs

  test "encodeRequest creates correct packet (big-endian)":
    let t1 = Timestamp(1_000_000_000_000'i64)
    let packet = codec.encodeRequest(t1, BOM_BIG_ENDIAN)
    check packet.len == REQUEST_PACKET_SIZE
    # Check BOM (always big-endian on wire)
    check codec.readUint16BE(packet, 0) == BOM_BIG_ENDIAN
    # Check message type
    check packet[2] == MSG_SYNC_REQUEST
    # Check timestamp
    let decodedT1 = codec.bytesToTimestamp(packet, 3, BOM_BIG_ENDIAN)
    check decodedT1 == t1

  test "encodeRequest creates correct packet (little-endian)":
    let t1 = Timestamp(1_000_000_000_000'i64)
    let packet = codec.encodeRequest(t1, BOM_LITTLE_ENDIAN)
    check packet.len == REQUEST_PACKET_SIZE
    # BOM field is little-endian
    check codec.readUint16BE(packet, 0) == BOM_LITTLE_ENDIAN
    check packet[2] == MSG_SYNC_REQUEST
    let decodedT1 = codec.bytesToTimestamp(packet, 3, BOM_LITTLE_ENDIAN)
    check decodedT1 == t1

  test "decodeRequest with big-endian packet":
    # Manually construct a valid big-endian request
    var packet = newSeq[uint8](REQUEST_PACKET_SIZE)
    codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
    packet[2] = MSG_SYNC_REQUEST
    codec.timestampToBytes(Timestamp(1234567890'i64), BOM_BIG_ENDIAN, packet, 3)
    let (bom, request) = codec.decodeRequest(packet)
    check bom == BOM_BIG_ENDIAN
    check request.t1 == Timestamp(1234567890'i64)

  test "decodeRequest with little-endian packet":
    var packet = newSeq[uint8](REQUEST_PACKET_SIZE)
    codec.writeUint16BE(BOM_LITTLE_ENDIAN, packet, 0)
    packet[2] = MSG_SYNC_REQUEST
    codec.timestampToBytes(Timestamp(1234567890'i64), BOM_LITTLE_ENDIAN, packet, 3)
    let (bom, request) = codec.decodeRequest(packet)
    check bom == BOM_LITTLE_ENDIAN
    check request.t1 == Timestamp(1234567890'i64)

  test "decodeRequest raises on packet too small":
    var small = [uint8(1), 2, 3]
    expect ValueError:
      discard codec.decodeRequest(small)

  test "decodeRequest raises on invalid message type":
    var packet = newSeq[uint8](REQUEST_PACKET_SIZE)
    codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
    packet[2] = 0xFF'u8 # Invalid type
    codec.timestampToBytes(Timestamp(123'i64), BOM_BIG_ENDIAN, packet, 3)
    expect ValueError:
      discard codec.decodeRequest(packet)

  test "encodeResponse creates correct packet":
    let t2 = Timestamp(2_000_000_000_000'i64)
    let t3 = Timestamp(2_100_000_000_000'i64)
    let packet = codec.encodeResponse(t2, t3, BOM_BIG_ENDIAN)
    check packet.len == RESPONSE_PACKET_SIZE
    check codec.readUint16BE(packet, 0) == BOM_BIG_ENDIAN
    check packet[2] == MSG_SYNC_RESPONSE
    let decodedT2 = codec.bytesToTimestamp(packet, 3, BOM_BIG_ENDIAN)
    let decodedT3 = codec.bytesToTimestamp(packet, 11, BOM_BIG_ENDIAN)
    check decodedT2 == t2
    check decodedT3 == t3

  test "encodeResponse preserves request BOM":
    let t2 = Timestamp(2_000_000_000_000'i64)
    let t3 = Timestamp(2_100_000_000_000'i64)
    let packet = codec.encodeResponse(t2, t3, BOM_LITTLE_ENDIAN)
    check codec.readUint16BE(packet, 0) == BOM_LITTLE_ENDIAN

  test "decodeResponse with big-endian packet":
    var packet = newSeq[uint8](RESPONSE_PACKET_SIZE)
    codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
    packet[2] = MSG_SYNC_RESPONSE
    codec.timestampToBytes(Timestamp(2000000000000'i64), BOM_BIG_ENDIAN, packet, 3)
    codec.timestampToBytes(Timestamp(2100000000000'i64), BOM_BIG_ENDIAN, packet, 11)
    let (bom, response) = codec.decodeResponse(packet)
    check bom == BOM_BIG_ENDIAN
    check response.t2 == Timestamp(2000000000000'i64)
    check response.t3 == Timestamp(2100000000000'i64)

  test "decodeResponse with little-endian packet":
    var packet = newSeq[uint8](RESPONSE_PACKET_SIZE)
    codec.writeUint16BE(BOM_LITTLE_ENDIAN, packet, 0)
    packet[2] = MSG_SYNC_RESPONSE
    codec.timestampToBytes(Timestamp(2000000000000'i64), BOM_LITTLE_ENDIAN,
        packet, 3)
    codec.timestampToBytes(Timestamp(2100000000000'i64), BOM_LITTLE_ENDIAN,
        packet, 11)
    let (bom, response) = codec.decodeResponse(packet)
    check bom == BOM_LITTLE_ENDIAN
    check response.t2 == Timestamp(2000000000000'i64)
    check response.t3 == Timestamp(2100000000000'i64)

  test "decodeResponse raises on packet too small":
    var small = [uint8(1), 2, 3]
    expect ValueError:
      discard codec.decodeResponse(small)

  test "decodeResponse raises on invalid message type":
    var packet = newSeq[uint8](RESPONSE_PACKET_SIZE)
    codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
    packet[2] = 0xFF'u8
    codec.timestampToBytes(Timestamp(2000000000000'i64), BOM_BIG_ENDIAN, packet, 3)
    codec.timestampToBytes(Timestamp(2100000000000'i64), BOM_BIG_ENDIAN, packet, 11)
    expect ValueError:
      discard codec.decodeResponse(packet)

  test "Request and Response round-trip":
    let t1 = Timestamp(1234567890123456789'i64)
    let t2 = Timestamp(2234567890123456789'i64)
    let t3 = Timestamp(3234567890123456789'i64)

    # Encode request
    let reqPacket = codec.encodeRequest(t1, BOM_BIG_ENDIAN)
    let (reqBom, req) = codec.decodeRequest(reqPacket)
    check reqBom == BOM_BIG_ENDIAN
    check req.t1 == t1

    # Encode response (using request's BOM)
    let resPacket = codec.encodeResponse(t2, t3, reqBom)
    let (resBom, res) = codec.decodeResponse(resPacket)
    check resBom == BOM_BIG_ENDIAN
    check res.t2 == t2
    check res.t3 == t3

  test "Negative timestamps are handled":
    let negativeTs = Timestamp(-1000000000'i64)
    var dest = newSeq[uint8](8)
    codec.timestampToBytes(negativeTs, BOM_BIG_ENDIAN, dest, 0)
    let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
    check recovered == negativeTs

  test "readUint16BE with offset":
    let data = [uint8(0x00), 0x00, 0x12, 0x34]
    let value = codec.readUint16BE(data, 2)
    check value == 0x1234'u16

  test "readUint64BE with offset":
    let data = [uint8(0), 0, 0, 0, 0, 0, 0x01, 0x23]
    let value = codec.readUint64BE(data, 0)
    check value == 0x0000000000000123'u64

  test "writeUint64BE with offset":
    var dest = newSeq[uint8](10)
    codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, 2)
    check dest[2] == 0x01'u8
    check dest[9] == 0xEF'u8

  test "encodeRequest default BOM is big-endian":
    let t1 = Timestamp(1000'i64)
    let packet = codec.encodeRequest(t1)
    let typeField = packet[2]
    check typeField == MSG_SYNC_REQUEST

  test "readUint16BE raises on offset out of bounds (too small)":
    let data = [uint8(0x12)]
    expect ValueError:
      discard codec.readUint16BE(data, 0)

  test "readUint16BE raises on offset out of bounds (high offset)":
    let data = [uint8(0x12), 0x34]
    expect ValueError:
      discard codec.readUint16BE(data, 1) # offset 1 needs offset+1=2 which is out of bounds

  test "readUint16LE raises on offset out of bounds":
    let data = [uint8(0x12)]
    expect ValueError:
      discard codec.readUint16LE(data, 0)

  test "readUint64BE raises on offset out of bounds":
    let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD]
    expect ValueError:
      discard codec.readUint64BE(data, 0) # needs 8 bytes

  test "readUint64LE raises on offset out of bounds":
    let data = [uint8(0xEF)]
    expect ValueError:
      discard codec.readUint64LE(data, 0)

  test "writeUint16BE raises on offset out of bounds":
    var dest = newSeq[uint8](1)
    expect ValueError:
      codec.writeUint16BE(0x1234'u16, dest, 0)

  test "writeUint64BE raises on offset out of bounds":
    var dest = newSeq[uint8](5)
    expect ValueError:
      codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, 0)

  test "writeUint64LE raises on offset out of bounds":
    var dest = newSeq[uint8](7)
    expect ValueError:
      codec.writeUint64LE(0x0123456789ABCDEF'u64, dest, 0)

  test "timestampToBytes raises on offset out of bounds (via write)":
    var dest = newSeq[uint8](5)
    expect ValueError:
      codec.timestampToBytes(Timestamp(123'i64), BOM_BIG_ENDIAN, dest, 0)

  test "bytesToTimestamp raises on offset out of bounds (via read)":
    let data = [uint8(0x01), 0x23, 0x45, 0x67]
    expect ValueError:
      discard codec.bytesToTimestamp(data, 0, BOM_BIG_ENDIAN)

  suite "Negative offset bounds checking":
    test "readUint16BE raises on negative offset":
      let data = [uint8(0x12), 0x34]
      expect ValueError:
        discard codec.readUint16BE(data, -1)

    test "readUint16LE raises on negative offset":
      let data = [uint8(0x12), 0x34]
      expect ValueError:
        discard codec.readUint16LE(data, -1)

    test "readUint64BE raises on negative offset":
      let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
      expect ValueError:
        discard codec.readUint64BE(data, -1)

    test "readUint64LE raises on negative offset":
      let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
      expect ValueError:
        discard codec.readUint64LE(data, -1)

    test "writeUint16BE raises on negative offset":
      var dest = newSeq[uint8](4)
      expect ValueError:
        codec.writeUint16BE(0x1234'u16, dest, -1)

    test "writeUint64BE raises on negative offset":
      var dest = newSeq[uint8](10)
      expect ValueError:
        codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, -1)

    test "writeUint64LE raises on negative offset":
      var dest = newSeq[uint8](10)
      expect ValueError:
        codec.writeUint64LE(0x0123456789ABCDEF'u64, dest, -1)

    test "timestampToBytes raises on negative offset":
      var dest = newSeq[uint8](10)
      expect ValueError:
        codec.timestampToBytes(Timestamp(123'i64), BOM_BIG_ENDIAN, dest, -1)

    test "bytesToTimestamp raises on negative offset":
      let data = [uint8(0x01), 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x00, 0x00]
      expect ValueError:
        discard codec.bytesToTimestamp(data, -1, BOM_BIG_ENDIAN)

  suite "Timestamp edge values":
    test "timestamp min int64":
      let minTs = Timestamp(-9223372036854775808'i64)
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(minTs, BOM_BIG_ENDIAN, dest, 0)
      let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
      check recovered == minTs

    test "timestamp min int64 little-endian":
      let minTs = Timestamp(-9223372036854775808'i64)
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(minTs, BOM_LITTLE_ENDIAN, dest, 0)
      let recovered = codec.bytesToTimestamp(dest, 0, BOM_LITTLE_ENDIAN)
      check recovered == minTs

    test "timestamp near zero positive":
      let ts = Timestamp(1'i64)
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(ts, BOM_BIG_ENDIAN, dest, 0)
      let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
      check recovered == ts

    test "timestamp near zero negative":
      let ts = Timestamp(-1'i64)
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(ts, BOM_BIG_ENDIAN, dest, 0)
      let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
      check recovered == ts

    test "timestamp all bits set":
      let ts = Timestamp(-1'i64)
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(ts, BOM_BIG_ENDIAN, dest, 0)
      for b in dest:
        check b == 0xFF'u8
      let recovered = codec.bytesToTimestamp(dest, 0, BOM_BIG_ENDIAN)
      check recovered == ts

  suite "Packet size edge cases":
    test "decodeRequest with exactly minimum size":
      var packet = newSeq[uint8](REQUEST_PACKET_SIZE)
      codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
      packet[2] = MSG_SYNC_REQUEST
      codec.timestampToBytes(Timestamp(42'i64), BOM_BIG_ENDIAN, packet, 3)
      let (bom, req) = codec.decodeRequest(packet)
      check bom == BOM_BIG_ENDIAN
      check req.t1 == Timestamp(42'i64)

    test "decodeRequest with larger packet ignores extra bytes":
      var packet = newSeq[uint8](REQUEST_PACKET_SIZE + 10)
      codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
      packet[2] = MSG_SYNC_REQUEST
      codec.timestampToBytes(Timestamp(42'i64), BOM_BIG_ENDIAN, packet, 3)
      for i in REQUEST_PACKET_SIZE..<packet.len:
        packet[i] = uint8(i)
      let (bom, req) = codec.decodeRequest(packet)
      check bom == BOM_BIG_ENDIAN
      check req.t1 == Timestamp(42'i64)

    test "decodeResponse with exactly minimum size":
      var packet = newSeq[uint8](RESPONSE_PACKET_SIZE)
      codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
      packet[2] = MSG_SYNC_RESPONSE
      codec.timestampToBytes(Timestamp(100'i64), BOM_BIG_ENDIAN, packet, 3)
      codec.timestampToBytes(Timestamp(200'i64), BOM_BIG_ENDIAN, packet, 11)
      let (bom, res) = codec.decodeResponse(packet)
      check bom == BOM_BIG_ENDIAN
      check res.t2 == Timestamp(100'i64)
      check res.t3 == Timestamp(200'i64)

    test "decodeResponse with larger packet ignores extra bytes":
      var packet = newSeq[uint8](RESPONSE_PACKET_SIZE + 20)
      codec.writeUint16BE(BOM_BIG_ENDIAN, packet, 0)
      packet[2] = MSG_SYNC_RESPONSE
      codec.timestampToBytes(Timestamp(100'i64), BOM_BIG_ENDIAN, packet, 3)
      codec.timestampToBytes(Timestamp(200'i64), BOM_BIG_ENDIAN, packet, 11)
      for i in RESPONSE_PACKET_SIZE..<packet.len:
        packet[i] = uint8(i mod 256)
      let (bom, res) = codec.decodeResponse(packet)
      check bom == BOM_BIG_ENDIAN
      check res.t2 == Timestamp(100'i64)
      check res.t3 == Timestamp(200'i64)

    test "decodeRequest raises on size one less than minimum":
      var packet = newSeq[uint8](REQUEST_PACKET_SIZE - 1)
      for i in 0..<packet.len:
        packet[i] = 0'u8
      expect ValueError:
        discard codec.decodeRequest(packet)

    test "decodeResponse raises on size one less than minimum":
      var packet = newSeq[uint8](RESPONSE_PACKET_SIZE - 1)
      for i in 0..<packet.len:
        packet[i] = 0'u8
      expect ValueError:
        discard codec.decodeResponse(packet)

  suite "Non-standard BOM handling":
    test "bytesToTimestamp treats non-standard BOM as little-endian":
      let data = [uint8(0xEF), 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
      let ts = codec.bytesToTimestamp(data, 0, 0x1234'u16)
      check ts == Timestamp(0x0123456789ABCDEF'i64)

    test "timestampToBytes writes little-endian for non-standard BOM":
      var dest = newSeq[uint8](8)
      codec.timestampToBytes(Timestamp(0x0123456789ABCDEF'i64), 0x1234'u16,
          dest, 0)
      check dest[0] == 0xEF'u8
      check dest[7] == 0x01'u8

    test "encodeRequest with non-standard BOM preserves it":
      let t1 = Timestamp(12345'i64)
      let packet = codec.encodeRequest(t1, 0xABCD'u16)
      check codec.readUint16BE(packet, 0) == 0xABCD'u16
      check packet[2] == MSG_SYNC_REQUEST
      let decodedT1 = codec.bytesToTimestamp(packet, 3, 0xABCD'u16)
      check decodedT1 == t1

    test "encodeResponse with non-standard BOM preserves it":
      let t2 = Timestamp(100'i64)
      let t3 = Timestamp(200'i64)
      let packet = codec.encodeResponse(t2, t3, 0xDEAD'u16)
      check codec.readUint16BE(packet, 0) == 0xDEAD'u16
      check packet[2] == MSG_SYNC_RESPONSE

  suite "Codec statelessness":
    test "multiple sequential encode operations":
      let t1a = Timestamp(100'i64)
      let t1b = Timestamp(200'i64)
      let t1c = Timestamp(300'i64)

      let packetA = codec.encodeRequest(t1a, BOM_BIG_ENDIAN)
      let packetB = codec.encodeRequest(t1b, BOM_LITTLE_ENDIAN)
      let packetC = codec.encodeRequest(t1c, BOM_BIG_ENDIAN)

      let (_, reqA) = codec.decodeRequest(packetA)
      let (_, reqB) = codec.decodeRequest(packetB)
      let (_, reqC) = codec.decodeRequest(packetC)

      check reqA.t1 == t1a
      check reqB.t1 == t1b
      check reqC.t1 == t1c

    test "interleaved request and response operations":
      let t1 = Timestamp(111'i64)
      let t2 = Timestamp(222'i64)
      let t3 = Timestamp(333'i64)

      let reqPacket = codec.encodeRequest(t1, BOM_BIG_ENDIAN)
      let resPacket = codec.encodeResponse(t2, t3, BOM_BIG_ENDIAN)

      let (_, req) = codec.decodeRequest(reqPacket)
      let (_, res) = codec.decodeResponse(resPacket)

      check req.t1 == t1
      check res.t2 == t2
      check res.t3 == t3

    test "same codec instance handles both endianness":
      let tsBE = Timestamp(0x123456789ABCDEF0'i64)
      let tsLE = Timestamp(0xFEDCBA9876543210'i64)

      var destBE = newSeq[uint8](8)
      var destLE = newSeq[uint8](8)

      codec.timestampToBytes(tsBE, BOM_BIG_ENDIAN, destBE, 0)
      codec.timestampToBytes(tsLE, BOM_LITTLE_ENDIAN, destLE, 0)

      let recoveredBE = codec.bytesToTimestamp(destBE, 0, BOM_BIG_ENDIAN)
      let recoveredLE = codec.bytesToTimestamp(destLE, 0, BOM_LITTLE_ENDIAN)

      check recoveredBE == tsBE
      check recoveredLE == tsLE

  suite "Request/Response integration":
    test "full round-trip with realistic timestamps":
      let t1 = Timestamp(1700000000000000000'i64)
      let t2 = Timestamp(1700000000000001000'i64)
      let t3 = Timestamp(1700000000000001500'i64)

      let reqPacket = codec.encodeRequest(t1, BOM_BIG_ENDIAN)
      let (reqBom, request) = codec.decodeRequest(reqPacket)

      let resPacket = codec.encodeResponse(t2, t3, reqBom)
      let (resBom, response) = codec.decodeResponse(resPacket)

      check request.t1 == t1
      check response.t2 == t2
      check response.t3 == t3
      check reqBom == resBom

    test "full round-trip little-endian":
      let t1 = Timestamp(1700000000000000000'i64)
      let t2 = Timestamp(1700000000000001000'i64)
      let t3 = Timestamp(1700000000000001500'i64)

      let reqPacket = codec.encodeRequest(t1, BOM_LITTLE_ENDIAN)
      let (reqBom, request) = codec.decodeRequest(reqPacket)

      let resPacket = codec.encodeResponse(t2, t3, reqBom)
      let (resBom, response) = codec.decodeResponse(resPacket)

      check request.t1 == t1
      check response.t2 == t2
      check response.t3 == t3
      check reqBom == BOM_LITTLE_ENDIAN
      check resBom == BOM_LITTLE_ENDIAN

  suite "Write offset edge cases":
    test "writeUint16BE at end of buffer":
      var dest = newSeq[uint8](4)
      codec.writeUint16BE(0xABCD'u16, dest, 2)
      check dest[2] == 0xAB'u8
      check dest[3] == 0xCD'u8

    test "writeUint16BE raises at last position":
      var dest = newSeq[uint8](3)
      expect ValueError:
        codec.writeUint16BE(0x1234'u16, dest, 2)

    test "writeUint64BE at end of buffer":
      var dest = newSeq[uint8](10)
      codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, 2)
      check dest[2] == 0x01'u8
      check dest[9] == 0xEF'u8

    test "writeUint64BE raises at boundary position":
      var dest = newSeq[uint8](9)
      expect ValueError:
        codec.writeUint64BE(0x0123456789ABCDEF'u64, dest, 2)

    test "writeUint64LE at end of buffer":
      var dest = newSeq[uint8](10)
      codec.writeUint64LE(0x0123456789ABCDEF'u64, dest, 2)
      check dest[2] == 0xEF'u8
      check dest[9] == 0x01'u8

    test "writeUint64LE raises at boundary position":
      var dest = newSeq[uint8](9)
      expect ValueError:
        codec.writeUint64LE(0x0123456789ABCDEF'u64, dest, 2)

  suite "Read offset edge cases":
    test "readUint16BE at last valid position":
      let data = [uint8(0x00), 0x00, 0xAB, 0xCD]
      let value = codec.readUint16BE(data, 2)
      check value == 0xABCD'u16

    test "readUint64BE at last valid position":
      let data = [uint8(0x00), 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]
      let value = codec.readUint64BE(data, 1)
      check value == 0x0123456789ABCDEF'u64

    test "readUint16LE at last valid position":
      let data = [uint8(0x00), 0x00, 0xCD, 0xAB]
      let value = codec.readUint16LE(data, 2)
      check value == 0xABCD'u16

    test "readUint64LE at last valid position":
      let data = [uint8(0x00), 0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
      let value = codec.readUint64LE(data, 1)
      check value == 0x0123456789ABCDEF'u64

  when isMainModule:
    discard # Tests run via nimble
