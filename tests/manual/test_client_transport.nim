# Simple test to verify client transport is working
import std/[net, os, times, endians, sequtils, strutils]
import fractio/distributed/network/types
import fractio/distributed/network/serialization
import fractio/distributed/network/tcp_transport
import fractio/distributed/network/raft_transport
import fractio/distributed/network/config

proc testClientTransport() =
  echo "Creating TCP transport..."
  let config = newNetworkConfig(toNodeID(1), 30000, "127.0.0.1")
  let transport = newTCPTransport(config, 30001, "client")

  # Register a simple handler
  var handlerCalled = false
  transport.registerHandler(uint16(cmtBatchRequest), proc(
      data: string): string {.gcsafe.} =
    echo "Handler called! Data length: ", data.len
    handlerCalled = true
    # Return a simple response
    var resp: BatchResponseMsg
    resp.header = newMessageHeader(uint16(cmtBatchResponse), 1, toNodeID(1),
        toNodeID(2))
    resp.requestId = 1
    resp.success = true
    resp.responses = @[]
    result = encodeBatchResponseMsg(resp)
  )

  echo "Starting server on port 30001..."
  if not transport.startServer():
    echo "Failed to start server"
    return

  echo "Server started, waiting for accept thread..."
  sleep(500) # Give the accept thread time to start

  echo "Creating client socket..."
  let client = newSocket()
  client.connect("127.0.0.1", Port(30001), timeout = 5000)
  echo "Client connected"

  # Create a batch request
  var msg: BatchRequestMsg
  msg.header = newMessageHeader(uint16(cmtBatchRequest), 1, toNodeID(2),
      toNodeID(1))
  msg.requestId = 1
  msg.groupId = 1
  msg.transactionId = 0
  msg.requests = @[]

  let encoded = encodeBatchRequestMsg(msg)
  let frame = encodeFrame(encoded)

  echo "Sending frame of size: ", frame.len
  echo "Encoded message size: ", encoded.len

  # Print first few bytes for debugging
  echo "Frame first 20 bytes: ", frame[0..<min(20, frame.len)].mapIt(
      it.byte.int.toHex(2)).join(" ")
  echo "Payload first 20 bytes: ", encoded[0..<min(20, encoded.len)].mapIt(
      it.byte.int.toHex(2)).join(" ")

  client.send(frame)
  echo "Frame sent, waiting for response..."

  # Read response
  var headerBuf = newString(FRAME_HEADER_SIZE)
  let n = client.recv(headerBuf, FRAME_HEADER_SIZE, 5000)
  echo "Received header bytes: ", n

  if n >= FRAME_HEADER_SIZE:
    let (header, _) = decodeFrameHeader(headerBuf)
    echo "Payload length: ", header.payloadLen

    var payload = newString(header.payloadLen.int)
    let n2 = client.recv(payload, header.payloadLen.int, 5000)
    echo "Received payload bytes: ", n2

    if n2 >= header.payloadLen.int:
      let resp = decodeBatchResponseMsg(payload)
      echo "Response success: ", resp.success
    else:
      echo "Failed to receive full payload"
  else:
    echo "Failed to receive header, timeout or connection closed"

  echo "Handler was called: ", handlerCalled

  client.close()
  transport.stopServer()
  transport.close()
  echo "Test complete"

when isMainModule:
  testClientTransport()
