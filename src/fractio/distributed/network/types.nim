# Network Types - Helper procs for message types
# TCP-based network communication for distributed Fractio
#
# This file contains ONLY executable code (procs).
# Type definitions are in types_base.nim which is excluded from coverage.

import ./types_base
from ../raft/group_types import `$` # Import $ for GroupID
export types_base

# ==========================================================================
# Helper Procs
# ==========================================================================

proc newMessageHeader*(msgType: uint16, msgId: uint64,
                       source, target: NodeID,
                           term: uint64 = 0,
                           groupId: GroupID = ZeroGroupID()): MessageHeader =
  ## Create a new message header
  result.messageType = msgType
  result.messageId = msgId
  result.sourceNodeId = source
  result.targetNodeId = target
  result.term = term
  result.timestamp = 0 # Set by sender
  result.groupId = groupId

proc newNetworkError*(code: NetworkErrorCode, msg: string): NetworkError =
  ## Create a new network error
  result = NetworkError(
    code: code,
    msg: msg
  )

proc `$`*(header: MessageHeader): string =
  ## String representation of message header
  result = "MessageHeader(type=" & $header.messageType &
           ", id=" & $header.messageId &
           ", src=" & string(header.sourceNodeId) &
           ", dst=" & string(header.targetNodeId) &
           ", term=" & $header.term &
           ", groupId=" & $header.groupId & ")"
