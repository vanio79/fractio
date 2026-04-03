# Debug Logging Templates
#
# Compile-time conditional debug output for Fractio.
# Enable with -d:debugRpc, -d:debugStorage, -d:debugRaft, or -d:debug (all).
#
# Usage:
#   debugRpc("sendRequest: gid=", gid, " term=", term, " dst=", dstNodeId)
#
# Benefits:
#   - Zero overhead in production (compiles to nothing)
#   - Consistent format across Nim codebase
#   - No C/C++ debug macros needed

import std/strutils

# =============================================================================
# Helper: Join varargs into a single string
# =============================================================================

proc joinArgs*(args: varargs[string]): string {.inline.} =
  result = ""
  for a in args:
    result.add(a)

# =============================================================================
# Debug Templates by Category
# =============================================================================

template debugRpc*(args: varargs[any, `$`]) =
  ## Log RPC/messaging operations (Raft message delivery, serialization)
  ## Enable with: nim c -d:debugRpc
  when defined(debugRpc) or defined(debug):
    stderr.writeLine("[RPC] " & joinArgs(args))

template debugRaft*(args: varargs[any, `$`]) =
  ## Log Raft consensus operations (elections, leader changes)
  ## Enable with: nim c -d:debugRaft or -d:debugRpc
  when defined(debugRaft) or defined(debugRpc) or defined(debug):
    stderr.writeLine("[RAFT] " & joinArgs(args))

template debugStorage*(args: varargs[any, `$`]) =
  ## Log storage operations (MVCC, WAL, SSTables, WiscKey)
  ## Enable with: nim c -d:debugStorage
  when defined(debugStorage) or defined(debug):
    stderr.writeLine("[STORAGE] " & joinArgs(args))

template debugTxn*(args: varargs[any, `$`]) =
  ## Log transaction operations (begin, commit, rollback)
  ## Enable with: nim c -d:debugTxn
  when defined(debugTxn) or defined(debug):
    stderr.writeLine("[TXN] " & joinArgs(args))

template debugRebalance*(args: varargs[any, `$`]) =
  ## Log space rebalancing operations
  ## Enable with: nim c -d:debugRebalance
  when defined(debugRebalance) or defined(debug):
    stderr.writeLine("[REBALANCE] " & joinArgs(args))

template debugTransport*(args: varargs[any, `$`]) =
  ## Log transport/network operations (TCP connections, message framing)
  ## Enable with: nim c -d:debugTransport
  when defined(debugTransport) or defined(debug):
    stderr.writeLine("[TRANSPORT] " & joinArgs(args))

# =============================================================================
# General Debug Template
# =============================================================================

template debug*(args: varargs[any, `$`]) =
  ## General debug output (always enabled with -d:debug)
  when defined(debug):
    stderr.writeLine("[DEBUG] " & joinArgs(args))
