# Authentication module for the Fractio protocol layer.
#
# Supported methods:
#   amNone     (0x00) — no authentication; always succeeds
#   amPassword (0x01) — username:password in authData (UTF-8, colon-separated)
#   amToken    (0x02) — opaque bearer token in authData
#
# Wire format for amPassword authData:
#   [usernameLen: 1 byte][username: N bytes][passwordLen: 1 byte][password: M bytes]
#
# Wire format for amToken authData:
#   [tokenLen: 4 bytes BE][token: N bytes]
#
# Passwords are stored as plain strings for simplicity (no hash in Phase 4;
# production would use bcrypt/argon2 — tracked as a Phase 5 hardening task).
#
# All procs are {.gcsafe, raises: [].} so they can be called safely from
# the per-connection reader thread.

import std/[tables, locks]
import ./types
import ./codec

# ---------------------------------------------------------------------------
# Authenticator
# ---------------------------------------------------------------------------

type
  Authenticator* = ref object
    authMethod*: AuthMethod
    users*: Table[string, string]  ## username -> plaintext password (amPassword)
    tokens*: Table[string, string] ## token -> principal name (amToken)
    mu*: Lock

proc newAuthenticator*(meth: AuthMethod = amNone): Authenticator =
  result = Authenticator(
    authMethod: meth,
    users: initTable[string, string](),
    tokens: initTable[string, string](),
  )
  initLock(result.mu)

proc addUser*(auth: Authenticator, username, password: string) =
  ## Register a username/password credential (amPassword).
  acquire(auth.mu)
  defer: release(auth.mu)
  auth.users[username] = password

proc addToken*(auth: Authenticator, token, principal: string) =
  ## Register a bearer token mapping to a principal name (amToken).
  acquire(auth.mu)
  defer: release(auth.mu)
  auth.tokens[token] = principal

# ---------------------------------------------------------------------------
# authData encoding helpers (used by client to build ClientHandshake.authData)
# ---------------------------------------------------------------------------

proc encodePasswordAuthData*(username, password: string): string =
  ## Encode username + password into the authData wire format.
  ## Format: [usernameLen:1][username:N][passwordLen:1][password:M]
  var buf = ""
  buf.writeBytes8(username)
  buf.writeBytes8(password)
  buf

proc encodeTokenAuthData*(token: string): string =
  ## Encode bearer token into the authData wire format.
  ## Format: [tokenLen:4 BE][token:N]
  var buf = ""
  buf.writeBytes(token) # writeBytes = uint32-length-prefixed
  buf

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

proc authenticate*(auth: Authenticator,
    authType: uint8,
    authData: string): bool {.gcsafe, raises: [].} =
  ## Returns true if authentication succeeds for the given authType + authData.
  ## Always returns true when authMethod == amNone.
  let meth = AuthMethod(authType)
  case meth
  of amNone:
    return true

  of amPassword:
    # Decode: [usernameLen:1][username:N][passwordLen:1][password:M]
    var pos = 0
    let userR = readBytes8(authData, pos)
    if userR.isErr: return false
    let passR = readBytes8(authData, pos)
    if passR.isErr: return false
    let username = userR.value
    let password = passR.value
    acquire(auth.mu)
    let stored = auth.users.getOrDefault(username, "")
    release(auth.mu)
    return stored.len > 0 and stored == password

  of amToken:
    # Decode: [tokenLen:4 BE][token:N]
    var pos = 0
    let tokR = readBytes(authData, pos)
    if tokR.isErr: return false
    let token = tokR.value
    acquire(auth.mu)
    let principal = auth.tokens.getOrDefault(token, "")
    release(auth.mu)
    return principal.len > 0

  of amTLS:
    # TLS client certificate validation is out of scope for Phase 4.
    # Defer to the TLS layer; if we reach here treat as denied.
    return false
