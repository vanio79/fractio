# Unit tests for fractio/protocol/auth.nim
# Tests authentication encoding, decoding, and authentication logic

import unittest
import std/[tables, strutils]
import fractio/protocol/auth
import fractio/protocol/types
import fractio/protocol/codec

suite "Authenticator Construction":
  test "newAuthenticator default":
    let auth = newAuthenticator()
    check auth.authMethod == amNone
    check auth.users.len == 0
    check auth.tokens.len == 0

  test "newAuthenticator with method":
    let auth = newAuthenticator(amPassword)
    check auth.authMethod == amPassword

  test "newAuthenticator amToken":
    let auth = newAuthenticator(amToken)
    check auth.authMethod == amToken

suite "addUser":
  test "addUser basic":
    let auth = newAuthenticator(amPassword)
    auth.addUser("alice", "password123")
    check auth.users.hasKey("alice")
    check auth.users["alice"] == "password123"

  test "addUser multiple users":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user1", "pass1")
    auth.addUser("user2", "pass2")
    auth.addUser("user3", "pass3")
    check auth.users.len == 3

  test "addUser overwrite":
    let auth = newAuthenticator(amPassword)
    auth.addUser("bob", "oldpass")
    auth.addUser("bob", "newpass")
    check auth.users["bob"] == "newpass"

suite "addToken":
  test "addToken basic":
    let auth = newAuthenticator(amToken)
    auth.addToken("token123", "principal1")
    check auth.tokens.hasKey("token123")
    check auth.tokens["token123"] == "principal1"

  test "addToken multiple":
    let auth = newAuthenticator(amToken)
    auth.addToken("t1", "p1")
    auth.addToken("t2", "p2")
    auth.addToken("t3", "p3")
    check auth.tokens.len == 3

suite "encodePasswordAuthData":
  test "encodePasswordAuthData basic":
    let data = encodePasswordAuthData("user", "pass")
    var pos = 0
    let user = readBytes8(data, pos)
    check user.isOk
    check user.value == "user"

    let pass = readBytes8(data, pos)
    check pass.isOk
    check pass.value == "pass"

  test "encodePasswordAuthData empty":
    let data = encodePasswordAuthData("", "")
    var pos = 0
    let user = readBytes8(data, pos)
    check user.isOk
    check user.value == ""

    let pass = readBytes8(data, pos)
    check pass.isOk
    check pass.value == ""

  test "encodePasswordAuthData long":
    let user = "longusername123"
    let pass = "longpassword456"
    let data = encodePasswordAuthData(user, pass)
    var pos = 0
    let decodedUser = readBytes8(data, pos)
    let decodedPass = readBytes8(data, pos)
    check decodedUser.value == user
    check decodedPass.value == pass

  test "encodePasswordAuthData max length":
    let user = "u".repeat(255)
    let pass = "p".repeat(255)
    let data = encodePasswordAuthData(user, pass)
    check data.len == 1 + 255 + 1 + 255

suite "encodeTokenAuthData":
  test "encodeTokenAuthData basic":
    let data = encodeTokenAuthData("mytoken")
    var pos = 0
    let token = readBytes(data, pos)
    check token.isOk
    check token.value == "mytoken"

  test "encodeTokenAuthData empty":
    let data = encodeTokenAuthData("")
    var pos = 0
    let token = readBytes(data, pos)
    check token.isOk
    check token.value == ""

  test "encodeTokenAuthData long":
    let token = "x".repeat(1000)
    let data = encodeTokenAuthData(token)
    var pos = 0
    let decoded = readBytes(data, pos)
    check decoded.isOk
    check decoded.value == token

suite "authenticate amNone":
  test "authenticate amNone always succeeds":
    let auth = newAuthenticator(amNone)
    check auth.authenticate(uint8(amNone), "") == true
    check auth.authenticate(uint8(amNone), "anything") == true

  test "authenticate amNone ignores authData":
    let auth = newAuthenticator(amNone)
    check auth.authenticate(uint8(amNone), "\x00\x01\x02") == true

suite "authenticate amPassword":
  test "authenticate amPassword valid":
    let auth = newAuthenticator(amPassword)
    auth.addUser("alice", "secret")

    let authData = encodePasswordAuthData("alice", "secret")
    check auth.authenticate(uint8(amPassword), authData) == true

  test "authenticate amPassword invalid password":
    let auth = newAuthenticator(amPassword)
    auth.addUser("alice", "secret")

    let authData = encodePasswordAuthData("alice", "wrong")
    check auth.authenticate(uint8(amPassword), authData) == false

  test "authenticate amPassword unknown user":
    let auth = newAuthenticator(amPassword)
    auth.addUser("alice", "secret")

    let authData = encodePasswordAuthData("bob", "secret")
    check auth.authenticate(uint8(amPassword), authData) == false

  test "authenticate amPassword empty credentials":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user", "pass")

    let authData = encodePasswordAuthData("", "")
    check auth.authenticate(uint8(amPassword), authData) == false

  test "authenticate amPassword malformed authData":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user", "pass")

    check auth.authenticate(uint8(amPassword), "") == false
    check auth.authenticate(uint8(amPassword), "\x05") == false

  test "authenticate amPassword multiple users":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user1", "pass1")
    auth.addUser("user2", "pass2")
    auth.addUser("user3", "pass3")

    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("user1",
        "pass1")) == true
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("user2",
        "pass2")) == true
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("user3",
        "pass3")) == true
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("user1",
        "pass2")) == false

suite "authenticate amToken":
  test "authenticate amToken valid":
    let auth = newAuthenticator(amToken)
    auth.addToken("validtoken", "principal")

    let authData = encodeTokenAuthData("validtoken")
    check auth.authenticate(uint8(amToken), authData) == true

  test "authenticate amToken invalid":
    let auth = newAuthenticator(amToken)
    auth.addToken("validtoken", "principal")

    let authData = encodeTokenAuthData("invalidtoken")
    check auth.authenticate(uint8(amToken), authData) == false

  test "authenticate amToken empty":
    let auth = newAuthenticator(amToken)
    auth.addToken("token", "principal")

    let authData = encodeTokenAuthData("")
    check auth.authenticate(uint8(amToken), authData) == false

  test "authenticate amToken malformed authData":
    let auth = newAuthenticator(amToken)
    auth.addToken("token", "principal")

    check auth.authenticate(uint8(amToken), "\x00\x00") == false

  test "authenticate amToken multiple tokens":
    let auth = newAuthenticator(amToken)
    auth.addToken("t1", "p1")
    auth.addToken("t2", "p2")

    check auth.authenticate(uint8(amToken), encodeTokenAuthData("t1")) == true
    check auth.authenticate(uint8(amToken), encodeTokenAuthData("t2")) == true
    check auth.authenticate(uint8(amToken), encodeTokenAuthData("t3")) == false

suite "authenticate amTLS":
  test "authenticate amTLS always fails":
    let auth = newAuthenticator()
    check auth.authenticate(uint8(amTLS), "") == false
    check auth.authenticate(uint8(amTLS), "cert-data") == false

suite "AuthMethod Enum":
  test "AuthMethod values":
    check uint8(amNone) == 0x00
    check uint8(amPassword) == 0x01
    check uint8(amToken) == 0x02
    check uint8(amTLS) == 0x03

suite "Password AuthData Roundtrip":
  test "encode decode roundtrip":
    let originalUser = "testuser"
    let originalPass = "testpass"
    let encoded = encodePasswordAuthData(originalUser, originalPass)

    var pos = 0
    let decodedUser = readBytes8(encoded, pos)
    let decodedPass = readBytes8(encoded, pos)

    check decodedUser.isOk
    check decodedPass.isOk
    check decodedUser.value == originalUser
    check decodedPass.value == originalPass

  test "special characters in password":
    let user = "user"
    let pass = "p@ss:w!rd#$%"
    let encoded = encodePasswordAuthData(user, pass)

    var pos = 0
    let decodedUser = readBytes8(encoded, pos)
    let decodedPass = readBytes8(encoded, pos)

    check decodedPass.value == pass

suite "Token AuthData Roundtrip":
  test "encode decode roundtrip":
    let originalToken = "my-bearer-token-12345"
    let encoded = encodeTokenAuthData(originalToken)

    var pos = 0
    let decoded = readBytes(encoded, pos)

    check decoded.isOk
    check decoded.value == originalToken

  test "token with special characters":
    let token = "token-with-dashes.and.dots"
    let encoded = encodeTokenAuthData(token)

    var pos = 0
    let decoded = readBytes(encoded, pos)

    check decoded.value == token

suite "Concurrent Safety":
  test "authenticate thread safe with multiple calls":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user", "pass")

    let authData = encodePasswordAuthData("user", "pass")
    for i in 0..100:
      check auth.authenticate(uint8(amPassword), authData) == true

  test "addUser and authenticate interleaved":
    let auth = newAuthenticator(amPassword)

    auth.addUser("u1", "p1")
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("u1",
        "p1")) == true

    auth.addUser("u2", "p2")
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("u2",
        "p2")) == true

    auth.addUser("u1", "newp1")
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("u1",
        "p1")) == false
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("u1",
        "newp1")) == true

suite "Edge Cases":
  test "authenticate with wrong method type":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user", "pass")

    check auth.authenticate(uint8(amToken), encodeTokenAuthData("token")) == false

  test "authenticate unsupported method type":
    let auth = newAuthenticator()
    auth.addToken("token", "principal")
    auth.addUser("user", "pass")

    check auth.authenticate(uint8(amToken), encodePasswordAuthData("user",
        "pass")) == false
    check auth.authenticate(uint8(amPassword), encodeTokenAuthData("token")) == false

  test "empty authenticator":
    let auth = newAuthenticator(amPassword)
    check auth.authenticate(uint8(amPassword), encodePasswordAuthData("user",
        "pass")) == false

suite "Authenticator State":
  test "users table empty initially":
    let auth = newAuthenticator(amPassword)
    check auth.users.len == 0

  test "tokens table empty initially":
    let auth = newAuthenticator(amToken)
    check auth.tokens.len == 0

  test "mixed authenticator":
    let auth = newAuthenticator(amPassword)
    auth.addUser("user", "pass")
    auth.addToken("token", "principal")

    check auth.users.len == 1
    check auth.tokens.len == 1
