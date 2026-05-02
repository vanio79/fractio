## ULID - Universally Unique Lexicographically Sortable Identifier
## Minimal pure-Nim implementation (replaces external ulid package)
## Uses std/random to avoid dependency conflicts with the nimble "random" package.

import std/[times, random]

const
  alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  alphabetSize = len(alphabet)

var rng = initRand()

proc encodeTime(now: int, length = 10): string =
  result = ""
  var t = now
  for _ in 1..length:
    let mo = t mod alphabetSize
    result = alphabet[mo] & result
    t = (t - mo) div alphabetSize

proc encodeRandom(length = 16): string =
  result = ""
  for _ in 1..length:
    let r = rng.rand(alphabetSize - 1)
    result = alphabet[r] & result

proc ulid*(now = 0): string =
  ## Generate a new ULID string.
  ## If `now` is 0, uses the current Unix time in milliseconds.
  var t = now
  if t == 0:
    t = int(times.epochTime() * 1000)
  return encodeTime(t) & encodeRandom()
