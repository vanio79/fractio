# Minimal test for LSM Tree v2 crash

import std/[options, times, os]
import fractio/storage/lsm_tree_v2/[lsm_tree, config, types, error]

proc makeKeyStr(prefix: string, i: uint64, keySize: int): string =
  let suffix = "_" & $i
  let padLen = keySize - prefix.len - suffix.len
  if padLen > 0:
    result = newString(keySize)
    for j in 0 ..< keySize:
      if j < prefix.len:
        result[j] = prefix[j]
      elif j < prefix.len + padLen:
        result[j] = 'k'
      else:
        result[j] = suffix[j - prefix.len - padLen]
  else:
    result = prefix & suffix

proc makeValueStr(valueSize: int): string =
  if valueSize > 0:
    result = newString(valueSize)
    for j in 0 ..< valueSize:
      result[j] = 'v'
  else:
    result = "v"

proc main() =
  echo "Starting test..."

  let tmpDir = "/tmp/test_lsm_bench"
  if dirExists(tmpDir):
    removeDir(tmpDir)
  createDir(tmpDir)

  let cfg = newDefaultConfig(tmpDir)
  let treeResult = createNewTree(cfg, 0)

  if treeResult.isErr:
    echo "Error creating tree: ", treeResult.error
    quit(1)

  let tree = treeResult.value
  echo "Tree created"

  let keySize = 16

  # First 10 warmup inserts
  for i in 0'u64 ..< 10'u64:
    let key = makeKeyStr("warm", i, keySize)
    let value = "value"
    discard tree.insert(key, value, i)

  echo "10 warmup done"

  # Now try more "seq" keys with higher seqno
  var seqno: uint64 = 10
  for i in 0'u64 ..< 80'u64:
    let key = makeKeyStr("seq", i, keySize)
    let value = "value"
    discard tree.insert(key, value, seqno)
    seqno += 1
    if i mod 10 == 0:
      echo "  ", i

  echo "80 seq writes done"

  echo "Test passed!"

when isMainModule:
  main()
