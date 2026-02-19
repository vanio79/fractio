# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

import fractio/storage/[types, keyspace]

type
  Item* = object
    # Keyspace
    keyspace*: Keyspace

    # User-defined key - an arbitrary byte array
    # Supports up to 2^16 bytes
    key*: UserKey

    # User-defined value - an arbitrary byte array
    # Supports up to 2^32 bytes
    value*: UserValue

    # Tombstone marker - if this is true, the value has been deleted
    valueType*: ValueType

# Debug representation
proc `$`*(item: Item): string =
  let valueTypeStr = case item.valueType
    of vtValue: "V"
    of vtTombstone: "T"
    of vtWeakTombstone: "W"
    of vtIndirection: "Vb" # Assuming this exists in ValueType

  $item.keyspace.id & ":" & $item.key & ":" & valueTypeStr & " => " & $item.value

# Constructor
proc newItem*(keyspace: Keyspace, key: UserKey, value: UserValue,
    valueType: ValueType): Item =
  # Validate key is not empty
  assert(key.len > 0)

  # Validate key length (up to 65535 bytes)
  assert(key.len <= 65535, "Keys can be up to 65535 bytes long")

  # Validate value length (up to 2^32 bytes)
  assert(value.len <= uint32.high, "Values can be up to 2^32 bytes long")

  Item(
    keyspace: keyspace,
    key: key,
    value: value,
    valueType: valueType
  )
