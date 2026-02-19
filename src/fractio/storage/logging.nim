# Copyright (c) 2024-present, fjall-rs
# This source code is licensed under both the Apache 2.0 and MIT License
# (found in the LICENSE-* files in the repository)

## Simple logging templates
##
## These templates provide basic logging functionality.

template logInfo*(msg: string) =
  echo "[INFO] " & msg

template logDebug*(msg: string) =
  echo "[DEBUG] " & msg

template logTrace*(msg: string) =
  echo "[TRACE] " & msg

template logError*(msg: string) =
  echo "[ERROR] " & msg

template logWarn*(msg: string) =
  echo "[WARN] " & msg
