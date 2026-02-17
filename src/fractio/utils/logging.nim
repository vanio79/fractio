# Minimal logging utility for Fractio
# Provides basic logger interface for diagnostic messages

import std/[tables, strformat]

type
  LogLevel* = enum
    llDebug, llInfo, llWarn, llError

  Logger* = ref object
    name*: string
    minLevel*: LogLevel
    # In production, would have output handlers

proc newLogger*(name: string = "", minLevel: LogLevel = llInfo): Logger =
  ## Create a new logger with optional name and minimum level
  Logger(name: name, minLevel: minLevel)

proc shouldLog*(logger: Logger, level: LogLevel): bool =
  level >= logger.minLevel

proc log*(logger: Logger, level: LogLevel, msg: string,
          fields: Table[string, string] = initTable[string, string]()) =
  if not logger.shouldLog(level):
    return
  var fullMsg = msg
  if fields.len > 0:
    var fieldStr = ""
    for k, v in fields.pairs:
      fieldStr.add(fmt"{k}={v} ")
    fullMsg = fmt"[{logger.name}] {msg} {fieldStr}"
  else:
    fullMsg = fmt"[{logger.name}] {msg}"
  # In production, would route to file/metrics
  echo fullMsg

proc debug*(logger: Logger, msg: string, fields: Table[string,
    string] = initTable[string, string]()) =
  logger.log(llDebug, msg, fields)

proc info*(logger: Logger, msg: string, fields: Table[string,
    string] = initTable[string, string]()) =
  logger.log(llInfo, msg, fields)

proc warn*(logger: Logger, msg: string, fields: Table[string,
    string] = initTable[string, string]()) =
  logger.log(llWarn, msg, fields)

proc error*(logger: Logger, msg: string, fields: Table[string,
    string] = initTable[string, string]()) =
  logger.log(llError, msg, fields)
