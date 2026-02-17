# Simple OOP Logger for Fractio

import std/[tables, strformat]

type
  LogLevel* = enum
    llDebug, llInfo, llWarn, llError

  Logger* = ref object
    ## OOP logger with pluggable handlers
    name*: string
    minLevel*: LogLevel
    handlers*: seq[proc(level: LogLevel, msg: string, fields: Table[string,
        string]) {.gcsafe.}]

proc newLogger*(name: string = "", minLevel: LogLevel = llInfo): Logger =
  ## Create a new logger with optional name and minimum level
  result = Logger(name: name, minLevel: minLevel, handlers: @[])
  # Add default handler that prints to stdout
  result.handlers.add(proc(level: LogLevel, msg: string, fields: Table[string,
      string]) {.gcsafe.} =
    echo msg
  )

proc setMinLevel*(self: Logger, level: LogLevel) =
  self.minLevel = level

proc shouldLog*(self: Logger, level: LogLevel): bool {.gcsafe.} =
  level >= self.minLevel

proc formatMessage*(self: Logger, level: LogLevel, msg: string, fields: Table[
    string, string]): string {.gcsafe.} =
  var fieldStr = ""
  for k, v in fields.pairs:
    fieldStr.add(k & "=" & v & " ")
  if fieldStr.len > 0:
    result = fmt"[{self.name}] {msg} {fieldStr}"
  else:
    result = fmt"[{self.name}] {msg}"

proc log*(self: Logger, level: LogLevel, msg: string,
         fields: Table[string, string] = initTable[string, string]()) {.gcsafe.} =
  if not self.shouldLog(level):
    return
  let formatted = self.formatMessage(level, msg, fields)
  for handler in self.handlers:
    handler(level, formatted, fields)

proc debug*(self: Logger, msg: string,
            fields: Table[string, string] = initTable[string, string]()) {.gcsafe.} =
  self.log(llDebug, msg, fields)

proc info*(self: Logger, msg: string,
           fields: Table[string, string] = initTable[string, string]()) {.gcsafe.} =
  self.log(llInfo, msg, fields)

proc warn*(self: Logger, msg: string,
           fields: Table[string, string] = initTable[string, string]()) {.gcsafe.} =
  self.log(llWarn, msg, fields)

proc error*(self: Logger, msg: string,
            fields: Table[string, string] = initTable[string, string]()) {.gcsafe.} =
  self.log(llError, msg, fields)

proc addHandler*(self: Logger, handler: proc(level: LogLevel, msg: string,
    fields: Table[string, string]) {.gcsafe.}) =
  self.handlers.add(handler)
