# Error definitions and handling for Fractio

import tables
import strutils

type
  FractioErrorKind* = enum
    fekNone,          # No error
    fekSyntax,        # SQL syntax error
    fekSemantic,      # Semantic error (unknown table/column)
    fekConstraint,    # Constraint violation
    fekTransaction,   # Transaction error
    fekDeadlock,      # Deadlock detected
    fekSharding,      # Sharding error
    fekReplication,   # Replication error
    fekNetwork,       # Network error
    fekStorage,       # Storage error
    fekConfig,        # Configuration error
    fekPermission,    # Permission denied
    fekNotImplemented # Not implemented

  FractioError* = object
    kind*: FractioErrorKind
    message*: string
    code*: int
    context*: string

const
  ErrorCodes* = {
    fekSyntax: 1000,
    fekSemantic: 2000,
    fekConstraint: 3000,
    fekTransaction: 4000,
    fekDeadlock: 4100,
    fekSharding: 5000,
    fekReplication: 6000,
    fekNetwork: 7000,
    fekStorage: 8000,
    fekConfig: 9000,
    fekPermission: 10000,
    fekNotImplemented: 11000
  }.toTable

proc newError*(kind: FractioErrorKind, message: string,
    context: string = ""): FractioError =
  FractioError(kind: kind, message: message, code: ErrorCodes[kind],
      context: context)

proc isError*(err: FractioError): bool =
  err.kind != fekNone

proc `$`*(err: FractioError): string =
  result = "FractioError[$1] $2" % [$err.code, err.message]
  if err.context.len > 0:
    result &= " (context: " & err.context & ")"

# Common error constructors
proc syntaxError*(message, context: string): FractioError =
  newError(fekSyntax, message, context)

proc semanticError*(message, context: string): FractioError =
  newError(fekSemantic, message, context)

proc constraintError*(message, context: string): FractioError =
  newError(fekConstraint, message, context)

proc transactionError*(message, context: string): FractioError =
  newError(fekTransaction, message, context)

proc deadlockError*(): FractioError =
  newError(fekDeadlock, "Deadlock detected, transaction aborted", "")

proc shardingError*(message, context: string): FractioError =
  newError(fekSharding, message, context)

proc replicationError*(message, context: string): FractioError =
  newError(fekReplication, message, context)

proc networkError*(message, context: string): FractioError =
  newError(fekNetwork, message, context)

proc storageError*(message, context: string): FractioError =
  newError(fekStorage, message, context)

proc configError*(message, context: string): FractioError =
  newError(fekConfig, message, context)

proc permissionError*(message, context: string): FractioError =
  newError(fekPermission, message, context)

proc notImplementedError*(message, context: string): FractioError =
  newError(fekNotImplemented, message, context)
