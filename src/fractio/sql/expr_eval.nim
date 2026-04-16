# SQL Expression Evaluator
#
# Pure expression evaluation functions extracted from executor.nim.
# These functions are fully testable without any I/O dependencies.
# They operate on DataRow and Expr types only.

import std/[options, json, strutils]
import ./ast
import ./data_row

# ---------------------------------------------------------------------------
# DataRow-based expression evaluator (preferred, fully typed)
# ---------------------------------------------------------------------------

proc evalExprDataRow*(expr: Expr, row: DataRow): DataRowValue =
  ## Evaluate an expression against a DataRow.
  ## Pure function - fully testable without I/O.
  case expr.kind
  of exLiteral:
    if expr.litValue == nil:
      return newRowValue()
    case expr.litValue.kind
    of dtInt: return newRowValue(expr.litValue.intValue)
    of dtFloat: return newRowValue(expr.litValue.floatValue)
    of dtString: return newRowValue(expr.litValue.strValue)
    of dtBool: return newRowValue(expr.litValue.boolValue)
    else: return newRowValue()

  of exColumn:
    let name = expr.colName
    if row.hasColumn(name):
      return row[name]
    return newRowValue()

  of exBinOp:
    let left = evalExprDataRow(expr.binLeft, row)
    let right = evalExprDataRow(expr.binRight, row)

    case expr.binOp
    of boEq:
      return newRowValue(left == right)
    of boNeq:
      return newRowValue(left != right)
    of boLt:
      return newRowValue(left < right)
    of boLte:
      return newRowValue(left <= right)
    of boGt:
      return newRowValue(left > right)
    of boGte:
      return newRowValue(left >= right)
    of boAnd:
      return left and right
    of boOr:
      return left or right
    of boAdd:
      return left + right
    of boSub:
      return left - right
    of boMul:
      return left * right
    of boDiv:
      return left div right
    of boMod:
      return left mod right

  of exUnaryOp:
    let inner = evalExprDataRow(expr.unaryExpr, row)
    case expr.unaryOp
    of uoNot:
      return not inner
    of uoNeg:
      return -inner

  of exIsNull:
    let inner = evalExprDataRow(expr.isNullExpr, row)
    let isNull = inner.kind == drvkNull
    return newRowValue(if expr.isNullNot: not isNull else: isNull)

  of exIn:
    let val = evalExprDataRow(expr.inExpr, row)
    var found = false
    for item in expr.inList:
      if evalExprDataRow(item, row) == val:
        found = true
        break
    return newRowValue(if expr.inNot: not found else: found)

  of exBetween:
    let val = evalExprDataRow(expr.betweenExpr, row)
    let lo = evalExprDataRow(expr.betweenLo, row)
    let hi = evalExprDataRow(expr.betweenHi, row)
    var inRange = val >= lo and val <= hi
    return newRowValue(if expr.betweenNot: not inRange else: inRange)

  of exLike:
    # Simple LIKE: only handle % wildcard at start/end
    let val = evalExprDataRow(expr.likeExpr, row)
    let pat = evalExprDataRow(expr.likePattern, row)
    if val.kind == drvkString and pat.kind == drvkString:
      let s = val.strVal
      let p = pat.strVal
      var matches = false
      if p.startsWith("%") and p.endsWith("%"):
        matches = p[1..^2] in s
      elif p.startsWith("%"):
        matches = s.endsWith(p[1..^1])
      elif p.endsWith("%"):
        matches = s.startsWith(p[0..^2])
      else:
        matches = s == p
      return newRowValue(if expr.likeNot: not matches else: matches)
    return newRowValue()

  of exStar, exParam, exList:
    return newRowValue()

proc matchesFilterDataRow*(filter: Option[Expr], row: DataRow): bool =
  ## Check if a DataRow passes the WHERE filter.
  ## Pure function - fully testable without I/O.
  if filter.isNone:
    return true
  let result = evalExprDataRow(filter.get(), row)
  result.kind == drvkBool and result.boolVal

# ---------------------------------------------------------------------------
# Row helpers (pure functions)
# ---------------------------------------------------------------------------

proc extractColumnsFromDataRow*(row: DataRow, columns: seq[string]): seq[string] =
  ## Extract column values from a DataRow as strings.
  ## Pure function - fully testable without I/O.
  for col in columns:
    result.add(row[col].toStringValue())

proc getPkValueFromDataRow*(row: DataRow, pkColumn: string): string =
  ## Get primary key value from a DataRow.
  ## Pure function - fully testable without I/O.
  if row.hasColumn(pkColumn):
    let v = row[pkColumn]
    case v.kind
    of drvkString: return v.strVal
    of drvkInt: return $v.intVal
    else: return v.toStringValue()
  ""

# ---------------------------------------------------------------------------
# Legacy JSON-based evaluators (kept for backward compatibility)
# DEPRECATED: Use evalExprDataRow with DataRow instead.
# ---------------------------------------------------------------------------

proc evalExpr*(expr: Expr, row: JsonNode): JsonNode =
  ## Evaluate an expression against a JSON row object.
  ## DEPRECATED: Use evalExprDataRow with DataRow instead.
  case expr.kind
  of exLiteral:
    if expr.litValue == nil:
      return newJNull()
    case expr.litValue.kind
    of dtInt: return newJInt(expr.litValue.intValue)
    of dtFloat: return newJFloat(expr.litValue.floatValue)
    of dtString: return newJString(expr.litValue.strValue)
    of dtBool: return newJBool(expr.litValue.boolValue)
    else: return newJNull()

  of exColumn:
    let name = expr.colName
    if row.hasKey(name):
      return row[name]
    return newJNull()

  of exBinOp:
    let left = evalExpr(expr.binLeft, row)
    let right = evalExpr(expr.binRight, row)

    case expr.binOp
    of boEq:
      return newJBool(left == right)
    of boNeq:
      return newJBool(left != right)
    of boLt:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt < right.getInt)
      if left.kind == JString and right.kind == JString:
        return newJBool(left.getStr < right.getStr)
      return newJBool(false)
    of boLte:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt <= right.getInt)
      return newJBool(false)
    of boGt:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt > right.getInt)
      return newJBool(false)
    of boGte:
      if left.kind == JInt and right.kind == JInt:
        return newJBool(left.getInt >= right.getInt)
      return newJBool(false)
    of boAnd:
      return newJBool(left.getBool(false) and right.getBool(false))
    of boOr:
      return newJBool(left.getBool(false) or right.getBool(false))
    of boAdd:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt + right.getInt)
      return newJNull()
    of boSub:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt - right.getInt)
      return newJNull()
    of boMul:
      if left.kind == JInt and right.kind == JInt:
        return newJInt(left.getInt * right.getInt)
      return newJNull()
    of boDiv:
      if left.kind == JInt and right.kind == JInt and right.getInt != 0:
        return newJInt(left.getInt div right.getInt)
      return newJNull()
    of boMod:
      if left.kind == JInt and right.kind == JInt and right.getInt != 0:
        return newJInt(left.getInt mod right.getInt)
      return newJNull()

  of exUnaryOp:
    let inner = evalExpr(expr.unaryExpr, row)
    case expr.unaryOp
    of uoNot:
      return newJBool(not inner.getBool(false))
    of uoNeg:
      if inner.kind == JInt:
        return newJInt(-inner.getInt)
      return newJNull()

  of exIsNull:
    let inner = evalExpr(expr.isNullExpr, row)
    let isNull = inner.kind == JNull
    return newJBool(if expr.isNullNot: not isNull else: isNull)

  of exIn:
    let val = evalExpr(expr.inExpr, row)
    var found = false
    for item in expr.inList:
      if evalExpr(item, row) == val:
        found = true
        break
    return newJBool(if expr.inNot: not found else: found)

  of exBetween:
    let val = evalExpr(expr.betweenExpr, row)
    let lo = evalExpr(expr.betweenLo, row)
    let hi = evalExpr(expr.betweenHi, row)
    var inRange = false
    if val.kind == JInt and lo.kind == JInt and hi.kind == JInt:
      inRange = val.getInt >= lo.getInt and val.getInt <= hi.getInt
    return newJBool(if expr.betweenNot: not inRange else: inRange)

  of exLike:
    # Simple LIKE: only handle % wildcard at start/end
    let val = evalExpr(expr.likeExpr, row)
    let pat = evalExpr(expr.likePattern, row)
    if val.kind == JString and pat.kind == JString:
      let s = val.getStr
      let p = pat.getStr
      var matches = false
      if p.startsWith("%") and p.endsWith("%"):
        matches = p[1..^2] in s
      elif p.startsWith("%"):
        matches = s.endsWith(p[1..^1])
      elif p.endsWith("%"):
        matches = s.startsWith(p[0..^2])
      else:
        matches = s == p
      return newJBool(if expr.likeNot: not matches else: matches)
    return newJBool(false)

  of exStar, exParam, exList:
    return newJNull()

proc matchesFilter*(filter: Option[Expr], row: JsonNode): bool =
  ## Check if a row passes the WHERE filter.
  ## DEPRECATED: Use matchesFilterDataRow with DataRow instead.
  if filter.isNone:
    return true
  let result = evalExpr(filter.get(), row)
  result.kind == JBool and result.getBool(false)

proc jsonToStringValue*(j: JsonNode): string =
  ## Convert JSON node to string value.
  ## Pure function - fully testable.
  case j.kind
  of JString: j.getStr
  of JInt: $j.getInt
  of JFloat: $j.getFloat
  of JBool: $j.getBool
  of JNull: "NULL"
  else: $j

proc extractColumns*(row: JsonNode, columns: seq[string]): seq[string] =
  ## Extract column values from a JSON row.
  ## DEPRECATED: Use extractColumnsFromDataRow with DataRow instead.
  for col in columns:
    if row.hasKey(col):
      result.add(jsonToStringValue(row[col]))
    else:
      result.add("NULL")

proc getPkValue*(row: JsonNode, pkColumn: string): string =
  ## Get primary key value from a JSON row.
  ## DEPRECATED: Use getPkValueFromDataRow with DataRow instead.
  if row.hasKey(pkColumn):
    let v = row[pkColumn]
    case v.kind
    of JString: return v.getStr
    of JInt: return $v.getInt
    else: return $v
  ""
