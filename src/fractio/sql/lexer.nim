# SQL Lexer for Fractio
#
# Tokenises a SQL string into a seq[Token].
# Case-insensitive for keywords; identifiers and string literals preserve case.

import std/[strutils, unicode, tables]

type
  TokenKind* = enum
    # Literals
    tkInt        ## integer literal
    tkFloat      ## floating-point literal
    tkString     ## single-quoted string  'hello'
    tkIdent      ## identifier / unquoted name
    tkNull       ## NULL keyword (treated as a literal)
    tkTrue       ## TRUE
    tkFalse      ## FALSE

    # DDL keywords
    tkCreate
    tkDrop
    tkTable
    tkDatabase
    tkSchema
    tkIfExists
    tkIfNotExists
    tkPrimary
    tkKey
    tkUnique
    tkNot
    tkDefault

    # DML keywords
    tkSelect
    tkInsert
    tkUpdate
    tkDelete
    tkInto
    tkValues
    tkSet
    tkFrom
    tkWhere
    tkAnd
    tkOr
    tkIn
    tkIs
    tkBetween
    tkLike
    tkLimit
    tkOffset
    tkOrder
    tkBy
    tkAsc
    tkDesc
    tkAll        ## SELECT ALL (synonym for SELECT, kept for completeness)
    tkDistinct

    # Transaction keywords
    tkBegin
    tkCommit
    tkRollback
    tkTransaction
    tkWork

    # SHOW / USE keywords
    tkShow       ## SHOW
    tkUse        ## USE
    tkDatabases  ## DATABASES
    tkSchemas    ## SCHEMAS
    tkTables     ## TABLES

    # Misc keywords
    tkExplain    ## EXPLAIN

    # Storage / replication keywords
    tkWith       ## WITH
    tkSpace      ## SPACE
    tkSpaces     ## SPACES

    # Data type keywords
    tkTkInt      ## INT / INTEGER / BIGINT
    tkTkFloat    ## FLOAT / DOUBLE / REAL
    tkTkText     ## TEXT / VARCHAR / CHAR
    tkTkBool     ## BOOLEAN / BOOL
    tkTkDate     ## DATE
    tkTkDateTime ## DATETIME / TIMESTAMP
    tkTkBytes    ## BLOB / BYTES / BYTEA

    # Operators / punctuation
    tkStar       ## *
    tkComma      ## ,
    tkDot        ## .
    tkSemicolon  ## ;
    tkLParen     ## (
    tkRParen     ## )
    tkEq         ## =
    tkNeq        ## <> or !=
    tkLt         ## <
    tkLte        ## <=
    tkGt         ## >
    tkGte        ## >=
    tkPlus       ## +
    tkMinus      ## -
    tkSlash      ## /
    tkPercent    ## %

    # End / error
    tkEOF
    tkError

  Token* = object
    kind*: TokenKind
    value*: string ## raw text of the token
    line*: int
    col*: int

# ---------------------------------------------------------------------------
# Keyword table — all uppercase, mapped to the token kind
# ---------------------------------------------------------------------------

const keywords = {
  "CREATE": tkCreate,
  "DROP": tkDrop,
  "TABLE": tkTable,
  "DATABASE": tkDatabase,
  "SCHEMA": tkSchema,
  "IF": tkIdent, # handled contextually by the parser
  "PRIMARY": tkPrimary,
  "KEY": tkKey,
  "UNIQUE": tkUnique,
  "NOT": tkNot,
  "NULL": tkNull,
  "DEFAULT": tkDefault,
  "SELECT": tkSelect,
  "INSERT": tkInsert,
  "UPDATE": tkUpdate,
  "DELETE": tkDelete,
  "INTO": tkInto,
  "VALUES": tkValues,
  "SET": tkSet,
  "FROM": tkFrom,
  "WHERE": tkWhere,
  "AND": tkAnd,
  "OR": tkOr,
  "IN": tkIn,
  "IS": tkIs,
  "BETWEEN": tkBetween,
  "LIKE": tkLike,
  "LIMIT": tkLimit,
  "OFFSET": tkOffset,
  "ORDER": tkOrder,
  "BY": tkBy,
  "ASC": tkAsc,
  "DESC": tkDesc,
  "ALL": tkAll,
  "DISTINCT": tkDistinct,
  "BEGIN": tkBegin,
  "COMMIT": tkCommit,
  "ROLLBACK": tkRollback,
  "TRANSACTION": tkTransaction,
  "WORK": tkWork,
  "TRUE": tkTrue,
  "FALSE": tkFalse,
  "EXPLAIN": tkExplain,
  "WITH": tkWith,
  "SHOW": tkShow,
  "USE": tkUse,
  "DATABASES": tkDatabases,
  "SCHEMAS": tkSchemas,
  "TABLES": tkTables,
  "SPACE": tkSpace,
  "SPACES": tkSpaces,
  # Type aliases
  "INT": tkTkInt,
  "INTEGER": tkTkInt,
  "BIGINT": tkTkInt,
  "SMALLINT": tkTkInt,
  "TINYINT": tkTkInt,
  "FLOAT": tkTkFloat,
  "DOUBLE": tkTkFloat,
  "REAL": tkTkFloat,
  "NUMERIC": tkTkFloat,
  "DECIMAL": tkTkFloat,
  "TEXT": tkTkText,
  "VARCHAR": tkTkText,
  "CHAR": tkTkText,
  "STRING": tkTkText,
  "BOOLEAN": tkTkBool,
  "BOOL": tkTkBool,
  "DATE": tkTkDate,
  "DATETIME": tkTkDateTime,
  "TIMESTAMP": tkTkDateTime,
  "BLOB": tkTkBytes,
  "BYTES": tkTkBytes,
  "BYTEA": tkTkBytes,
  "BINARY": tkTkBytes,
}.toTable

# ---------------------------------------------------------------------------
# Lexer state
# ---------------------------------------------------------------------------

type
  Lexer* = object
    src*: string
    pos*: int
    line*: int
    col*: int

proc newLexer*(src: string): Lexer =
  Lexer(src: src, pos: 0, line: 1, col: 1)

proc atEnd(l: Lexer): bool {.inline.} = l.pos >= l.src.len

proc peek(l: Lexer, offset: int = 0): char {.inline.} =
  let i = l.pos + offset
  if i < l.src.len: l.src[i] else: '\0'

proc advance(l: var Lexer): char =
  result = l.src[l.pos]
  inc l.pos
  if result == '\n':
    inc l.line
    l.col = 1
  else:
    inc l.col

proc skipWhitespaceAndComments(l: var Lexer) =
  while not l.atEnd:
    let c = l.peek
    if c in Whitespace:
      discard l.advance
    elif c == '-' and l.peek(1) == '-':
      # single-line comment
      while not l.atEnd and l.peek != '\n':
        discard l.advance
    elif c == '/' and l.peek(1) == '*':
      # block comment
      discard l.advance; discard l.advance
      while not l.atEnd:
        if l.peek == '*' and l.peek(1) == '/':
          discard l.advance; discard l.advance
          break
        discard l.advance
    else:
      break

proc lexString(l: var Lexer, startLine, startCol: int): Token =
  discard l.advance # consume opening quote
  var s = ""
  while not l.atEnd:
    let c = l.advance
    if c == '\'':
      if l.peek == '\'':
        # escaped single quote
        discard l.advance
        s.add('\'')
      else:
        return Token(kind: tkString, value: s, line: startLine, col: startCol)
    else:
      s.add(c)
  Token(kind: tkError, value: "unterminated string literal", line: startLine, col: startCol)

proc lexNumber(l: var Lexer, startLine, startCol: int): Token =
  var s = ""
  var isFloat = false
  while not l.atEnd and l.peek in {'0'..'9'}:
    s.add(l.advance)
  if not l.atEnd and l.peek == '.' and l.peek(1) in {'0'..'9'}:
    isFloat = true
    s.add(l.advance) # dot
    while not l.atEnd and l.peek in {'0'..'9'}:
      s.add(l.advance)
  if not l.atEnd and l.peek in {'e', 'E'}:
    isFloat = true
    s.add(l.advance)
    if not l.atEnd and l.peek in {'+', '-'}:
      s.add(l.advance)
    while not l.atEnd and l.peek in {'0'..'9'}:
      s.add(l.advance)
  Token(kind: if isFloat: tkFloat else: tkInt,
        value: s, line: startLine, col: startCol)

proc lexIdent(l: var Lexer, startLine, startCol: int): Token =
  var s = ""
  while not l.atEnd and (l.peek in IdentChars or l.peek == '_'):
    s.add(l.advance)
  let upper = s.toUpperAscii
  let kind = keywords.getOrDefault(upper, tkIdent)
  Token(kind: kind, value: s, line: startLine, col: startCol)

proc lexQuotedIdent(l: var Lexer, startLine, startCol: int): Token =
  discard l.advance # consume opening "
  var s = ""
  while not l.atEnd:
    let c = l.advance
    if c == '"':
      if l.peek == '"':
        s.add('"')
        discard l.advance
      else:
        return Token(kind: tkIdent, value: s, line: startLine, col: startCol)
    else:
      s.add(c)
  Token(kind: tkError, value: "unterminated quoted identifier", line: startLine, col: startCol)

proc nextToken*(l: var Lexer): Token =
  l.skipWhitespaceAndComments
  if l.atEnd:
    return Token(kind: tkEOF, value: "", line: l.line, col: l.col)

  let startLine = l.line
  let startCol = l.col
  let c = l.peek

  case c
  of '\'': return l.lexString(startLine, startCol)
  of '"': return l.lexQuotedIdent(startLine, startCol)
  of '`':
    # MySQL-style quoted identifier
    discard l.advance
    var s = ""
    while not l.atEnd and l.peek != '`': s.add(l.advance)
    if not l.atEnd: discard l.advance
    return Token(kind: tkIdent, value: s, line: startLine, col: startCol)
  of '0'..'9':
    return l.lexNumber(startLine, startCol)
  of 'a'..'z', 'A'..'Z', '_':
    return l.lexIdent(startLine, startCol)
  of '*':
    discard l.advance
    return Token(kind: tkStar, value: "*", line: startLine, col: startCol)
  of ',':
    discard l.advance
    return Token(kind: tkComma, value: ",", line: startLine, col: startCol)
  of '.':
    discard l.advance
    return Token(kind: tkDot, value: ".", line: startLine, col: startCol)
  of ';':
    discard l.advance
    return Token(kind: tkSemicolon, value: ";", line: startLine, col: startCol)
  of '(':
    discard l.advance
    return Token(kind: tkLParen, value: "(", line: startLine, col: startCol)
  of ')':
    discard l.advance
    return Token(kind: tkRParen, value: ")", line: startLine, col: startCol)
  of '=':
    discard l.advance
    return Token(kind: tkEq, value: "=", line: startLine, col: startCol)
  of '<':
    discard l.advance
    if not l.atEnd and l.peek == '=':
      discard l.advance
      return Token(kind: tkLte, value: "<=", line: startLine, col: startCol)
    elif not l.atEnd and l.peek == '>':
      discard l.advance
      return Token(kind: tkNeq, value: "<>", line: startLine, col: startCol)
    return Token(kind: tkLt, value: "<", line: startLine, col: startCol)
  of '>':
    discard l.advance
    if not l.atEnd and l.peek == '=':
      discard l.advance
      return Token(kind: tkGte, value: ">=", line: startLine, col: startCol)
    return Token(kind: tkGt, value: ">", line: startLine, col: startCol)
  of '!':
    discard l.advance
    if not l.atEnd and l.peek == '=':
      discard l.advance
      return Token(kind: tkNeq, value: "!=", line: startLine, col: startCol)
    return Token(kind: tkError, value: "!", line: startLine, col: startCol)
  of '+':
    discard l.advance
    return Token(kind: tkPlus, value: "+", line: startLine, col: startCol)
  of '-':
    discard l.advance
    return Token(kind: tkMinus, value: "-", line: startLine, col: startCol)
  of '/':
    discard l.advance
    return Token(kind: tkSlash, value: "/", line: startLine, col: startCol)
  of '%':
    discard l.advance
    return Token(kind: tkPercent, value: "%", line: startLine, col: startCol)
  else:
    discard l.advance
    return Token(kind: tkError, value: $c, line: startLine, col: startCol)

proc tokenize*(src: string): seq[Token] =
  var l = newLexer(src)
  while true:
    let tok = l.nextToken
    result.add(tok)
    if tok.kind in {tkEOF, tkError}: break
