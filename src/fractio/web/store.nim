# Fractio Web Dashboard - Centralized State Management
#
# This module provides a unified state management system for the SPA frontend.
# It consolidates all reactive state, loading indicators, and error handling.

import happyx
import std/[jsffi, sets, tables]

# =============================================================================
# Theme State
# =============================================================================

var gDarkMode* {.global.}: State[bool] = remember false

proc toggleDarkMode*(): bool =
  gDarkMode.set(not gDarkMode.get())
  # Update Shoelace theme
  {.emit: """
  if (`gDarkMode`.value) {
    document.documentElement.classList.add('sl-theme-dark');
    document.documentElement.setAttribute('data-theme', 'dark');
  } else {
    document.documentElement.classList.remove('sl-theme-dark');
    document.documentElement.setAttribute('data-theme', 'light');
  }
  """.}
  true

# =============================================================================
# Server Info State
# =============================================================================

type NodeInfo* = object
  nodeId*: int
  version*: string
  uptimeSecs*: int
  role*: string
  shardCount*: int
  clientCount*: int
  clusterName*: string

var gInfo* {.global.}: State[JsObject] = remember newJsObject()
var gHealth* {.global.}: State[JsObject] = remember newJsObject()
var gMetrics* {.global.}: State[JsObject] = remember newJsObject()
var gStorage* {.global.}: State[JsObject] = remember newJsObject()

# =============================================================================
# Nodes and Spaces State
# =============================================================================

var gNodes* {.global.}: State[JsObject] = remember newJsObject()
var gSpaces* {.global.}: State[JsObject] = remember newJsObject()

# Expanded state for UI
var gExpandedNodes* {.global.}: State[seq[int]] = remember newSeq[int]()
var gExpandedSpaces* {.global.}: State[seq[int]] = remember newSeq[int]()

# =============================================================================
# Data Browser State
# =============================================================================

var gDatabases* {.global.}: State[seq[string]] = remember newSeq[string]()
var gSchemas* {.global.}: State[seq[string]] = remember newSeq[string]()
var gTables* {.global.}: State[seq[string]] = remember newSeq[string]()
var gTableData* {.global.}: State[JsObject] = remember newJsObject()

# System tables state
var gSysTables* {.global.}: State[JsObject] = remember newJsObject()
var gSysTableData* {.global.}: State[JsObject] = remember newJsObject()

# =============================================================================
# SQL Editor State
# =============================================================================

var gCurrentDatabase* {.global.}: State[string] = remember "default"
var gCurrentSchema* {.global.}: State[string] = remember "public"
var gSqlQuery* {.global.}: State[string] = remember ""
var gSqlResult* {.global.}: State[JsObject] = remember newJsObject()
var gSqlHistory* {.global.}: State[seq[string]] = remember newSeq[string]()
var gSavedQueries* {.global.}: State[seq[(string, string)]] = remember newSeq[(
    string, string)]()

# =============================================================================
# Loading State - Unified tracking
# =============================================================================

type LoadingKey* = enum
  lkInfo
  lkHealth
  lkMetrics
  lkStorage
  lkNodes
  lkSpaces
  lkDatabases
  lkSchemas
  lkTables
  lkTableData
  lkSysTables
  lkSysTableData
  lkSqlQuery

var gLoading* {.global.}: State[HashSet[string]] = remember initHashSet[string]()

proc isLoading*(key: string): bool =
  gLoading.get().contains(key)

proc setLoading*(key: string, loading: bool) =
  var cur = gLoading.get()
  if loading:
    cur.incl(key)
  else:
    cur.excl(key)
  gLoading.set(cur)

proc isLoadingAny*: bool =
  gLoading.get().len > 0

# =============================================================================
# Error State - Unified tracking
# =============================================================================

var gErrors* {.global.}: State[Table[string, string]] = remember initTable[
    string, string]()
var gLastError* {.global.}: State[string] = remember ""
var gShowToast* {.global.}: State[bool] = remember false
var gToastMessage* {.global.}: State[string] = remember ""
var gToastType* {.global.}: State[string] = remember "info" # "info", "success", "warning", "danger"

proc showError*(key: string, message: string) =
  var errs = gErrors.get()
  errs[key] = message
  gErrors.set(errs)
  gLastError.set(message)
  gToastMessage.set(message)
  gToastType.set("danger")
  gShowToast.set(true)

proc showSuccess*(message: string) =
  gToastMessage.set(message)
  gToastType.set("success")
  gShowToast.set(true)

proc showWarning*(message: string) =
  gToastMessage.set(message)
  gToastType.set("warning")
  gShowToast.set(true)

proc showInfo*(message: string) =
  gToastMessage.set(message)
  gToastType.set("info")
  gShowToast.set(true)

proc clearError*(key: string) =
  var errs = gErrors.get()
  errs.del(key)
  gErrors.set(errs)

proc clearAllErrors*() =
  gErrors.set(initTable[string, string]())
  gLastError.set("")
  gShowToast.set(false)

# =============================================================================
# Pagination State
# =============================================================================

type PaginationState* = object
  page*: int
  pageSize*: int
  totalRows*: int
  searchQuery*: string

var gTablePagination* {.global.}: State[
    PaginationState] = remember PaginationState(
  page: 1,
  pageSize: 50,
  totalRows: 0,
  searchQuery: ""
)

proc totalPages*(p: PaginationState): int =
  if p.pageSize == 0: return 1
  max(1, p.totalRows div p.pageSize + (if p.totalRows mod p.pageSize > 0: 1 else: 0))

proc nextPage*(): int =
  var p = gTablePagination.get()
  if p.page < p.totalPages:
    p.page += 1
    gTablePagination.set(p)
  0

proc prevPage*(): int =
  var p = gTablePagination.get()
  if p.page > 1:
    p.page -= 1
    gTablePagination.set(p)
  0

proc setPage*(n: int): int =
  var p = gTablePagination.get()
  p.page = max(1, min(n, p.totalPages))
  gTablePagination.set(p)
  0

proc setPageSize*(size: int): int =
  var p = gTablePagination.get()
  p.pageSize = max(10, min(500, size))
  p.page = 1 # Reset to first page
  gTablePagination.set(p)
  0

proc setSearchQuery*(query: string): int =
  var p = gTablePagination.get()
  p.searchQuery = query
  p.page = 1 # Reset to first page
  gTablePagination.set(p)
  0

# =============================================================================
# Data Loaded State - Track what has been fetched
# =============================================================================

var
  loadedDatabases* {.global.}: bool = false
  loadedSchemasKey* {.global.}: string = ""
  loadedTablesKey* {.global.}: string = ""
  loadedTableDataKey* {.global.}: string = ""
  loadedSysTables* {.global.}: bool = false
  loadedSysTableDataKey* {.global.}: string = ""
  loadedSpaces* {.global.}: bool = false

# =============================================================================
# Clock Drift State - Plain globals (not reactive to avoid re-render)
# =============================================================================
# MaxSamples is defined in chart.nim (120 = 2 minutes @ 1Hz)

var gDriftSamples*: seq[float] = @[]
var gDriftLastStr*: string = "—"
var gDriftWsStr*: string = "connecting…"
var gDriftWs*: JsObject = nil

# =============================================================================
# Toggle Functions for UI
# =============================================================================

proc toggleNodeExpanded*(nodeId: int): bool =
  var cur = gExpandedNodes.get()
  let idx = cur.find(nodeId)
  if idx >= 0:
    cur.delete(idx)
  else:
    cur.add(nodeId)
  gExpandedNodes.set(cur)
  true

proc toggleSpaceExpanded*(spaceId: int): bool =
  var cur = gExpandedSpaces.get()
  let idx = cur.find(spaceId)
  if idx >= 0:
    cur.delete(idx)
  else:
    cur.add(spaceId)
  gExpandedSpaces.set(cur)
  true

# =============================================================================
# Message State (for join node form)
# =============================================================================

var gMsg* {.global.}: State[string] = remember ""
var gMsgOk* {.global.}: State[bool] = remember false

# =============================================================================
# Modal State
# =============================================================================

type ModalConfig* = object
  title*: string
  message*: string
  confirmText*: string
  cancelText*: string
  onConfirm*: proc()
  dangerous*: bool

var gModalOpen* {.global.}: State[bool] = remember false
var gModalConfig* {.global.}: State[ModalConfig] = remember ModalConfig()

proc showModal*(title: string, message: string, onConfirm: proc(),
                confirmText = "Confirm", cancelText = "Cancel",
                    dangerous = false) =
  gModalConfig.set(ModalConfig(
    title: title,
    message: message,
    confirmText: confirmText,
    cancelText: cancelText,
    onConfirm: onConfirm,
    dangerous: dangerous
  ))
  gModalOpen.set(true)

proc closeModal*() =
  gModalOpen.set(false)

proc confirmModal*() =
  let cfg = gModalConfig.get()
  if cfg.onConfirm != nil:
    cfg.onConfirm()
  closeModal()
